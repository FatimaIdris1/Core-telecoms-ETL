from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import boto3
import json
import os
import io
import logging
import sys
from datetime import datetime
import time
import gspread
from google.oauth2.service_account import Credentials
from botocore.exceptions import ClientError
import botocore
import pandas as pd
import psycopg2
from psycopg2 import sql
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

# ===================================
# LOGGING SETUP
# ===================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log")
    ]
)
logger = logging.getLogger(__name__)


# ===================================
# METADATA LOGGING CLASSES
# ===================================
@dataclass
class OperationMetadata:
    """Metadata for tracking operation execution"""
    operation_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    status: str = "RUNNING"
    records_processed: int = 0
    records_success: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    additional_info: Optional[Dict] = None

    def complete(self, status: str = "SUCCESS", error: Optional[str] = None):
        """Mark operation as complete and calculate duration"""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = status
        if error:
            self.error_message = error

    def to_dict(self) -> Dict:
        """Convert to dictionary for logging"""
        return {
            **asdict(self),
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None
        }

    def log_summary(self):
        """Log operation summary"""
        logger.info("=" * 60)
        logger.info(f"OPERATION: {self.operation_name}")
        logger.info(f"Status: {self.status}")
        logger.info(f"Duration: {self.duration_seconds:.2f} seconds")
        logger.info(f"Records Processed: {self.records_processed}")
        logger.info(f"Records Success: {self.records_success}")
        logger.info(f"Records Failed: {self.records_failed}")
        if self.error_message:
            logger.info(f"Error: {self.error_message}")
        if self.additional_info:
            logger.info(f"Additional Info: {json.dumps(self.additional_info, indent=2)}")
        logger.info("=" * 60)


# ===================================
# SECTION 1: S3 INCREMENTAL COPY
# ===================================
class S3IncrementalCopy:
    """Handles incremental copying and conversion of files from source to target S3 bucket"""
    
    def __init__(self, source_bucket: str, target_bucket: str, region_name: str = "eu-north-1"):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.region_name = region_name
        self.s3 = boto3.client("s3", region_name=region_name)

    def execute(self, prefix: str = "") -> OperationMetadata:
        """Execute incremental copy operation"""
        metadata = OperationMetadata(
            operation_name=f"S3_Incremental_Copy_{prefix}",
            start_time=datetime.now()
        )

        try:
            logger.info(f"Starting S3 incremental copy for prefix: {prefix}")
            
            # List source files
            src_objects = self.s3.list_objects_v2(Bucket=self.source_bucket, Prefix=prefix)
            if "Contents" not in src_objects:
                logger.warning("No files found in source bucket.")
                metadata.complete(status="SUCCESS")
                metadata.additional_info = {"message": "No files found"}
                metadata.log_summary()
                return metadata

            # List existing target files
            tgt_objects = self.s3.list_objects_v2(Bucket=self.target_bucket, Prefix=prefix)
            existing_files = set()
            if "Contents" in tgt_objects:
                existing_files = {obj["Key"] for obj in tgt_objects["Contents"]}

            logger.info(f"{len(existing_files)} existing parquet files in target bucket.")

            # Process each file
            for obj in src_objects["Contents"]:
                key = obj["Key"]
                metadata.records_processed += 1

                # Only CSV or JSON
                if not (key.endswith(".csv") or key.endswith(".json")):
                    logger.info(f"Skipping unsupported file: {key}")
                    continue

                # Build target parquet key
                parquet_key = key.rsplit(".", 1)[0] + ".parquet"

                # Skip if parquet already exists
                if parquet_key in existing_files:
                    logger.info(f"Skipping (already converted): {parquet_key}")
                    continue

                try:
                    # Download and convert
                    logger.info(f"Processing: {key}")
                    obj_data = self.s3.get_object(Bucket=self.source_bucket, Key=key)
                    raw_data = obj_data["Body"].read().decode("utf-8")

                    # Convert to DataFrame
                    if key.endswith(".csv"):
                        df = pd.read_csv(io.StringIO(raw_data))
                    else:
                        df = pd.read_json(io.StringIO(raw_data))

                    # Convert to Parquet
                    table = pa.Table.from_pandas(df)
                    parquet_buffer = io.BytesIO()
                    pq.write_table(table, parquet_buffer)

                    # Upload to S3
                    self.s3.put_object(
                        Bucket=self.target_bucket,
                        Key=parquet_key,
                        Body=parquet_buffer.getvalue()
                    )

                    metadata.records_success += 1
                    logger.info(f"SUCCESS: {parquet_key}")

                except Exception as e:
                    metadata.records_failed += 1
                    logger.error(f"FAILED processing {key}: {e}")

            metadata.complete(status="SUCCESS")
            metadata.additional_info = {
                "source_bucket": self.source_bucket,
                "target_bucket": self.target_bucket,
                "prefix": prefix
            }

        except Exception as e:
            metadata.complete(status="FAILED", error=str(e))
            logger.error(f"S3 incremental copy failed: {e}")

        metadata.log_summary()
        return metadata


# ===================================
# SECTION 2: GOOGLE SHEETS TO S3
# ===================================
class GoogleSheetsToS3:
    """Extract data from Google Sheets and upload to S3 as Parquet"""
    
    def __init__(self, service_account_file: str, spreadsheet_id: str, 
                 s3_bucket: str, region_name: str = "eu-north-1"):
        self.service_account_file = service_account_file
        self.spreadsheet_id = spreadsheet_id
        self.s3_bucket = s3_bucket
        self.region_name = region_name
        self.s3 = boto3.client("s3", region_name=region_name)
        
        # Setup Google Sheets credentials
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
        self.gc = gspread.authorize(creds)

    def execute(self, sheet_name: str, s3_key: str) -> OperationMetadata:
        """Extract sheet data and upload to S3"""
        metadata = OperationMetadata(
            operation_name=f"GoogleSheets_To_S3_{sheet_name}",
            start_time=datetime.now()
        )

        try:
            logger.info(f"Starting Google Sheets extraction: {sheet_name}")
            
            # Get sheet data
            sheet = self.gc.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            
            metadata.records_processed = len(df)
            logger.info(f"Loaded {len(df)} rows from Google Sheet")

            # Convert to Parquet in memory
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            # Upload to S3
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=buffer.getvalue()
            )

            metadata.records_success = len(df)
            metadata.complete(status="SUCCESS")
            metadata.additional_info = {
                "spreadsheet_id": self.spreadsheet_id,
                "sheet_name": sheet_name,
                "s3_location": f"s3://{self.s3_bucket}/{s3_key}",
                "row_count": len(df),
                "column_count": len(df.columns)
            }
            logger.info(f"Successfully uploaded to s3://{self.s3_bucket}/{s3_key}")

        except Exception as e:
            metadata.records_failed = metadata.records_processed
            metadata.complete(status="FAILED", error=str(e))
            logger.error(f"Google Sheets extraction failed: {e}")

        metadata.log_summary()
        return metadata


# ===================================
# SECTION 3: POSTGRES TO S3
# ===================================
class PostgresToS3:
    """Extract data from PostgreSQL and upload to S3 as Parquet"""
    
    def __init__(self, ssm_prefix: str, s3_bucket: str, region_name: str = "eu-north-1"):
        self.ssm_prefix = ssm_prefix
        self.s3_bucket = s3_bucket
        self.region_name = region_name
        self.s3 = boto3.client("s3", region_name=region_name)
        self.db_params = self._get_db_params()

    def _get_db_params(self) -> Dict[str, str]:
        """Load database parameters from AWS SSM"""
        ssm = boto3.client("ssm", region_name=self.region_name)
        
        keys = {
            "host": "db_host",
            "database": "db_name",
            "username": "db_username",
            "password": "db_password",
            "port": "db_port",
            "schema": "table_schema_name"
        }

        params = {}
        for k, ssm_key in keys.items():
            path = f"{self.ssm_prefix}{ssm_key}"
            logger.info(f"Fetching SSM param: {path}")
            response = ssm.get_parameter(Name=path, WithDecryption=True)
            params[k] = response["Parameter"]["Value"]

        return params

    def _get_all_tables(self) -> List[str]:
        """Get all tables in the schema"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.db_params["host"],
                port=int(self.db_params["port"]),
                user=self.db_params["username"],
                password=self.db_params["password"],
                dbname=self.db_params["database"]
            )

            query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_type = 'BASE TABLE';
            """

            with conn.cursor() as cur:
                cur.execute(query, (self.db_params["schema"],))
                tables = [r[0] for r in cur.fetchall()]
                logger.info(f"Found {len(tables)} tables in schema '{self.db_params['schema']}'")
                return tables
        finally:
            if conn:
                conn.close()

    def _fetch_table_data(self, table_name: str) -> pd.DataFrame:
        """Fetch data from a specific table"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.db_params["host"],
                port=int(self.db_params["port"]),
                user=self.db_params["username"],
                password=self.db_params["password"],
                dbname=self.db_params["database"]
            )

            query = sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(self.db_params["schema"]),
                sql.Identifier(table_name)
            )

            logger.info(f"Extracting table: {table_name}")
            df = pd.read_sql(query.as_string(conn.cursor()), conn)
            logger.info(f"Rows extracted from {table_name}: {len(df)}")
            return df

        finally:
            if conn:
                conn.close()

    def _file_exists_in_s3(self, key: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=key)
            return True
        except:
            return False

    def _upload_to_s3(self, df: pd.DataFrame, key: str):
        """Upload DataFrame as Parquet to S3"""
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        
        self.s3.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=buffer.getvalue()
        )
        logger.info(f"Uploaded: s3://{self.s3_bucket}/{key}")

    def execute(self, skip_existing: bool = True) -> OperationMetadata:
        """Execute PostgreSQL to S3 extraction"""
        metadata = OperationMetadata(
            operation_name="Postgres_To_S3_Full_Schema",
            start_time=datetime.now()
        )

        table_details = []
        total_rows = 0

        try:
            logger.info("Starting PostgreSQL to S3 extraction")
            tables = self._get_all_tables()
            metadata.records_processed = len(tables)

            schema = self.db_params["schema"]

            for table in tables:
                table_start = time.time()
                s3_key = f"{schema}/{table}.parquet"

                # Check if file exists
                if skip_existing and self._file_exists_in_s3(s3_key):
                    logger.info(f"Skipping {table} — already exists in S3")
                    table_details.append({
                        "table": table,
                        "status": "SKIPPED",
                        "reason": "Already exists"
                    })
                    continue

                try:
                    # Extract and upload
                    df = self._fetch_table_data(table)
                    row_count = len(df)
                    total_rows += row_count

                    self._upload_to_s3(df, s3_key)
                    
                    table_duration = time.time() - table_start
                    metadata.records_success += 1
                    
                    table_details.append({
                        "table": table,
                        "status": "SUCCESS",
                        "rows": row_count,
                        "duration_seconds": round(table_duration, 2),
                        "s3_location": f"s3://{self.s3_bucket}/{s3_key}"
                    })

                except Exception as e:
                    metadata.records_failed += 1
                    logger.error(f"Failed for table {table}: {e}")
                    table_details.append({
                        "table": table,
                        "status": "FAILED",
                        "error": str(e)
                    })

            metadata.complete(status="SUCCESS" if metadata.records_failed == 0 else "PARTIAL")
            metadata.additional_info = {
                "total_rows_extracted": total_rows,
                "schema": schema,
                "table_details": table_details
            }

        except Exception as e:
            metadata.complete(status="FAILED", error=str(e))
            logger.error(f"PostgreSQL extraction failed: {e}")

        metadata.log_summary()
        return metadata


# ===================================
# MAIN ORCHESTRATION
# ===================================
def main():
    """Main orchestration function"""
    pipeline_start = datetime.now()
    all_metadata = []

    logger.info("=" * 80)
    logger.info("STARTING ETL PIPELINE")
    logger.info("=" * 80)

    # ===================================
    # 1. S3 INCREMENTAL COPY
    # ===================================

    s3_copier = S3IncrementalCopy(
        source_bucket="core-telecoms-data-lake",
        target_bucket="core-telecoms-dl-fatima-idris",
        region_name="eu-north-1"
    )
    
    for prefix in ["customers", "social_medias", "call logs"]:
        metadata = s3_copier.execute(prefix=prefix)
        all_metadata.append(metadata)

    # ===================================
    # 2. GOOGLE SHEETS TO S3
    # ===================================

    # sheets_extractor = GoogleSheetsToS3(
    #     service_account_file="/home/fatima/Downloads/service_account.json",
    #     spreadsheet_id="1UaXPsXbQPOv3RiTswTulbeLSAtKjM1BvyGzLFIpDJKU",
    #     s3_bucket="core-telecoms-dl-fatima-idris",
    #     region_name="eu-north-1"
    # )
    # 
    # metadata = sheets_extractor.execute(
    #     sheet_name="agents",
    #     s3_key="coretelecomms_agents/agents.parquet"
    # )
    # all_metadata.append(metadata)

    # ===================================
    # 3. POSTGRES TO S3
    # ===================================
    postgres_extractor = PostgresToS3(
        ssm_prefix="/coretelecomms/database/",
        s3_bucket="core-telecoms-dl-fatima-idris",
        region_name="eu-north-1"
    )
    
    metadata = postgres_extractor.execute(skip_existing=True)
    all_metadata.append(metadata)

    # ===================================
    # FINAL PIPELINE SUMMARY
    # ===================================
    pipeline_end = datetime.now()
    pipeline_duration = (pipeline_end - pipeline_start).total_seconds()

    logger.info("")
    logger.info("=" * 80)
    logger.info("FINAL PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Pipeline Start: {pipeline_start.isoformat()}")
    logger.info(f"Pipeline End: {pipeline_end.isoformat()}")
    logger.info(f"Total Duration: {pipeline_duration:.2f} seconds")
    logger.info(f"Total Operations: {len(all_metadata)}")
    
    successful_ops = sum(1 for m in all_metadata if m.status == "SUCCESS")
    failed_ops = sum(1 for m in all_metadata if m.status == "FAILED")
    
    logger.info(f"Successful Operations: {successful_ops}")
    logger.info(f"Failed Operations: {failed_ops}")
    logger.info("=" * 80)

    # Save metadata to JSON file
    metadata_file = f"pipeline_metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(metadata_file, 'w') as f:
        json.dump([m.to_dict() for m in all_metadata], f, indent=2)
    
    logger.info(f"Metadata saved to: {metadata_file}")


if __name__ == "__main__":
    main()