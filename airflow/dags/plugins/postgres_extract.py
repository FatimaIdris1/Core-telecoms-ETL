import boto3
import psycopg2
import pandas as pd
import io
import os
from psycopg2 import sql
from datetime import datetime
from dotenv import load_dotenv

from .utilities import OperationMetadata, logger

# Load environment variables
load_dotenv()


# ------------------------------
# Extractor Class
# ------------------------------

class PostgresToS3:
    def __init__(
        self,
        ssm_prefix,
        s3_bucket,
        source_region_name="eu-north-1",
        target_region_name="us-west-2"
    ):
        self.ssm_prefix = ssm_prefix
        self.s3_bucket = s3_bucket
        self.source_region_name = source_region_name
        self.target_region_name = target_region_name

        # Source AWS client
        src_access = os.getenv("SOURCE_AWS_ACCESS_KEY_ID")
        src_secret = os.getenv("SOURCE_AWS_SECRET_ACCESS_KEY")
        if not src_access or not src_secret:
            raise ValueError("Source AWS credentials not found in .env file")
        self.ssm = boto3.client(
            "ssm",
            region_name=source_region_name,
            aws_access_key_id=src_access,
            aws_secret_access_key=src_secret
        )

        # Target AWS client
        tgt_access = os.getenv("AWS_ACCESS_KEY_ID")
        tgt_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not tgt_access or not tgt_secret:
            raise ValueError("Target AWS credentials not found in .env file")
        self.s3 = boto3.client(
            "s3",
            region_name=target_region_name,
            aws_access_key_id=tgt_access,
            aws_secret_access_key=tgt_secret
        )

        # Load Postgres credentials
        self.db_params = self._load_db_credentials()

    def _load_db_credentials(self):
        keys = {
            "host": "db_host",
            "database": "db_name",
            "username": "db_username",
            "password": "db_password",
            "port": "db_port",
            "schema": "table_schema_name"
        }
        params = {}
        for k, key in keys.items():
            full_path = f"{self.ssm_prefix}{key}"
            try:
                response = self.ssm.get_parameter(Name=full_path, WithDecryption=True)
                params[k] = response["Parameter"]["Value"]
                logger.info(f"Successfully loaded parameter: {full_path}")
            except Exception as e:
                logger.error(f"Failed to load parameter {full_path}: {str(e)}")
                raise
        return params

    def execute(self, skip_existing=True):
        metadata = OperationMetadata(
            operation_name="Postgres_To_S3",
            start_time=datetime.now()
        )
        metadata.records_skipped = 0
        metadata.records_success = 0
        skipped_tables = []

        conn = cursor = None
        try:
            logger.info(f"Connecting to PostgreSQL at {self.db_params['host']}")
            conn = psycopg2.connect(
                host=self.db_params["host"],
                port=int(self.db_params["port"]),
                user=self.db_params["username"],
                password=self.db_params["password"],
                dbname=self.db_params["database"]
            )
            cursor = conn.cursor()

            logger.info(f"Fetching tables from schema: {self.db_params['schema']}")
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (self.db_params["schema"],)
            )
            tables = [t[0] for t in cursor.fetchall()]
            metadata.records_processed = len(tables)
            logger.info(f"Found {len(tables)} tables to process")

            # Existing S3 files
            existing_files = set()
            prefix = f"{self.db_params['schema']}/"
            try:
                response = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)
                if "Contents" in response:
                    existing_files = {obj["Key"] for obj in response["Contents"]}
                    logger.info(f"Found {len(existing_files)} existing files in S3")
            except Exception as e:
                logger.warning(f"Could not list S3 objects: {str(e)}")

            # Process tables
            for table in tables:
                key = f"{self.db_params['schema']}/{table}.parquet"
                if skip_existing and key in existing_files:
                    logger.info(f"Skipping existing file: {key}")
                    metadata.records_skipped += 1
                    skipped_tables.append(table)
                    continue

                logger.info(f"Processing table: {table}")
                cursor.execute(sql.SQL("SELECT * FROM {}.{}").format(
                    sql.Identifier(self.db_params["schema"]),
                    sql.Identifier(table)
                ))
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Table {table} has {len(df)} rows and {len(columns)} columns")

                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                self.s3.put_object(Bucket=self.s3_bucket, Key=key, Body=buffer.getvalue())
                logger.info(f"Successfully uploaded {key} to S3")
                metadata.records_success += 1

            metadata.complete("SUCCESS")
            if skipped_tables:
                logger.info(f"Skipped tables: {skipped_tables}")

        except Exception as e:
            logger.error(f"Error during execution: {str(e)}")
            metadata.complete("FAILED", str(e))
        finally:
            if cursor: cursor.close()
            if conn: conn.close()
            logger.info("Database connections closed")

        metadata.log_summary()
        return metadata


# ------------------------------
# Airflow Wrapper
# ------------------------------

def run_postgres_to_s3(skip_existing=True):
    extractor = PostgresToS3(
        ssm_prefix="/coretelecomms/database/",
        s3_bucket="coretelecomms-dl",
        source_region_name="eu-north-1",
        target_region_name="us-west-2"
    )
    extractor.execute(skip_existing)
