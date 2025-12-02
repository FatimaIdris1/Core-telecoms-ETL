import boto3
import pandas as pd
import io
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

from .utilities import OperationMetadata, logger

load_dotenv()


class S3FullLoad:
    def __init__(self, source_bucket, target_bucket, region_name="eu-north-1", max_workers=10):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.region_name = region_name
        self.max_workers = max_workers  # Parallel processing threads

        # SOURCE credentials
        source_key = os.getenv("SOURCE_AWS_ACCESS_KEY_ID")
        source_secret = os.getenv("SOURCE_AWS_SECRET_ACCESS_KEY")
        if not source_key or not source_secret:
            raise ValueError("Source AWS credentials not found in .env file")

        self.source_s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=source_key,
            aws_secret_access_key=source_secret
        )

        # TARGET credentials
        target_key = os.getenv("AWS_ACCESS_KEY_ID")
        target_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not target_key or not target_secret:
            raise ValueError("Target AWS credentials not found in .env file")

        self.target_s3 = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION"),
            aws_access_key_id=target_key,
            aws_secret_access_key=target_secret
        )

    def list_all_objects(self, prefix=""):
        """List ALL objects with pagination support"""
        all_objects = []
        continuation_token = None

        while True:
            list_kwargs = {
                "Bucket": self.source_bucket,
                "Prefix": prefix
            }
            if continuation_token:
                list_kwargs["ContinuationToken"] = continuation_token

            response = self.source_s3.list_objects_v2(**list_kwargs)

            if "Contents" in response:
                all_objects.extend(response["Contents"])

            if not response.get("IsTruncated"):
                break

            continuation_token = response.get("NextContinuationToken")

        return all_objects

    def process_single_file(self, key):
        """Process a single CSV file - designed for parallel execution"""
        try:
            if not key.endswith(".csv"):
                return {"status": "skipped", "key": key}

            parquet_key = key.rsplit(".", 1)[0] + ".parquet"

            # Check if already exists in target (optional - remove if full reload)
            try:
                self.target_s3.head_object(Bucket=self.target_bucket, Key=parquet_key)
                logger.info(f"Already exists, skipping: {parquet_key}")
                return {"status": "exists", "key": key}
            except ClientError:
                pass

            # Download CSV
            file_data = self.source_s3.get_object(Bucket=self.source_bucket, Key=key)
            raw_content = file_data["Body"].read().decode("utf-8")

            # Convert to DataFrame
            df = pd.read_csv(io.StringIO(raw_content))

            if df.empty:
                logger.warning(f"Empty CSV: {key}")
                return {"status": "empty", "key": key}

            # Convert to Parquet
            table = pa.Table.from_pandas(df)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression='snappy')

            # Upload to target
            self.target_s3.put_object(
                Bucket=self.target_bucket,
                Key=parquet_key,
                Body=buffer.getvalue()
            )

            logger.info(f"✓ Converted: {key} -> {parquet_key} ({len(df)} rows)")
            return {"status": "success", "key": key, "rows": len(df)}

        except Exception as e:
            logger.error(f"✗ Failed to process {key}: {str(e)}")
            return {"status": "failed", "key": key, "error": str(e)}

    def execute(self, prefix=""):
        metadata = OperationMetadata(
            operation_name=f"S3_Full_Load_{prefix}",
            start_time=datetime.now()
        )

        try:
            logger.info(f"Starting full load for prefix: '{prefix}'")

            # List ALL objects with pagination
            all_objects = self.list_all_objects(prefix)

            if not all_objects:
                logger.info("No files found in source bucket.")
                metadata.complete("SUCCESS")
                return metadata

            # Filter to CSV files only
            csv_files = [obj["Key"] for obj in all_objects if obj["Key"].endswith(".csv")]
            logger.info(f"Found {len(csv_files)} CSV files to process")

            if not csv_files:
                logger.info("No CSV files to process")
                metadata.complete("SUCCESS")
                return metadata

            # Process files in parallel
            results = {"success": 0, "failed": 0, "skipped": 0, "empty": 0, "exists": 0}
            failed_files = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_key = {
                    executor.submit(self.process_single_file, key): key 
                    for key in csv_files
                }

                # Process completed tasks
                for future in as_completed(future_to_key):
                    metadata.records_processed += 1
                    result = future.result()
                    
                    status = result["status"]
                    results[status] = results.get(status, 0) + 1

                    if status == "success":
                        metadata.records_success += 1
                    elif status == "failed":
                        metadata.records_failed += 1
                        failed_files.append(result["key"])

                    # Progress logging every 10 files
                    if metadata.records_processed % 10 == 0:
                        logger.info(
                            f"Progress: {metadata.records_processed}/{len(csv_files)} "
                            f"(Success: {results['success']}, Failed: {results['failed']})"
                        )

            # Final summary
            logger.info(f"Processing complete: {results}")
            
            if failed_files:
                logger.warning(f"Failed files ({len(failed_files)}): {failed_files[:10]}")

            if results["failed"] > 0:
                metadata.complete("PARTIAL_SUCCESS", f"{results['failed']} files failed")
            else:
                metadata.complete("SUCCESS")

        except Exception as e:
            metadata.complete("FAILED", str(e))
            logger.error(f"S3FullLoad failed: {e}")
            metadata.log_summary()
            raise  # Propagate error to Airflow

        metadata.log_summary()
        logger.info("S3FullLoad completed successfully")
        return metadata


# Airflow wrapper
def run_s3_full_load(prefix=""):
    """Airflow task wrapper for S3 full load"""
    logger.info(f"Starting S3 full load for prefix: {prefix}")
    
    loader = S3FullLoad(
        source_bucket="core-telecoms-data-lake",
        target_bucket="coretelecomms-dl",
        max_workers=10  # Adjust based on your resources
    )
    
    result = loader.execute(prefix)
    logger.info(f"S3 full load completed with status: {result.status}")
    
    return result  # Return metadata to Airflow