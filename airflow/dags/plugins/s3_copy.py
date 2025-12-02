import boto3
import pandas as pd
import io
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from dotenv import load_dotenv

from .utilities import OperationMetadata, logger

# Load environment variables
load_dotenv()


class S3IncrementalCopy:
    def __init__(
        self,
        source_bucket,
        target_bucket,
        region_name="eu-north-1"
    ):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        self.region_name = region_name

        # -------------------------------
        # SOURCE ACCOUNT CREDENTIALS FROM .ENV
        # -------------------------------
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

        # -------------------------------
        # TARGET ACCOUNT CREDENTIALS FROM .ENV
        # -------------------------------
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

    def execute(self, prefix=""):
        metadata = OperationMetadata(
            operation_name=f"S3_Incremental_Copy_{prefix}",
            start_time=datetime.now()
        )

        try:
            #  List source bucket files 
            src_objects = self.source_s3.list_objects_v2(
                Bucket=self.source_bucket,
                Prefix=prefix
            )

            if "Contents" not in src_objects:
                metadata.complete("SUCCESS")
                return metadata

            #  List target existing parquet files 
            tgt_objects = self.target_s3.list_objects_v2(
                Bucket=self.target_bucket,
                Prefix=prefix
            )

            existing_files = {
                obj["Key"] for obj in tgt_objects.get("Contents", [])
            }

            # Process source files
            for obj in src_objects["Contents"]:
                key = obj["Key"]

                if not key.endswith((".csv", ".json")):
                    continue

                metadata.records_processed += 1

                parquet_key = key.rsplit(".", 1)[0] + ".parquet"

                if parquet_key in existing_files:
                    continue

                # Read from SOURCE bucket
                file_data = self.source_s3.get_object(
                    Bucket=self.source_bucket,
                    Key=key
                )
                raw_content = file_data["Body"].read().decode("utf-8")

                # Convert to dataframe
                df = (
                    pd.read_csv(io.StringIO(raw_content))
                    if key.endswith(".csv") else
                    pd.read_json(io.StringIO(raw_content))
                )

                # Convert to Parquet
                table = pa.Table.from_pandas(df)
                buffer = io.BytesIO()
                pq.write_table(table, buffer)

                # Upload to TARGET bucket
                self.target_s3.put_object(
                    Bucket=self.target_bucket,
                    Key=parquet_key,
                    Body=buffer.getvalue()
                )

                metadata.records_success += 1

            metadata.complete("SUCCESS")

        except Exception as e:
            metadata.complete("FAILED", str(e))

        metadata.log_summary()
        return metadata


# Airflow Task Wrapper
def run_s3_incremental_copy(prefix):
    copier = S3IncrementalCopy(
        source_bucket="core-telecoms-data-lake",
        target_bucket="coretelecomms-dl"
    )

    copier.execute(prefix)
