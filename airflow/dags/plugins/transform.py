import os
import io
import logging
from typing import Dict
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import boto3
import pandas as pd

# -----------------------------------------
# CONFIGURATION
# -----------------------------------------
load_dotenv()

class Config:
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    @classmethod
    def validate(cls):
        """Validate required environment variables"""
        required_vars = ['AWS_ACCESS_KEY', 'AWS_SECRET_KEY']
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")


# -----------------------------------------
# LOGGING
# -----------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------------------
# S3 CLIENT SINGLETON
# -----------------------------------------
class S3Client:
    """Singleton S3 client with error handling"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            try:
                cls._instance = boto3.client(
                    "s3",
                    aws_access_key_id=Config.AWS_ACCESS_KEY,
                    aws_secret_access_key=Config.AWS_SECRET_KEY
                )
            except Exception as e:
                logger.error(f"Failed to create S3 client: {e}")
                raise
        return cls._instance

# -----------------------------------------
# ETL OPERATIONS
# -----------------------------------------
class ETLPipeline:
    """Incremental S3 → pandas → S3 ETL Pipeline"""
    
    def __init__(self, s3_bucket: str, raw_prefix: str, 
                 staging_prefix: str, rename_map: Dict[str, str]):
        self.s3_bucket = s3_bucket
        self.raw_prefix = raw_prefix
        self.staging_prefix = staging_prefix
        self.rename_map = rename_map
        self.s3_client = S3Client()
    
    def load_parquet_file(self, key: str) -> pd.DataFrame:
        """Load a single Parquet file from S3"""
        try:
            logger.info(f"Downloading: {key}")
            obj_data = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(obj_data["Body"].read()))
            return df
        except ClientError as e:
            logger.error(f"Failed to download {key}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to parse {key}: {e}")
            raise
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform dataframe"""
        try:
            logger.info(f"Cleaning data: {len(df)} rows before cleaning")
            
            # Rename columns
            if self.rename_map:
                df = df.rename(columns=self.rename_map)
            
            # Remove rows that are completely empty
            df = df.dropna(how="all")
            
            # Remove duplicates
            initial_count = len(df)
            df = df.drop_duplicates()
            duplicates_removed = initial_count - len(df)
            if duplicates_removed > 0:
                logger.info(f"Removed {duplicates_removed} duplicate rows")
            
            # Strip whitespace from string columns
            string_cols = df.select_dtypes(include=['object']).columns
            for col in string_cols:
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            
            logger.info(f"Cleaned data: {len(df)} rows after cleaning")
            
            if df.empty:
                raise ValueError("DataFrame is empty after cleaning")
            
            return df
            
        except Exception as e:
            logger.error(f"Data cleaning failed: {e}")
            raise
    
    def export_to_staging(self, df: pd.DataFrame, staging_key: str):
        """Export cleaned data to S3 staging area"""
        try:
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, compression='snappy')
            buffer.seek(0)
            
            logger.info(f"Uploading to staging: {staging_key}")
            self.s3_client.put_object(Bucket=self.s3_bucket, Key=staging_key, Body=buffer.getvalue())
            logger.info("Successfully uploaded to staging")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during export: {e}")
            raise
    
    def run(self):
        """Execute incremental ETL for all files in raw_prefix"""
        try:
            logger.info("=" * 50)
            logger.info(f"Starting incremental ETL for S3 bucket: {self.s3_bucket}")
            logger.info("=" * 50)
            
            # Validate AWS credentials
            Config.validate()
            
            paginator = self.s3_client.get_paginator("list_objects_v2")
            
            files_processed = 0
            files_skipped = 0
            
            for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.raw_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".parquet"):
                        continue
                    
                    # Determine staging key
                    staging_key = f"{self.staging_prefix}{os.path.basename(key).replace('.parquet','_cleaned.parquet')}"
                    
                    # Skip if already processed
                    try:
                        self.s3_client.head_object(Bucket=self.s3_bucket, Key=staging_key)
                        logger.info(f"Skipping {key}, already processed in staging")
                        files_skipped += 1
                        continue
                    except ClientError:
                        # File not found in staging, process it
                        pass
                    
                    # Load, clean, and export
                    df = self.load_parquet_file(key)
                    cleaned_df = self.clean_data(df)
                    self.export_to_staging(cleaned_df, staging_key=staging_key)
                    files_processed += 1
            
            logger.info(f"ETL finished. {files_processed} files processed, {files_skipped} files skipped.")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error("=" * 50)
            logger.error(f"ETL Failed: {e}")
            logger.error("=" * 50)
            raise

# -----------------------------------------
# MAIN EXECUTION
# -----------------------------------------
def transform_etl(s3_bucket: str, raw_prefix: str, staging_prefix: str, 
                  rename_map: Dict[str, str]):

    pipeline = ETLPipeline(
        s3_bucket=s3_bucket,
        raw_prefix=raw_prefix,
        staging_prefix=staging_prefix,
        rename_map=rename_map
    )
    pipeline.run()
