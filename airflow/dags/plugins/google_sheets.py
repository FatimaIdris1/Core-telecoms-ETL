import gspread
import io
import os
import pandas as pd
import boto3
from google.oauth2.service_account import Credentials
from datetime import datetime
from dotenv import load_dotenv

from .utilities import OperationMetadata, logger

# Load environment variables
load_dotenv()


class GoogleSheetsToS3:
    def __init__(
        self,
        service_account_file,
        spreadsheet_id,
        s3_bucket,
        region_name="us-west-2",
        aws_access_key_id=None,
        aws_secret_access_key=None
    ):
        # ----------------------------
        # GOOGLE SHEETS AUTH
        # ----------------------------
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
        self.gc = gspread.authorize(creds)

        self.spreadsheet_id = spreadsheet_id
        self.s3_bucket = s3_bucket
        self.region_name = region_name

        # ----------------------------
        # AWS AUTH FROM .ENV FILE
        # ----------------------------
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        if not access_key or not secret_key:
            raise ValueError("AWS credentials not found in .env file")

        self.s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def file_exists_in_s3(self, s3_key):
        """Check if file already exists in S3 bucket"""
        try:
            self.s3.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise

    def execute(self, sheet_name, s3_key):
        metadata = OperationMetadata(
            operation_name=f"GoogleSheets_To_S3_{sheet_name}",
            start_time=datetime.now()
        )

        try:
            # Check if file already exists
            if self.file_exists_in_s3(s3_key):
                logger.info(f"File {s3_key} already exists in bucket {self.s3_bucket}")
            
            # Fetch Google Sheet 
            sheet = self.gc.open_by_key(self.spreadsheet_id).worksheet(sheet_name)
            df = pd.DataFrame(sheet.get_all_records())

            # Convert to Parquet 
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)

            # Upload to TARGET S3 
            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=buffer.getvalue()
            )

            metadata.records_processed = len(df)
            metadata.records_success = len(df)
            metadata.complete("SUCCESS")

        except Exception as e:
            metadata.complete("FAILED", str(e))

        metadata.log_summary()
        return metadata


# Wrapper for Airflow Task
def run_google_sheets_to_s3(sheet_name, s3_key):
    extractor = GoogleSheetsToS3(
        service_account_file="/opt/airflow/config/service_account.json",
        spreadsheet_id=os.getenv("GOOGLE_SHEET_ID"),
        s3_bucket="coretelecomms-dl"
    )
    extractor.execute(sheet_name, s3_key)