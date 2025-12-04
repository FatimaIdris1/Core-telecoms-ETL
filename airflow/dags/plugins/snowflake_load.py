import snowflake.connector
import boto3
import pyarrow.parquet as pq
import io
import logging
from dotenv import load_dotenv
import os

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('snowflake_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# AWS S3 Client
try:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    logger.info("Successfully initialized S3 client")
except Exception as e:
    logger.error(f"Failed to initialize S3 client: {e}")
    raise

BUCKET = "coretelecomms-dl"
PREFIX_ROOT = "staging/"


# Snowflake Connection

def get_sf_conn():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "STAGING")
        )
        logger.debug("Snowflake connection established")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


# Setup processed files tracking table

def setup_processed_files_table():
    logger.info("Setting up PROCESSED_FILES tracking table")
    try:
        conn = get_sf_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS PROCESSED_FILES (
                FOLDER_NAME STRING,
                FILE_NAME STRING,
                LOADED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.close()
        conn.close()
        logger.info("PROCESSED_FILES table setup complete")
    except Exception as e:
        logger.error(f"Failed to setup PROCESSED_FILES table: {e}")
        raise


# S3 folder / file utilities

def get_s3_folders():
    logger.info(f"Fetching folders from S3 bucket: {BUCKET}/{PREFIX_ROOT}")
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX_ROOT, Delimiter="/")
        folders = [p["Prefix"].replace(PREFIX_ROOT, "").replace("/", "") 
                   for p in response.get("CommonPrefixes", [])]
        folders = [f for f in folders if f]  # remove empty
        logger.info(f"Found {len(folders)} folders: {folders}")
        return folders
    except Exception as e:
        logger.error(f"Failed to list S3 folders: {e}")
        raise

def get_all_parquet_files(folder):
    logger.debug(f"Fetching parquet files from folder: {folder}")
    try:
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{PREFIX_ROOT}{folder}/")
        files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]
        logger.info(f"Found {len(files)} parquet files in folder: {folder}")
        return files
    except Exception as e:
        logger.error(f"Failed to list parquet files in folder {folder}: {e}")
        raise


# Snowflake processed file tracking

def is_file_processed(conn, folder, file_name):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) 
            FROM PROCESSED_FILES 
            WHERE FOLDER_NAME = %s AND FILE_NAME = %s
        """, (folder, file_name))
        processed = cur.fetchone()[0] > 0
        cur.close()
        return processed
    except Exception as e:
        logger.error(f"Failed to check if file is processed ({folder}/{file_name}): {e}")
        raise

def mark_file_as_processed(conn, folder, file_name):
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO PROCESSED_FILES (FOLDER_NAME, FILE_NAME) VALUES (%s, %s)
        """, (folder, file_name))
        cur.close()
        logger.debug(f"Marked file as processed: {folder}/{file_name}")
    except Exception as e:
        logger.error(f"Failed to mark file as processed ({folder}/{file_name}): {e}")
        raise


# Create Snowflake table from Parquet schema

def create_table_from_parquet(folder, sample_key):
    logger.info(f"Creating table {folder.upper()} from parquet schema: {sample_key}")
    try:
        buffer = io.BytesIO()
        s3.download_fileobj(BUCKET, sample_key, buffer)
        buffer.seek(0)
        parquet = pq.ParquetFile(buffer)
        schema = parquet.schema.to_arrow_schema()

        columns = []
        for field in schema:
            sf_type = "VARCHAR" if str(field.type) == "string" else (
                      "FLOAT" if "float" in str(field.type) else 
                      "NUMBER" if "int" in str(field.type) else "VARCHAR")
            columns.append(f'"{field.name.upper()}" {sf_type}')

        create_sql = f'CREATE TABLE IF NOT EXISTS "{folder.upper()}" ({",".join(columns)});'
        logger.debug(f"Table creation SQL: {create_sql}")

        conn = get_sf_conn()
        cur = conn.cursor()
        cur.execute(create_sql)
        cur.close()
        conn.close()
        logger.info(f"Table created successfully: {folder.upper()}")
    except Exception as e:
        logger.error(f"Failed to create table from parquet ({folder}): {e}")
        raise


# Load Parquet files into Snowflake

def load_folder_to_snowflake(folder):
    logger.info(f"Starting to load folder into Snowflake: {folder}")
    conn = get_sf_conn()
    parquet_files = get_all_parquet_files(folder)
    
    loaded_count = 0
    skipped_count = 0

    try:
        for file_key in parquet_files:
            file_name = file_key.split("/")[-1]
            if is_file_processed(conn, folder, file_name):
                logger.info(f"Skipping already processed file: {file_name}")
                skipped_count += 1
                continue

            # Load file into Snowflake
            copy_sql = f"""
                COPY INTO "{folder.upper()}"
                FROM @CORETELECOM_DB.CORETELECOM_STAGING.CORETELECOMMS_STAGE/{folder}/{file_name}
                FILE_FORMAT = (FORMAT_NAME = CORETELECOM_DB.CORETELECOM_STAGING.MY_PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
            """
            logger.debug(f"Executing COPY command for file: {file_name}")
            cur = conn.cursor()
            cur.execute(copy_sql)
            cur.close()

            # Mark file as processed
            mark_file_as_processed(conn, folder, file_name)
            logger.info(f"Successfully loaded file: {file_name}")
            loaded_count += 1

        logger.info(f"Folder load complete - {folder}: {loaded_count} loaded, {skipped_count} skipped")
    except Exception as e:
        logger.error(f"Error loading folder {folder}: {e}")
        raise
    finally:
        conn.close()

# -----------------------------
# Main runner
# -----------------------------
def process_all_folders():
    logger.info("=" * 60)
    logger.info("Starting Snowflake S3 data loading process")
    logger.info("=" * 60)
    
    try:
        setup_processed_files_table()
        folders = get_s3_folders()
        
        if not folders:
            logger.warning("No folders found to process")
            return
        
        for folder in folders:
            logger.info(f"Processing folder: {folder}")
            parquet_files = get_all_parquet_files(folder)
            
            if not parquet_files:
                logger.warning(f"No Parquet files found in folder: {folder}")
                continue

            # Use first file to create table schema
            create_table_from_parquet(folder, parquet_files[0])

            # Load all files (skipping already processed)
            load_folder_to_snowflake(folder)
        
        logger.info("=" * 60)
        logger.info("Data loading process completed successfully")
        logger.info("=" * 60)
    except Exception as e:
        logger.critical(f"Process failed with error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    process_all_folders()