from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email_smtp
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime
from pathlib import Path
from dotenv import load_dotenv

# Importing modules from plugins/
from plugins.s3_copy import run_s3_incremental_copy
from plugins.google_sheets import run_google_sheets_to_s3
from plugins.postgres_extract import run_postgres_to_s3
from plugins.customerload import run_s3_full_load
#from plugins.utilities import OperationMetadata, logger

# ==========================================
# Default DAG Settings
# ==========================================
default_args = {
    "owner": "fatima.idris",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2
}


# ==========================================
# DAG DEFINITION
# ==========================================
with DAG(
    dag_id="core_etl_pipeline",
    description="ETL Pipeline: S3 Copy + Google Sheets + Postgres to S3",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["coretelecoms", "etl", "pipeline", "fatima-idris"]
) as dag:

    # ---------------------------------------------------
    # 1. S3 INCREMENTAL COPY TASKS
    # ---------------------------------------------------

    s3_social_media = PythonOperator(
        task_id="s3_incremental_social_medias",
        python_callable=run_s3_incremental_copy,
        op_kwargs={"prefix": "social_medias"}
    )

    s3_call_logs = PythonOperator(
        task_id="s3_incremental_call_logs",
        python_callable=run_s3_incremental_copy,
        op_kwargs={"prefix": "call logs"}
    )

    s3_customers = PythonOperator(
        task_id="s3_incremental_customers",
        python_callable=run_s3_full_load,
        op_kwargs={"prefix": "customers"}
    )
    # ---------------------------------------------------
    # 2. GOOGLE SHEETS → S3 TASK
    # ---------------------------------------------------
    google_sheet_agents = PythonOperator(
        task_id="google_sheets_agents",
        python_callable=run_google_sheets_to_s3,
        op_kwargs={
            "sheet_name": "agents",
            "s3_key": "agents.parquet"
        }
    )

    # ---------------------------------------------------
    # 3. POSTGRES → S3 TASK
    # ---------------------------------------------------
    postgres_extract = PythonOperator(
        task_id="postgres_extract_all_tables",
        python_callable=run_postgres_to_s3,
        op_kwargs={"skip_existing": True}
    )




    # ==========================================
    # TASK DEPENDENCIES
    # ==========================================

    # All S3 incremental copy tasks run -222
    s3_customers >> [s3_social_media, s3_call_logs] >> google_sheet_agents >> postgres_extract
