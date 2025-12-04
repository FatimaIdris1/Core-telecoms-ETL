from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime

# Your existing plugin imports
from plugins.s3_copy import run_s3_incremental_copy
from plugins.google_sheets import run_google_sheets_to_s3
from plugins.postgres_extract import run_postgres_to_s3
from plugins.customerload import run_s3_full_load
from plugins.transform import transform_etl

# New Snowflake integration
from plugins.snowflake_load import process_all_folders

# -------------------------------
# Column rename maps
# -------------------------------
cl_rename_map = {
    "call ID": "call_id",
    "customeR iD": "customer_id",
    "COMPLAINT_catego ry": "complaint_category",
    "agent ID": "agent_id",
    "call_start_time": "call_start_time",
    "call_end_time": "call_end_time",
    "resolutionstatus": "resolution_status",
    "callLogsGenerationDate": "call_logs_generation_date"
}

web_rename_map = {
    "Column1": "column_1",
    "request_id": "request_id",
    "customeR iD": "customer_id",
    "COMPLAINT_catego ry": "complaint_category",
    "agent ID": "agent_id",
    "resolutionstatus": "resolution_status",
    "request_date": "request_date",
    "resolution_date": "resolution_date",
    "webFormGenerationDate": "web_form_generation_date"
}

media_rename_map = {
    "complaint_id": "complaint_id",
    "customeR iD": "customer_id",
    "COMPLAINT_catego ry": "complaint_category",
    "agent ID": "agent_id",
    "resolutionstatus": "resolution_status",
    "request_date": "request_date",
    "resolution_date": "resolution_date",
    "media_channel": "media_channel",
    "MediaComplaintGenerationDate": "media_complaint_generation_date"
}

# -------------------------------
# DAG defaults
# -------------------------------
default_args = {
    "owner": "fatima.idris",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2
}

# -------------------------------
# DAG definition
# -------------------------------
with DAG(
    dag_id="core_etl_pipeline",
    description="ETL Pipeline: S3 Copy + Google Sheets + Postgres + Snowflake Load",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["coretelecoms", "etl", "pipeline", "fatima-idris"]
) as dag:

    # -------------------------------------
    # Single Load (run only once)
    # -------------------------------------
    with TaskGroup("single_load", tooltip="One-time data loads") as single_load:
        s3_customers = PythonOperator(
            task_id="s3_full_load_customers",
            python_callable=run_s3_full_load,
            op_kwargs={"prefix": "customers"}
        )

        google_sheet_agents = PythonOperator(
            task_id="google_sheets_agents",
            python_callable=run_google_sheets_to_s3,
            op_kwargs={"sheet_name": "agents", "s3_key": "agents.parquet"}
        )

        s3_customers >> google_sheet_agents

    # -------------------------------------
    # Incremental Load
    # -------------------------------------
    with TaskGroup("incremental_load", tooltip="Recurring incremental extractions") as incremental_load:
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

        postgres_extract = PythonOperator(
            task_id="postgres_extract_all_tables",
            python_callable=run_postgres_to_s3,
            op_kwargs={"skip_existing": True}
        )

        [s3_social_media, s3_call_logs] >> postgres_extract

    # -------------------------------------
    # Transformations
    # -------------------------------------
    with TaskGroup("transformations", tooltip="Data transformations") as transformations:
        call_log_transform = PythonOperator(
            task_id="call_log_transform",
            python_callable=transform_etl,
            op_kwargs={
                "s3_bucket": "coretelecomms-dl",
                "raw_prefix": "call logs/",
                "staging_prefix": "staging/call_logs_cleaned/",
                "rename_map": cl_rename_map
            }
        )

        web_data_transform = PythonOperator(
            task_id="web_data_transform",
            python_callable=transform_etl,
            op_kwargs={
                "s3_bucket": "coretelecomms-dl",
                "raw_prefix": "customer_complaints/",
                "staging_prefix": "staging/web_customer_complaints/",
                "rename_map": web_rename_map
            }
        )

        media_data_transform = PythonOperator(
            task_id="media_data_transform",
            python_callable=transform_etl,
            op_kwargs={
                "s3_bucket": "coretelecomms-dl",
                "raw_prefix": "social_medias/",
                "staging_prefix": "staging/social_media_complaint/",
                "rename_map": media_rename_map
            }
        )

        call_log_transform >> web_data_transform >> media_data_transform

    # -------------------------------------
    # Load into Snowflake
    # -------------------------------------
    with TaskGroup("snowflake_load", tooltip="Database loading of cleaned data") as snowflake_load:
        load_s3_to_snowflake = PythonOperator(
            task_id='load_s3_to_snowflake',
            python_callable=process_all_folders
        )


    # -------------------------------------
    # DAG Dependencies--Change 33
    # -------------------------------------
    single_load >> incremental_load >> transformations >> snowflake_load
