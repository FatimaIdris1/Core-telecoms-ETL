from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email_smtp
from pendulum import datetime

# Your existing plugin imports
from plugins.s3_copy import run_s3_incremental_copy
from plugins.google_sheets import run_google_sheets_to_s3
from plugins.postgres_extract import run_postgres_to_s3
from plugins.customerload import run_s3_full_load
from plugins.transform import transform_etl

# New Snowflake integration
from plugins.snowflake_load import process_all_folders


def notify_failure(context):
    """
    Custom function to send an email alert when a task fails.from airflow.utils.email import send_email_smtp
    """
    task_instance = context.get('task_instance')
    dag = context.get('dag')
    dag_id = dag.dag_id if dag else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url if task_instance else ""

    subject = f"Airflow Task Failed: {dag_id}.{task_id}"
    html_content = f"""
    <h3 style="color:red;">Task Failed in DAG: {dag_id}</h3>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <p><a href="{log_url}">View Task Log</a></p>
    """

    send_email_smtp(
        to="fatimaidris388@gmail.com",
        subject=subject,
        html_content=html_content,
    )


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

agents_rename_map = {
    "iD": "id",
    "NamE": "name",
    "experience": "experience",
    "state": "state"
}

customer_rename_map = {
    "customer_id": "customer_id",
    "name": "name",
    "Gender": "gender",
    "DATE of biRTH": "date_of_birth",
    "signup_date": "signup_date",
    "email": "email",
    "address": "address"
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

        [s3_social_media, s3_call_logs, postgres_extract]

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

        agents_transform = PythonOperator(
            task_id="agents_transform",
            python_callable=transform_etl,
            op_kwargs={
                "s3_bucket": "coretelecomms-dl",
                "raw_prefix": "agents.parquet",
                "staging_prefix": "staging/agents/",
                "rename_map": agents_rename_map
            }
        )

        customers_transform = PythonOperator(
            task_id="customers_transform",
            python_callable=transform_etl,
            op_kwargs={
                "s3_bucket": "coretelecomms-dl",
                "raw_prefix": "customers/",
                "staging_prefix": "staging/customers/",
                "rename_map": customer_rename_map
            }
        )
        

        [call_log_transform, web_data_transform, media_data_transform, agents_transform, customers_transform]


    # -------------------------------------
    # Load into Snowflake
    # -------------------------------------
    with TaskGroup("snowflake_load", tooltip="Database loading of cleaned data") as snowflake_load:
        load_s3_to_snowflake = PythonOperator(
            task_id='load_s3_to_snowflake',
            python_callable=process_all_folders
        )

    send_mail = EmailOperator(
    task_id='send_results',
    to='abdmlk.911@gmail.com',
    from_email='fatimaidris388@gmail.com',
    subject='DAG Completed Successfully',
    html_content="""
        <h3> All Data Successfully Loaded!</h3>
        <p>Hello,</p>

        <p>This is to notify you that the latest data pipeline task has completed 
        successfully. All data has been loaded into <strong>Snowflake</strong> and 
        the <strong>dbt models are now active and up-to-date</strong>.</p>

        <p>You can verify the results in your Snowflake account:</p>
        <ul>
            <li> Data has been fully uploaded</li>
            <li> Schema is refreshed</li>
        </ul>
        
        <br>
        <p>Kind regards,</p>
        <p><strong>Airflow Pipeline</strong></p>
    """,
    conn_id='smtp_conn',
)


    # DAG Dependencies--Change 35

    single_load >> incremental_load >> transformations >> snowflake_load >> send_mail
