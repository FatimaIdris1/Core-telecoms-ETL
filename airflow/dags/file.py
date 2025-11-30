from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email_smtp
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime
from pathlib import Path
from dotenv import load_dotenv


