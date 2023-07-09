from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
import os

sys.path.append(os.getcwd())

from preprocess.main import Preprocess

POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID")

preprocess = Preprocess(POSTGRES_CONN_ID)

default_args = {
    "owner": "owner-name",
    "depends_on_past": False,
    "email": [".."],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

dag_args = dict(
    dag_id="preprocess",
    default_args=default_args,
    description="event log",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 4),
    catchup=False,
    tags=["example"],
)

with DAG(**dag_args) as dag:

    auth_preprocessing_task = PythonOperator(
        task_id="auth_preprocessing",
        python_callable=preprocess.makeDataFrame,
        op_kwargs={"file_name": "auth_events"},
    )

    ingest_auth_into_psql = PythonOperator(
        task_id="auth",
        python_callable=preprocess.writeSQL,
        op_kwargs={"file_name": "auth_events", "table_name": "auth"},
    )

    listen_preprocessing_task = PythonOperator(
        task_id="listen_preprocessing",
        python_callable=preprocess.makeDataFrame,
        op_kwargs={"file_name": "listen_events"},
    )

    ingest_listen_into_psql = PythonOperator(
        task_id="listen",
        python_callable=preprocess.writeSQL,
        op_kwargs={"file_name": "listen_events", "table_name": "listen"},
    )

    page_view_preprocessing_task = PythonOperator(
        task_id="ingest_preprocessing",
        python_callable=preprocess.makeDataFrame,
        op_kwargs={"file_name": "page_view_events"},
    )

    ingest_page_view_into_psql = PythonOperator(
        task_id="page_view",
        python_callable=preprocess.writeSQL,
        op_kwargs={"file_name": "page_view_events", "table_name": "page_view"},
    )

    status_preprocessing_task = PythonOperator(
        task_id="status_preprocessing",
        python_callable=preprocess.makeDataFrame,
        op_kwargs={"file_name": "status_change_events"},
    )

    ingest_status_into_psql = PythonOperator(
        task_id="status",
        python_callable=preprocess.writeSQL,
        op_kwargs={"file_name": "status_change_events", "table_name": "status_change"},
    )

    auth_preprocessing_task >> ingest_auth_into_psql
    listen_preprocessing_task >> ingest_listen_into_psql
    page_view_preprocessing_task >> ingest_page_view_into_psql
    status_preprocessing_task >> ingest_status_into_psql
