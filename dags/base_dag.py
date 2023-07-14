from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.append(os.getcwd())

from preprocess.main import Preprocess

POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID")
OBJECT_STORAGE_CONN_ID = os.environ.get("OBJECT_STORAGE_CONN_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
today_date = datetime.now().date()

preprocess = Preprocess(
    db_conn_id=POSTGRES_CONN_ID,
    obj_conn_id=OBJECT_STORAGE_CONN_ID,
    bucket_name=BUCKET_NAME,
)

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
        python_callable=preprocess.make_df,
        op_kwargs={"date": f"{today_date}", "id": "auth"},
    )

    ingest_auth_into_psql = PythonOperator(
        task_id="auth",
        python_callable=preprocess.write_sql,
        op_kwargs={"task_id": "auth_preprocessing", "table_name": "auth"},
    )

    listen_preprocessing_task = PythonOperator(
        task_id="listen_preprocessing",
        python_callable=preprocess.make_df,
        op_kwargs={"date": f"{today_date}", "id": "listen"},
    )

    ingest_listen_into_psql = PythonOperator(
        task_id="listen",
        python_callable=preprocess.write_sql,
        op_kwargs={"task_id": "listen_preprocessing", "table_name": "listen"},
    )

    page_view_preprocessing_task = PythonOperator(
        task_id="ingest_preprocessing",
        python_callable=preprocess.make_df,
        op_kwargs={"date": f"{today_date}", "id": "page_view"},
    )

    ingest_page_view_into_psql = PythonOperator(
        task_id="page_view",
        python_callable=preprocess.write_sql,
        op_kwargs={"task_id": "ingest_preprocessing", "table_name": "page_view"},
    )

    status_preprocessing_task = PythonOperator(
        task_id="status_preprocessing",
        python_callable=preprocess.make_df,
        op_kwargs={"date": f"{today_date}", "id": "status_change"},
    )

    ingest_status_into_psql = PythonOperator(
        task_id="status_change",
        python_callable=preprocess.write_sql,
        op_kwargs={"task_id": "status_preprocessing", "table_name": "status_change"},
    )

    auth_preprocessing_task >> ingest_auth_into_psql
    listen_preprocessing_task >> ingest_listen_into_psql
    page_view_preprocessing_task >> ingest_page_view_into_psql
    status_preprocessing_task >> ingest_status_into_psql
