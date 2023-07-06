from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sys
import os

sys.path.append(os.getcwd())

from preprocess.main import Preprocess

POSTGRES_CONN_ID = "db_conn_info"

# PostgreSQL 연결 정보
# db_host = os.environ.get("0.0.0.0")
# db_port = os.environ.get("5432")
# db_name = os.environ.get("postgres")
# db_user = os.environ.get("postgres")
# db_password = os.environ.get("1234")
# db_schema = os.environ.get("event")

preprocess = Preprocess(POSTGRES_CONN_ID)

# Default arguments for the DAG
default_args = {
    "owner": "owner-name",
    "depends_on_past": False,
    "email": [".."],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

# Arguments for the DAG
dag_args = dict(
    dag_id="preprocess",
    default_args=default_args,
    description="event log",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 4),
    catchup=False,
    tags=["example"],
)

# Define the DAG
with DAG(**dag_args) as dag:
    # Preprocessing task
    prepro_task = PythonOperator(
        task_id="preprocessing", python_callable=preprocess.makeDataFrame
    )

    auth = PythonOperator(
        task_id="writing",
        python_callable=preprocess.writeSQL,
        op_kwargs={"file": "auth"},
    )

    # Define the task dependencies
    prepro_task >> auth
