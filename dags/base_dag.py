from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os
sys.path.append(os.getcwd())

from preprocess.main import Preprocess


# PostgreSQL 연결 정보
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('DB_NAME')
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_schema = os.environ.get('DB_SCHEMA')

preprocess = Preprocess(db_host, db_port, db_name, db_user, db_password, db_schema)

# Default arguments for the DAG
default_args = {
    'owner': 'owner-name',
    'depends_on_past': False,
    'email': ['..'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30)
}

# Arguments for the DAG
dag_args = dict(
    dag_id="preprocess",
    default_args=default_args,
    description='event log',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 4),
    catchup=False,
    tags=['example'],
)

# Define the DAG
with DAG(**dag_args) as dag:

    # Preprocessing task
    prepro_task = PythonOperator(
        task_id='preprocessing',
        python_callable=preprocess.makeDataFrame
    )

    store_task = PythonOperator(
        task_id='writing',
        python_callable=preprocess.writeSQL
    )

    # Define the task dependencies
    prepro_task >> store_task


