import os
import pandas as pd
from datetime import datetime
from typing import Optional
from typing_extensions import Final
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {"owner": "airflow"}

POSTGRES_CONN_ID: Final[Optional[str]] = os.environ.get("POSTGRES_CONN_ID")
BUCKET_NAME: Final[Optional[str]] = os.environ.get("BUCKET_NAME")
AIRFLOW_HOME: Final[Optional[str]] = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

URL = "https://github.com/ankurchavda/streamify/raw/main/dbt/seeds/songs.csv"
CSV_FILENAME: Final[Optional[str]] = "songs.csv"

CSV_OUTFILE = f"{AIRFLOW_HOME}/{CSV_FILENAME}"
TABLE_NAME = "songs"


def upload_to_database(csv_file: str) -> None:
    if not csv_file.endswith("csv"):
        raise ValueError("The input file is not in csv format")

    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = create_engine(conn.get_uri())
    dataframe = pd.read_csv(csv_file)
    dataframe.to_sql(
        TABLE_NAME, engine, if_exists="append", index=False, schema="songs"
    )


with DAG(
    dag_id=f"load_songs_dag",
    default_args=default_args,
    description=f"Execute only once to create songs table in PostgreSQL",
    schedule_interval="@once",
    start_date=datetime(2023, 8, 21),
    end_date=datetime(2023, 8, 21),
    catchup=True,
    tags=["streamify"],
) as dag:
    download_songs_file_task = BashOperator(
        task_id="download_songs_file", bash_command=f"curl -sSLf {URL} > {CSV_OUTFILE}"
    )

    upload_to_database_task = PythonOperator(
        task_id="upload_to_database",
        python_callable=upload_to_database,
        op_kwargs={"csv_file": CSV_OUTFILE},
    )

    remove_files_from_local_task = BashOperator(
        task_id="remove_files_from_local",
        bash_command=f"rm {CSV_OUTFILE}",
    )

    (
        download_songs_file_task
        >> upload_to_database_task
        >> remove_files_from_local_task
    )
