from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
import pandas as pd
import io


class SQLWriter:
    def __init__(self, conn_id, bucket_name):
        self.conn_id = conn_id
        self.bucket_name = bucket_name

    def connect(self):
        conn = PostgresHook(postgres_conn_id=self.conn_id)
        return conn

    def create_engine(self, url):
        engine = create_engine(url)
        return engine

    def read_file(self, hook, date, id):
        data = hook.read_key(
            key=f"{str(id)}/{date}/{id}_event.csv", bucket_name=self.bucket_name
        )
        # Convert CSV string to DataFrame
        dataframe = pd.read_csv(io.StringIO(data))
        return dataframe

    def write_dataframe(self, dataframe, table_name, engine):
        dataframe.to_sql(table_name, engine, if_exists="append", index=False)
