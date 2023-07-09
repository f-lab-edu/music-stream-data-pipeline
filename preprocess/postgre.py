from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine


class SQLWriter:
    def __init__(self, conn_id):
        self.conn_id = conn_id

    def connect(self):
        conn = PostgresHook(postgres_conn_id=self.conn_id)
        return conn

    def create_engine(self, url):
        engine = create_engine(url)
        return engine

    def write_dataframe(self, dataframe, table_name, engine):
        dataframe.to_sql(table_name, engine, if_exists="append", index=False)
