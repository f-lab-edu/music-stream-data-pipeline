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

    def write_dataframe(self, dataframe, table_name, engine, schema_name="public"):
        dataframe.to_sql(
            table_name, engine, schema=schema_name, if_exists="append", index=False
        )

    # def close_connection(self, conn):
    #     conn.close()
