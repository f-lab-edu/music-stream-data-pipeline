import psycopg2
from sqlalchemy import create_engine
from preprocess.makeDF import EventDataframe


class SQLWriter:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    def connect(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password
        )
        return conn
    
    def create_engine(self):
        engine = create_engine(
            f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        return engine
    
    def write_dataframe(self, dataframe, table_name, conn, schema='public'):
        dataframe.to_sql(table_name, conn, schema=schema, if_exists='append', index=False)

    def close_connection(self, conn):
        conn.close()
    
    


