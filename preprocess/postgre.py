import psycopg2
from sqlalchemy import create_engine

class SQLWriter:
    def __init__(self, host, port, database, user, password, schema):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema
    
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
    
    def write_dataframe(self, dataframe, table_name, engine, schema_name='public'):
        dataframe.to_sql(table_name, engine,  schema=schema_name, if_exists='append', index=False)

    def close_connection(self, conn):
        conn.close()
    
    


