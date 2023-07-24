import pandas as pd
from preprocess.makeDF import GetS3Hook
from preprocess.makeDF import EventDataframe
from preprocess.postgre import SQLWriter


class Preprocess(GetS3Hook, EventDataframe, SQLWriter):
    def __init__(self, db_conn_id, obj_conn_id, bucket_name):
        GetS3Hook.__init__(self, obj_conn_id)
        EventDataframe.__init__(self, bucket_name)
        SQLWriter.__init__(self, db_conn_id, bucket_name)

    def make_df(self, date, id):
        hook = self.get_s3_hook()
        self.make_dataframe(hook, date, id)

    def write_sql(self, date, id, table_name):
        hook = self.get_s3_hook()
        file = self.read_file(hook, date, id)
        conn = self.connect()
        engine = self.create_engine(conn.get_uri())
        self.write_dataframe(file, table_name, engine)
