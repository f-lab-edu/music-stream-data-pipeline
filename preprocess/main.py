import pandas as pd
from preprocess.makeDF import EventDataframe
from preprocess.postgre import SQLWriter


class Preprocess(EventDataframe, SQLWriter):
    def __init__(self, db_conn_id, obj_conn_id, bucket_name):
        EventDataframe.__init__(self, obj_conn_id, bucket_name)
        SQLWriter.__init__(self, db_conn_id)

    def make_df(self, date, id):
        data = self.make_dataframe(date, id)
        return data

    def write_sql(self, task_id, table_name, **kwargs):
        file = kwargs["ti"].xcom_pull(task_ids=f"{task_id}")
        conn = self.connect()
        engine = self.create_engine(conn.get_uri())
        self.write_dataframe(file, table_name, engine)
