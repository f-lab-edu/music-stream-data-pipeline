import pandas as pd
import json
import pathlib
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class EventDataframe:
    def __init__(self, conn_s3, bucket_name):
        self.conn_s3 = conn_s3
        self.bucket_name = bucket_name

    def get_s3_hook(self):
        s3hook = S3Hook(self.conn_s3)
        return s3hook

    def read_json_file(self, date, id):
        hook = self.get_s3_hook()
        data = hook.read_key(
            key=f"{str(id)}/{date}/{id}_event.json", bucket_name=self.bucket_name
        )
        data = [json.loads(line) for line in data[:-1].split("\n")]
        return pd.DataFrame(data)

    def make_dataframe(self, date, id):
        dataframe = self.read_json_file(date, id)
        dataframe["userAgent"] = dataframe["userAgent"].apply(
            lambda x: f'"{x}"' if not x.startswith('"') else x
        )
        return dataframe
