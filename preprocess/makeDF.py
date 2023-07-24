import pandas as pd
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class GetS3Hook:
    def __init__(self, conn_s3):
        self.conn_s3 = conn_s3

    def get_s3_hook(self):
        s3hook = S3Hook(self.conn_s3)
        return s3hook


class EventDataframe:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def read_json_file(self, hook, date, id):
        data = hook.read_key(
            key=f"{str(id)}/{date}/{id}_event.json", bucket_name=self.bucket_name
        )
        data = [json.loads(line) for line in data[:-1].split("\n")]
        return pd.DataFrame(data)

    def make_dataframe(self, hook, date, id):
        dataframe = self.read_json_file(hook, date, id)
        dataframe["userAgent"] = dataframe["userAgent"].apply(
            lambda x: f'"{x}"' if not x.startswith('"') else x
        )

        csv_string = dataframe.to_csv(index=False)

        hook.load_string(
            string_data=csv_string,
            key=f"{str(id)}/{date}/{id}_event.csv",
            bucket_name=self.bucket_name,
        )
