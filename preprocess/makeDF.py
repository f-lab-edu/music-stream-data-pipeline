import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pyspark.sql import SparkSession, dataframe
from preprocess.schema import schema
import tempfile
from datetime import datetime


class GetS3Hook:
    def __init__(self, conn_s3: str) -> None:
        self.conn_s3 = conn_s3

    def get_s3_hook(self) -> S3Hook:
        s3hook = S3Hook(self.conn_s3)
        return s3hook


class EventDataframe:
    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name

    def get_spark_session(self, app_name: str) -> SparkSession:

        spark = SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()

        return spark

    def read_json_file(
        self, spark: SparkSession, hook: S3Hook, date: datetime, id: str
    ) -> dataframe.DataFrame:
        data = hook.read_key(
            key=f"{id}/{date}/{id}_event.json", bucket_name=self.bucket_name
        )

        data = [json.loads(line) for line in data[:-1].split("\n")]

        dataframe = spark.createDataFrame(
            spark.sparkContext.parallelize(data), schema=schema["auth_events"]
        )

        return dataframe

    def save_dataframe_as_parquet(
        self,
        hook: S3Hook,
        date: datetime,
        id: str,
        dataframe: dataframe.DataFrame,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            dataframe.write.parquet(temp, mode="overwrite")

            # hook.load_string(
            #     string_data=parquet_bytes,
            #     key=f"{str(id)}/{date}/{id}_event.parquet",
            #     bucket_name=self.bucket_name,
            # )

    def make_dataframe(self, hook: S3Hook, date: datetime, id: str) -> None:
        spark = self.get_spark_session("music_streaming")

        dataframe = self.read_json_file(spark, hook, date, id)

        dataframe = dataframe.drop(
            "itemInSession", "sessionId", "zip", "firstName", "lastName"
        )

        if id == "page_view":
            dataframe = dataframe.drop("artist", "song", "duration")

        self.save_dataframe_as_parquet(hook, date, id, dataframe)
