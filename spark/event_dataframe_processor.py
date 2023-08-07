from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, dataframe
from datetime import datetime
from schema import schema


class EventDataFrameProcessor(ABC):
    @abstractmethod
    def get_spark_session(self, app_name: str, master: str = "yarn") -> None:
        pass

    @abstractmethod
    def read_json_file(
        self, spark: SparkSession, date: datetime, id: str
    ) -> dataframe.DataFrame:
        pass

    @abstractmethod
    def save_dataframe_as_parquet(
        self,
        date: datetime,
        id: str,
        dataframe: dataframe.DataFrame,
    ) -> None:
        pass

    @abstractmethod
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass


class BaseDataFrameProcessor(EventDataFrameProcessor):
    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name

    def get_spark_session(self, app_name: str, master: str = "yarn") -> SparkSession:
        spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

        return spark

    def read_json_file(
        self, spark: SparkSession, date: datetime, id: str
    ) -> dataframe.DataFrame:
        data = spark.read.json(
            f"s3a://{self.bucket_name}/{id}/{date}/{id}_event.json",
            schema=schema["{id}_events"],
        )

        return data

    def save_dataframe_as_parquet(
        self,
        date: datetime,
        id: str,
        data: dataframe.DataFrame,
    ) -> None:
        data.write.parquet(
            f"s3a://{self.bucket_name}/{id}/{date}/{id}_event.parquet", mode="overwrite"
        )

    @abstractmethod
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass
