from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, dataframe
from schema import schema


class EventDataFrameProcessor(ABC):
    @abstractmethod
    def get_spark_session(self, app_name: str, master: str = "yarn") -> None:
        raise NotImplementedError()

    @abstractmethod
    def read_json_file(
        self, spark: SparkSession, date: str, id: str
    ) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def save_dataframe_as_parquet(
        self,
        date: str,
        id: str,
        dataframe: dataframe.DataFrame,
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        raise NotImplementedError()


class BaseDataFrameProcessor(EventDataFrameProcessor):
    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name

    def get_spark_session(self, app_name: str, master: str = "yarn") -> SparkSession:
        spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

        return spark

    def read_json_file(
        self, spark: SparkSession, date: str, id: str
    ) -> dataframe.DataFrame:
        data = spark.read.json(
            f"s3a://{self.bucket_name}/{id}/{date}/{id}_event.json",
            schema=schema[f"{id}_events"],
        )

        return data

    def save_dataframe_as_parquet(
        self,
        date: str,
        id: str,
        data: dataframe.DataFrame,
    ) -> None:
        if not data.rdd.isEmpty():
            data.write.partitionBy(date).parquet(
                f"s3a://{self.bucket_name}/{id}/{id}_event", mode="append"
            )

    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass
