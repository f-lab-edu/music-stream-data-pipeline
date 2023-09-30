from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, dataframe
from pyspark.sql import functions as F
from spark.silver.schema import schema


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
        id: str,
        dataframe: dataframe.DataFrame,
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def add_date_id_column(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def aggregation(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
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
        id: str,
        data: dataframe.DataFrame,
    ) -> None:
        if not data.rdd.isEmpty():
            data.write.partitionBy("date_id").parquet(
                f"s3a://{self.bucket_name}/{id}/{id}_event", mode="append"
            )

    def add_state_name(
        self, spark: SparkSession, data: dataframe.DataFrame
    ) -> dataframe.DataFrame:
        statecode = spark.read.csv(
            "spark/silver/data/state_codes.csv", header=True, inferSchema=True
        )

        data = data.join(
            statecode, data["state"] == statecode["stateCode"], "left"
        ).drop(statecode["stateCode"])

        return data

    def add_date_id_column(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.withColumn(
            "date_id",
            F.date_format(
                F.to_utc_timestamp(
                    F.from_unixtime(F.col("ts") / 1000, "yyyy-MM-dd"), "EST"
                ),
                "yyyy-MM-dd",
            ),
        )
        return data

    def aggregation(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        pass
