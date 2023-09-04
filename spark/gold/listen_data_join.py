from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, dataframe
from pyspark.sql import functions as F
from spark.gold.fact_schema import event_schema


class ListenDataJoinBaseProcessor(ABC):
    @abstractmethod
    def get_spark_session(self, app_name: str, master: str = "yarn") -> None:
        raise NotImplementedError()

    @abstractmethod
    def read_sql(self, spark: SparkSession, query: str) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def select_columns(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def read_event_data(
        self, spark: SparkSession, id: str, date: str
    ) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def join_fact_and_dim_table(
        self,
        fact: dataframe.DataFrame,
        dim: dataframe.DataFrame,
    ) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def add_date_id_column(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def save_dataframe_as_parquet(
        self,
        id: str,
        data: dataframe.DataFrame,
    ) -> None:
        raise NotImplementedError()


class ListenDataJoinProcessor(ListenDataJoinBaseProcessor):
    def __init__(
        self,
        bucket_name: str,
        host: str,
        port: str,
        database: str,
        username: str,
        password: str,
    ) -> None:
        self.bucket_name = bucket_name
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    def get_spark_session(self, app_name: str, master: str = "yarn") -> SparkSession:
        spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

        return spark

    def read_sql(self, spark: SparkSession, query: str) -> dataframe.DataFrame:
        result = (
            spark.read.format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", f"jdbc:postgresql://{self.host}:{self.port}/{self.database}")
            .option("query", query)
            .option("user", self.username)
            .option("password", self.password)
            .load()
        )
        return result

    def select_columns(self, data: dataframe.DataFrame) -> dataframe.DataFrame:

        music = [
            "year",
            "key",
            "key_confidence",
            "loudness",
            "song_hotttnesss",
            "tempo",
            "artist_name",
            "song_id",
            "title",
        ]
        result = data.select(music)
        return result

    def read_event_data(
        self, spark: SparkSession, id: str, date: str
    ) -> dataframe.DataFrame:
        data = spark.read.parquet(
            f"s3a://{self.bucket_name}/{id}/{id}_event",
            schema=event_schema[f"{id}_events"],
        )

        filtered_df = data.filter(data["date_id"] == date)

        return filtered_df

    def join_fact_and_dim_table(
        self,
        fact: dataframe.DataFrame,
        dim: dataframe.DataFrame,
    ) -> dataframe.DataFrame:
        join_condition = (fact["artist"] == dim["artist_name"]) & (
            fact["song"] == dim["title"]
        )
        joined_df = fact.join(dim, join_condition, "inner")
        joined_df = joined_df.drop("artist", "song")

        return joined_df

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

    def save_dataframe_as_parquet(
        self,
        id: str,
        data: dataframe.DataFrame,
    ) -> None:
        if not data.rdd.isEmpty():
            data.write.partitionBy("date_id").parquet(
                f"s3a://{self.bucket_name}/{id}/{id}_joined_event", mode="append"
            )
