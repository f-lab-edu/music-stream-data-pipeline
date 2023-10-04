from spark.silver.event_base_processor import BaseDataFrameProcessor
from pyspark.sql import dataframe
from pyspark.sql.functions import window, col, count, min
from pyspark.sql.types import TimestampType


class AuthDataFrameProcessor(BaseDataFrameProcessor):
    def aggregation(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.withColumn(
            "min", window((col("ts") / 1000).cast(TimestampType()), "1 minute")
        )

        data = (
            data.groupBy(
                "min",
                "gender",
                "state",
                "stateName",
                "level",
                "userAgent",
                "city",
                "registration",
                "success",
            )
            .agg(count("*").alias("event_count"), min("ts").cast("long").alias("ts"))
            .drop("min")
        ).coalesce(1)

        return data


class PageViewDataFrameProcessor(BaseDataFrameProcessor):
    def aggregation(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.withColumn(
            "min", window((col("ts") / 1000).cast(TimestampType()), "1 minute")
        )

        data = (
            data.groupBy(
                "min",
                "gender",
                "state",
                "stateName",
                "level",
                "userAgent",
                "city",
                "registration",
                "page",
                "method",
                "status",
                "auth",
            )
            .agg(count("*").alias("event_count"), min("ts").cast("long").alias("ts"))
            .drop("min")
        ).coalesce(1)

        return data


class ListenDataFrameProcessor(BaseDataFrameProcessor):
    def aggregation(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.withColumn(
            "min", window((col("ts") / 1000).cast(TimestampType()), "1 minute")
        )

        data = (
            data.groupBy(
                "min",
                "artist",
                "gender",
                "state",
                "stateName",
                "level",
                "userAgent",
                "city",
                "registration",
                "duration",
            )
            .agg(count("*").alias("event_count"), min("ts").cast("long").alias("ts"))
            .drop("min")
        ).coalesce(1)

        return data


class StatusDataFrameProcessor(BaseDataFrameProcessor):
    def aggregation(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.withColumn(
            "min", window((col("ts") / 1000).cast(TimestampType()), "1 minute")
        )

        data = (
            data.groupBy(
                "min",
                "gender",
                "state",
                "stateName",
                "level",
                "userAgent",
                "city",
                "registration",
            )
            .agg(count("*").alias("event_count"), min("ts").cast("long").alias("ts"))
            .drop("min")
        ).coalesce(1)

        return data
