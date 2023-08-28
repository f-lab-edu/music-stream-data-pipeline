from pyspark.sql.types import (
    StringType,
    DoubleType,
    StructField,
    StructType,
    LongType,
)

event_schema = {
    "listen_events": StructType(
        [
            StructField("artist", StringType(), True),
            StructField("song", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("ts", LongType(), True),
            StructField("auth", StringType(), True),
            StructField("level", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("stateName", StringType(), True),
            StructField("userAgent", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("registration", LongType(), True),
        ]
    ),
}
