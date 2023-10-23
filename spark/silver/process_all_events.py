from spark.silver.event_processors import (
    AuthDataFrameProcessor,
    ListenDataFrameProcessor,
    PageViewDataFrameProcessor,
    StatusDataFrameProcessor,
)
import sys
import os
from typing import Optional
from typing_extensions import Final

access_key: Final[Optional[str]] = os.environ.get("NCLOUD_ACCESS_KEY")
secret_key: Final[Optional[str]] = os.environ.get("NCLOUD_SECRET_KEY")
endpoint_url: Final[Optional[str]] = os.environ.get("NCLOUD_ENDPOINT")


def main() -> None:
    processors = {
        "auth": AuthDataFrameProcessor,
        "listen": ListenDataFrameProcessor,
        "page_view": PageViewDataFrameProcessor,
        "status": StatusDataFrameProcessor,
    }

    if len(sys.argv) < 4:
        print("Needs: <processor_type> <bucket_name> <date>")
        sys.exit(1)

    processor_type = sys.argv[1]
    bucket_name = sys.argv[2]
    date = sys.argv[3]

    processor_class = processors.get(processor_type)

    if not processor_class:
        print("Invalid processor type.")
        sys.exit(1)

    processor = processor_class(bucket_name)

    spark = processor.get_spark_session(app_name="spark", master="local[*]")

    sc = spark.sparkContext._jsc.hadoopConfiguration()
    sc.set("fs.s3a.access.key", access_key)
    sc.set("fs.s3a.secret.key", secret_key)
    sc.set("fs.s3a.endpoint", endpoint_url)
    sc.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    data = processor.read_json_file(spark, date, processor_type)
    data = processor.add_state_name(spark, data)
    data = processor.aggregation(data)
    data = processor.add_date_id_column(data)
    processor.save_dataframe_as_parquet(processor_type, data)


if __name__ == "__main__":
    main()
