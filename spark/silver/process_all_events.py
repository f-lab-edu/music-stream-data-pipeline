from event_processors import (
    AuthDataFrameProcessor,
    ListenDataFrameProcessor,
    PageViewDataFrameProcessor,
    StatusDataFrameProcessor,
)
import sys
import os
from typing import Optional
from typing_extensions import Final

access_key: Final[Optional[str]] = os.environ.get("access_key")
secret_key: Final[Optional[str]] = os.environ.get("secret_key")
endpoint_url: Final[Optional[str]] = os.environ.get("endpoint_url")


def main() -> None:
    processors = {
        "auth": AuthDataFrameProcessor,
        "listen": ListenDataFrameProcessor,
        "page_view": PageViewDataFrameProcessor,
        "status": StatusDataFrameProcessor,
    }

    if len(sys.argv) < 4:
        print("Needs: <id> <bucket_id> <date>")
        sys.exit(1)

    processor_type = sys.argv[1]
    processor_class = processors.get(processor_type)

    if not processor_class:
        print("Invalid processor type.")
        sys.exit(1)

    processor = processor_class(sys.argv[2])

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

    data = processor.read_json_file(spark, sys.argv[3], processor_type)
    data = processor.add_state_code(spark, data)
    data = processor.drop_table(data)
    processor.save_dataframe_as_parquet(sys.argv[3], processor_type, data)


if __name__ == "__main__":
    main()
