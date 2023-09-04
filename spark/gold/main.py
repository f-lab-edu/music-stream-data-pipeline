import os, sys
from listen_data_join import ListenDataJoinProcessor
from typing import Optional
from typing_extensions import Final

access_key: Final[Optional[str]] = os.environ.get("NCLOUD_ACCESS_KEY")
secret_key: Final[Optional[str]] = os.environ.get("NCLOUD_SECRET_KEY")
endpoint_url: Final[Optional[str]] = os.environ.get("NCLOUD_ENDPOINT")


def main() -> None:

    processor_type = sys.argv[1]
    bucket_name = sys.argv[2]
    date = sys.argv[3]
    database_url = sys.argv[4]
    database_name = sys.argv[5]
    database_id = sys.argv[6]
    database_pwd = sys.argv[7]

    if len(sys.argv) < 10:
        print(
            "Needs: <processor_type> <bucket_name> <date> <database_url> <database_name> <database_id> <database_pwd>"
        )
        sys.exit(1)

    processor = ListenDataJoinProcessor(
        bucket_name,
        database_url,
        "5432",
        database_name,
        database_id,
        database_pwd,
    )

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

    query = "select * from test.songs"
    songs_dim = processor.read_sql(spark, query)
    wide_fact = processor.read_event_data(spark, processor_type, date)
    songs_dim = processor.select_columns(songs_dim)
    result = processor.join_fact_and_dim_table(wide_fact, songs_dim)
    result = processor.add_date_id_column(result)
    processor.save_dataframe_as_parquet(processor_type, result)


if __name__ == "__main__":
    main()
