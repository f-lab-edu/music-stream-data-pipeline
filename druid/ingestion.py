from druid.schema import schema
import sys
import requests


def main() -> None:

    broker_host = sys.argv[1]
    broker_port = sys.argv[2]
    endpoint_url = sys.argv[3]
    region = sys.argv[4]
    access_key = sys.argv[5]
    secret_key = sys.argv[6]
    bucket_name = sys.argv[7]
    data_source = sys.argv[8]
    data_type = sys.argv[9]
    date = sys.argv[10]

    if len(sys.argv) < 9:
        print(
            "Needs: <broker_host> <broker_port> <endpoint_url> <region> <access_key> <secret_key> <bucket_name> <data_source> <data_type> <date>"
        )
        sys.exit(1)

    druid_url = f"http://{broker_host}:{broker_port}"
    ingest_url = druid_url + "/druid/indexer/v1/task"

    if data_type == "listen":
        s3_input_path = (
            f"s3://{bucket_name}/{data_type}/{data_type}_joined_event/date_id={date}"
        )
    else:
        s3_input_path = (
            f"s3://{bucket_name}/{data_type}/{data_type}_event/date_id={date}"
        )

    data_schema = {
        "dataSource": data_source,
        "timestampSpec": {"column": "ts", "format": "millis"},
        "dimensionsSpec": {"dimensions": schema[f"{data_type}"]},
    }

    ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index",
                "inputSource": {
                    "type": "s3",
                    "prefixes": [s3_input_path],
                    "endpointConfig": {"url": endpoint_url, "signingRegion": region},
                    "properties": {
                        "s3AccessKey": access_key,
                        "s3SecretKey": secret_key,
                    },
                },
                "inputFormat": {"type": "parquet"},
            },
            "tuningConfig": {"type": "index-parallel", "maxRowPerSegment": 1000000},
            "dataSchema": data_schema,
        },
    }

    session = requests.Session()
    session.post(ingest_url, json=ingestion_spec)


if __name__ == "__main__":
    main()
