from unittest.mock import Mock
from pyspark.sql import SparkSession, dataframe
from spark.silver.schema import schema
from spark.silver.event_processors import AuthDataFrameProcessor


class TestSilverDataFrameProcessor:
    def test_get_spark_session(self, test_processor: AuthDataFrameProcessor) -> None:
        spark = test_processor.get_spark_session("test_app", "local[*]")
        assert spark is not None
        assert spark.conf.get("spark.app.name") == "test_app"
        assert spark.conf.get("spark.master") == "local[*]"

    def test_read_json_file(self, test_processor: AuthDataFrameProcessor) -> None:
        bucket = "test_bucket"
        mock_spark = Mock(spec=SparkSession)
        mock_spark.read.json = Mock()
        test_date = "2023-08-25"
        test_id = "auth"

        test_processor.read_json_file(mock_spark, test_date, test_id)

        mock_spark.read.json.assert_called_once_with(
            f"s3a://{bucket}/{test_id}/{test_date}/{test_id}_event.json",
            schema=schema[f"{test_id}_events"],
        )

    def test_save_dataframe_as_parquet(
        self,
        test_processor: AuthDataFrameProcessor,
        mock_dataframe: dataframe.DataFrame,
    ) -> None:
        mock_data = Mock()
        mock_data.rdd.isEmpty.return_value = False
        mock_dataframe.return_value = mock_data

        test_processor.save_dataframe_as_parquet("test_id", mock_data)

        mock_data.write.partitionBy("date_id").parquet.assert_called_once_with(
            "s3a://test_bucket/test_id/test_id_event", mode="append"
        )

    def test_add_state_name(
        self, test_processor: AuthDataFrameProcessor, mock_spark_session: SparkSession
    ) -> None:

        test_data = mock_spark_session.createDataFrame(
            [("CA", "Los Angeles"), ("NY", "New York"), ("TX", "Houston")],
            ["state", "city"],
        )

        result_df = test_processor.add_state_name(mock_spark_session, test_data)

        expected_data = mock_spark_session.createDataFrame(
            [
                ("CA", "Los Angeles", "California"),
                ("NY", "New York", "New York"),
                ("TX", "Houston", "Texas"),
            ],
            ["state", "city", "stateName"],
        )

        assert result_df.collect() == expected_data.collect()

    def test_drop_table(
        self, test_processor: AuthDataFrameProcessor, mock_spark_session: SparkSession
    ) -> None:
        test_data = mock_spark_session.createDataFrame(
            [
                (1, "A", "B", "12345", "John", "Doe", 12.34, 56.78, "user1"),
                (2, "C", "D", "67890", "Jane", "Smith", 45.67, 89.12, "user2"),
            ],
            [
                "itemInSession",
                "sessionId",
                "zip",
                "firstName",
                "lastName",
                "lon",
                "lat",
                "userId",
            ],
        )

        result_df = test_processor.drop_table(test_data)

        assert "itemInSession" not in result_df.columns
        assert "sessionId" not in result_df.columns
        assert "zip" not in result_df.columns
        assert "firstName" not in result_df.columns
        assert "lastName" not in result_df.columns
        assert "lon" not in result_df.columns
        assert "lat" not in result_df.columns
        assert "userId" not in result_df.columns
