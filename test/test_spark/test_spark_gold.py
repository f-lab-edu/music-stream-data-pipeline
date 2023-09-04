from unittest.mock import Mock
from pyspark.sql import SparkSession
from spark.gold.fact_schema import event_schema


class TestGoldDataFrameProcessor:
    def test_get_spark_session(self, test_processor):
        spark = test_processor.get_spark_session("test_app", "local[*]")
        assert spark is not None
        assert spark.conf.get("spark.app.name") == "test_app"
        assert spark.conf.get("spark.master") == "local[*]"

    def test_read_sql(self, test_gold_processor):
        mock_spark = Mock(spec=SparkSession)

        mock_spark.read.format.return_value.option.return_value.load.return_value = (
            Mock()
        )
        test_query = "SELECT * FROM test"

        test_gold_processor.read_sql(mock_spark, test_query)
        mock_spark.read.format.assert_called_once_with("jdbc")
        mock_spark.read.format.return_value.option.assert_called_once_with(
            "driver", "org.postgresql.Driver"
        )

    def test_select_columns(self, mock_spark_session, test_gold_processor):
        data = [
            ("2023", "C", 0.8, -6.2, 0.6, 120.0, "Artist1", "ID1", "Song Title 1"),
            ("2022", "G", 0.9, -5.5, 0.7, 130.0, "Artist2", "ID2", "Song Title 2"),
        ]
        columns = [
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
        df = mock_spark_session.createDataFrame(data, columns)

        result = test_gold_processor.select_columns(df)

        assert result.collect() == df.collect()

    def test_read_event_data(self, test_gold_processor, mock_dataframe):
        bucket_name = "test_bucket"
        test_date = "2023-08-25"
        test_id = "listen"
        mock_spark = Mock(spec=SparkSession)
        mock_spark.read.parquet = Mock()
        mock_spark.read.parquet.return_value = mock_dataframe

        test_gold_processor.read_event_data(mock_spark, test_id, test_date)

        mock_spark.read.parquet.assert_called_once_with(
            f"s3a://{bucket_name}/{test_id}/{test_id}_event",
            schema=event_schema[f"{test_id}_events"],
        )

    def test_join_fact_and_dim_table(self, test_gold_processor, mock_spark_session):

        fact_df = mock_spark_session.createDataFrame(
            [
                ("Artist1", "Song Title 1", "Data1"),
                ("Artist2", "Song Title 2", "Data2"),
            ],
            ["artist", "song", "fact_data"],
        )

        dim_df = mock_spark_session.createDataFrame(
            [
                ("Artist1", "Song Title 1", "Genre1"),
                ("Artist2", "Song Title 2", "Genre2"),
            ],
            ["artist_name", "title", "genre"],
        )

        result = test_gold_processor.join_fact_and_dim_table(fact_df, dim_df)

        expected_result = mock_spark_session.createDataFrame(
            [
                ("Data1", "Artist1", "Song Title 1", "Genre1"),
                ("Data2", "Artist2", "Song Title 2", "Genre2"),
            ],
            ["fact_data", "artist_name", "title", "genre"],
        )

        assert result.collect() == expected_result.collect()

    def test_add_date_id_column(self, mock_spark_session, test_gold_processor):
        data = [
            (1630000000000, "Other Columns1", "Other Columns2"),
            (1631000000000, "Other Columns3", "Other Columns4"),
        ]
        columns = ["ts", "col1", "col2"]
        df = mock_spark_session.createDataFrame(data, columns)

        result = test_gold_processor.add_date_id_column(df)

        assert "date_id" in result.columns

        expected_date_id = [
            "2021-08-26",
            "2021-09-07",
        ]
        date_id_values = result.select("date_id").rdd.flatMap(lambda x: x).collect()
        assert date_id_values == expected_date_id

    def test_save_dataframe_as_parquet(self, test_gold_processor, mock_dataframe):
        bucket_name = "test_bucket"
        test_id = "listen"
        mock_data = Mock()
        mock_data.rdd.isEmpty.return_value = False
        mock_dataframe.return_value = mock_data

        test_gold_processor.save_dataframe_as_parquet(test_id, mock_data)

        mock_data.write.partitionBy("date_id").parquet.assert_called_once_with(
            f"s3a://{bucket_name}/{test_id}/{test_id}_joined_event", mode="append"
        )
