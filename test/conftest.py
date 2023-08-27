import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from spark.silver.event_processors import AuthDataFrameProcessor
from spark.gold.listen_data_join import ListenDataJoinProcessor


@pytest.fixture(scope="module")
def test_processor():
    return AuthDataFrameProcessor("test_bucket")


@pytest.fixture(scope="module")
def test_gold_processor():
    return ListenDataJoinProcessor(
        "test_bucket", "test_host", 6789, "test_database", "test_user", "test_password"
    )


@pytest.fixture(scope="module")
def mock_spark_session():
    spark = SparkSession.builder.master("local[*]").appName("test_app").getOrCreate()
    return spark


@pytest.fixture
def mock_dataframe():
    with patch("pyspark.sql.DataFrame") as mock_df:
        yield mock_df
