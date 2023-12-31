from spark.silver.event_base_processor import BaseDataFrameProcessor
from pyspark.sql import dataframe


class AuthDataFrameProcessor(BaseDataFrameProcessor):
    def drop_table(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop(
            "itemInSession",
            "sessionId",
            "zip",
            "firstName",
            "lastName",
            "lon",
            "lat",
            "userId",
        )
        return data


class PageViewDataFrameProcessor(BaseDataFrameProcessor):
    def drop_table(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop(
            "itemInSession",
            "sessionId",
            "zip",
            "firstName",
            "lastName",
            "artist",
            "song",
            "duration",
            "lon",
            "lat",
            "userId",
        )
        return data


class ListenDataFrameProcessor(BaseDataFrameProcessor):
    def drop_table(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop(
            "itemInSession",
            "sessionId",
            "zip",
            "firstName",
            "lastName",
            "lon",
            "lat",
            "userId",
        )
        return data


class StatusDataFrameProcessor(BaseDataFrameProcessor):
    def drop_table(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop(
            "itemInSession",
            "sessionId",
            "zip",
            "firstName",
            "lastName",
            "lon",
            "lat",
            "userId",
        )
        return data
