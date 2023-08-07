from event_dataframe_processor import BaseDataFrameProcessor
from pyspark.sql import dataframe
from datetime import datetime


class AuthDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop("itemInSession", "sessionId", "zip", "firstName", "lastName")
        return data


class PageViewDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop(
            "itemInSession",
            "sessionId",
            "zip",
            "firstName",
            "lastName",
            "artist",
            "song",
            "duration",
        )
        return data


class ListenDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop("itemInSession", "sessionId", "zip", "firstName", "lastName")
        return data


class StatusDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, data: dataframe.DataFrame) -> dataframe.DataFrame:
        data = data.drop("itemInSession", "sessionId", "zip", "firstName", "lastName")
        return data
