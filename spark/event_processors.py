from event_dataframe_processor import BaseDataFrameProcessor
from pyspark.sql import dataframe
from datetime import datetime


class AuthDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass


class PageViewDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass


class ListenDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass


class StatusDataFrameProcessor(BaseDataFrameProcessor):
    def preprocess(self, dataframe: dataframe.DataFrame) -> dataframe.DataFrame:
        pass
