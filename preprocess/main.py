import pandas as pd

from preprocess.makeDF import EventDataframe, PathConfig
from preprocess.postgre import SQLWriter


class Preprocess(EventDataframe, SQLWriter, PathConfig):
    def __init__(self, conn_id, schema_name):
        EventDataframe.__init__(self)
        SQLWriter.__init__(self, conn_id, schema_name)
        PathConfig.__init__(self)

    def makeDataFrame(self, file_name):
        file = self.make_dataframe(f"{self.data_path}/{file_name}")
        file.to_csv(f"{self.data_path}/{file_name}.csv", index=False)

    def writeSQL(self, file_name, table_name):
        file = pd.read_csv(f"{self.data_path}/{file_name}.csv")
        conn = self.connect()
        engine = self.create_engine(conn.get_uri())
        self.write_dataframe(file, table_name, engine)
