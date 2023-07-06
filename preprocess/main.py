import pandas as pd

from preprocess.makeDF import EventDataframe, PathConfig
from preprocess.postgre import SQLWriter
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Preprocess(EventDataframe, SQLWriter, PathConfig):
    def __init__(self, conn_id):
        EventDataframe.__init__(self)
        SQLWriter.__init__(self, conn_id)
        PathConfig.__init__(self)

    def makeDataFrame(self):
        auth = self.make_dataframe(f"{self.data_path}/auth_events")
        listen = self.make_dataframe(f"{self.data_path}/listen_events")
        page_view = self.make_dataframe(f"{self.data_path}/page_view_events")
        status_change = self.make_dataframe(f"{self.data_path}/status_change_events")

        auth.to_csv(f"{self.data_path}/auth_events.csv", index=False)
        listen.to_csv(f"{self.data_path}/listen_events.csv", index=False)
        page_view.to_csv(f"{self.data_path}/page_view_events.csv", index=False)
        status_change.to_csv(f"{self.data_path}/status_change_events.csv", index=False)

    def writeSQL(self, file):
        # filename = f"{self.data_path}/{file}"
        conn = self.connect()
        engine = self.create_engine(conn.get_uri())
        self.write_dataframe(file, "auth", engine, self.schema)

    # def writeSQL(self):
    #     auth = pd.read_csv(self.data_path + "/auth_events.csv")
    #     listen = pd.read_csv(self.data_path + "/listen_events.csv")
    #     page_view = pd.read_csv(self.data_path + "/page_view_events.csv")
    #     status_change = pd.read_csv(self.data_path + "/status_change_events.csv")

    #     conn = self.connect()
    #     engine = self.create_engine()

    #     self.write_dataframe(auth, "auth", engine, self.schema)
    #     self.write_dataframe(listen, "listen", engine, self.schema)
    #     self.write_dataframe(page_view, "page_view", engine, self.schema)
    #     self.write_dataframe(status_change, "status_change", engine, self.schema)

    #     self.close_connection(conn)

    #     return "End writing"
