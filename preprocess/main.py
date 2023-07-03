import pandas as pd 

from preprocess.postgre import SQLWriter
from preprocess.makeDF import EventDataframe



# PostgreSQL 연결 정보
db_host = 'localhost'
db_port = '8080'
db_name = 'event_log'
db_user = 'postgre'
db_password = '******'


class Preprocess(EventDataframe, SQLWriter):
    def __init__(self, host, port, database, user, password):
        EventDataframe.__init__(self)
        SQLWriter.__init__(self, host, port, database, user, password)

    def makeDF(self):
        auth = self.make_dataframe('auth_events')
        listen = self.make_dataframe('listen_events')
        page_view = self.make_dataframe('page_view_events')
        status_change = self.make_dataframe('status_change')

        auth.to_csv("data/auth_events.csv", index=False)
        listen.to_csv("data/listen_events.csv", index=False)
        page_view.to_csv("data/page_view_events.csv", index=False)
        status_change.to_csv("data/status_change_events.csv", index=False)

        return "End making Dataframe"

    def writeSQL(self):
        auth = pd.read_csv('data/auth_events.csv')
        listen = pd.read_csv('data/listen_events.csv')
        page_view = pd.read_csv('data/page_view_events.csv')
        status_change = pd.read_csv('data/status_change_events.csv')

        conn = self.connect()
        engine = self.create_engine()
        schema = 'testdb'

        self.write_dataframe(auth, 'auth', conn, schema)
        self.write_dataframe(listen, 'listen', conn, schema)
        self.write_dataframe(page_view, 'page_view', conn, schema)
        self.write_dataframe(status_change, 'status_change', conn, schema)

        self.close_connection(conn)

        return "End writing"



    
