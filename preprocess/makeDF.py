import pandas as pd
import json

class EventDataframe:

    def read_json_file(self, file_path):
        with open(file_path) as file:
            data = [json.loads(line) for line in file]
        return pd.DataFrame(data)

    def make_dataframe(self, file_name):
        data = self.read_json_file(file_name)
        dataframe = pd.DataFrame(data)
        return dataframe
