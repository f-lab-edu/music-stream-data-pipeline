import pandas as pd
import json
import pathlib

class PathConfig:
    def __init__(self):
        self.project_path = pathlib.Path(__file__).parent.resolve()
        self.data_path = f"{self.project_path}/data"

class EventDataframe:
    def read_json_file(self, file_path):
        with open(file_path) as file:
            data = [json.loads(line) for line in file]
        return pd.DataFrame(data)

    def make_dataframe(self, file_name):
        data = self.read_json_file(file_name)
        dataframe = pd.DataFrame(data)
        return dataframe
