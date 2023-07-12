import pandas as pd
import os


def test_makeDataFrame(preprocess):
    file_name = "example.json"
    file_path = f"{preprocess.data_path}/{file_name}"

    if not os.path.exists(preprocess.data_path):
        os.mkdir(preprocess.data_path)
    with open(file_path, "w") as file:
        file.write('{"name": "Alice", "age": 25}\n{"name": "Bob", "age": 30}')

    preprocess.makeDataFrame(file_name)

    expected_csv_path = f"{preprocess.data_path}/{file_name}.csv"
    assert os.path.exists(expected_csv_path)

    dataframe = pd.read_csv(expected_csv_path)
    assert isinstance(dataframe, pd.DataFrame)
    assert dataframe.columns.tolist() == ["name", "age"]
    assert dataframe.values.tolist() == [["Alice", 25], ["Bob", 30]]
    os.remove(file_path)
    os.remove(expected_csv_path)


def test_write_sql(mocker, preprocess):
    file_name = "example.json"
    table_name = "example_table"

    mock_read_csv = mocker.patch("pandas.read_csv")
    mock_connect = mocker.patch.object(preprocess, "connect")
    mock_create_engine = mocker.patch.object(preprocess, "create_engine")
    mock_to_sql = mocker.patch.object(preprocess, "write_dataframe")

    preprocess.writeSQL(file_name, table_name)

    mock_read_csv.assert_called_once_with(f"{preprocess.data_path}/{file_name}.csv")
    mock_connect.assert_called_once()
    mock_create_engine.assert_called_once_with(mock_connect.return_value.get_uri())
    mock_to_sql.assert_called_once_with(
        mock_read_csv.return_value, table_name, mock_create_engine.return_value
    )
