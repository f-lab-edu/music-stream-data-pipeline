import pytest
from preprocess.main import Preprocess
import psycopg2


@pytest.fixture(scope="module")
def preprocess():
    return Preprocess(conn_id="TEST")
