import pytest
from preprocess.main import Preprocess


@pytest.fixture(scope="module")
def preprocess():
    return Preprocess(conn_id="TEST")
