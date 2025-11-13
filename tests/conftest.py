import pytest
import dbm
import pickle


@pytest.fixture()
def cached_records():
    """
    uses data from api_cache.db to create fixture
    api_cache.db is created by running create_api_cache.py

    Returns:
        [dict]: name of file containing ids of candidate records mapped to list of the actual records retrieved from Discovery API
    """
    with dbm.open("api_cache.db") as db:
        print(db.keys())
        return {
            key.decode(): pickle.loads(value)
            for key, value in db.items()
        }

