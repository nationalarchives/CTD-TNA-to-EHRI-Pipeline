import pytest
import dbm
import pickle


@pytest.fixture()
def cached_records():
    with dbm.open("api_cache.db") as db:
        print(db.keys())
        return {
            key.decode(): pickle.loads(value)
            for key, value in db.items()
        }

