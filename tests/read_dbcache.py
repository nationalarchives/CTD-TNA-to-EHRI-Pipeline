"""
Script to read and summarize contents of the api_cache.db file
created by create_api_cache.py
Must be run in the same directory where api_cache.db is located
"""
import dbm
import pickle

from constants import DATA

with dbm.open(DATA.CACHE, "r") as db:
    print(db.keys())
    for key, value in db.items():
        records = pickle.loads(value)
        total_records = 0
        for group in records:
            total_records += len(group)
        print(f"{key.decode()=}: contains {total_records} records in {len(records)} group(s)")


