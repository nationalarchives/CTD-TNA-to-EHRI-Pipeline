from pathlib import Path
import shelve


from cache_builder import read_records_from_file, get_records_from_api, SERIES_PATH

test_data_files = [
    "TNA collections WO 311 German concentration camp staff_ready.xlsx",
    "TNA collections FO 950 Nazi persecution records_ready 1-2111.xlsx",
    "TNA collections rel to German War Criminals FO 371-104142-104156_ready.xlsx"
    ]

with shelve.open("api_cache.db") as shelf:
    for tna_file in test_data_files:
        print(f"Processing file: {tna_file}")
        tna_records: list[dict] = read_records_from_file(Path(SERIES_PATH / tna_file))
        records_for_cache = get_records_from_api(tna_records)

        shelf[tna_file] = records_for_cache

