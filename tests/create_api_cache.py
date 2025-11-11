from pathlib import Path
import shelve


from extract_records import read_records_from_file, get_records_from_api, SERIES_PATH

test_data_files = [
    "TNA collections WO 311 German concentration camp staff_ready.xlsx",
    "TNA collections FO 950 Nazi persecution records_ready.xlsx"
    ]

with shelve.open("api_cache.db") as shelf:
    for tna_file in test_data_files:
        tna_records: list[dict] = read_records_from_file(Path(SERIES_PATH / tna_file))
        records_for_EHRI = get_records_from_api(tna_records)

        print(f"{len(records_for_EHRI)=}")

        shelf[tna_file] = records_for_EHRI
    # shelf["last_updated"] = 1696846049.8469703
    # shelf["user_sessions"] = {
    #     "jdoe@domain.com": {
    #         "user_id": 4185395169,
    #         "roles": {"admin", "editor"},
    #         "preferences": {
    #             "language": "en_US",
    #             "dark_theme": False
    #         }
    #     }
    # }
