"""
Version 2.0 of TNA to EHRI ETL pipeline

This will extract sets of individual records from Discovery and save them for later transformation into EAD XML to load into the EHRI Portal 

"""
import requests
from pathlib import Path
from time import sleep
import shelve
from treelib import Tree
import shutil
from functools import lru_cache
import pprint
from typing import Generator, Iterator

from xlreader import read_file

from constants import DISCOVERY_API_URI, PAUSE_IN_SECONDS, PAGE_SIZE, DATA

pretty = pprint.PrettyPrinter(indent=4)


def touch_cache() -> None:
    with shelve.open(DATA.CACHE, "c") as shelf:
        if 'taxonomy' not in shelf:
            taxonomy = Tree()
            taxonomy.create_node("Catalogue", "root") 
            shelf['taxonomy'] = taxonomy
        if 'records' not in shelf:
            shelf['records'] = {}


def get_series_from_api(series: str) -> list[dict]:
    """
    Queries the Discovery API for all the records in the given series, in order, 
    uses nextBatchMark value to determine whether more records need to be retrieved. 

    Args:
        series (str): reference of the series e.g. "PREM 8"

    Returns:
        list[dict]: results in json
    """    

    batch_mark="*"
    records = []
    while batch_mark:
        api_query = f"{DISCOVERY_API_URI}/search/records?sps.recordSeries={series}&sps.searchQuery=*&sps.sortByOption=REFERENCE_ASCENDING&sps.resultsPageSize={PAGE_SIZE}&sps.batchStartMark={batch_mark}"   
        result = requests.get(api_query)
        if result.status_code != 200:
            return

        data = result.json()          
        records.extend(data['records'])
        batch_mark = data['nextBatchMark']

        reached_end_of_page = ((len(records)) % PAGE_SIZE == 0)
        if reached_end_of_page: 
            sleep(PAUSE_IN_SECONDS)
        
    print(f"\tResult: {len(records)} records retrieved.")

    return records
       

def read_records_from_file(excel_file: Path) -> list[dict]:  
    """
    Reads data from file as a dictionary where each {key, value} refers to one sheet and its rows (a list of tuples) 
    Returns a list of dictionaries, using the first row as the header

    Args:
        excel_file (Path):

    Returns:
        list[dict]: 
    """
    file_data = read_file(excel_file)
    sheet_name = list(file_data.keys())[0]
    data_rows = file_data[sheet_name]
    return [
        dict(zip(data_rows[0], row))
        for row in data_rows[1:]
        if row[1]
    ]


def load_data_from_search_file(search_file: Path) -> list[dict]:
    if search_file.suffix == ".xlsx":
        print(f"Processing file: {search_file.name}")
        return read_records_from_file(search_file)

    elif search_file.suffix == ".txt":           
        print(f"Processing file: {search_file.name}")
        return get_series_from_api(search_file.stem)


def convert_records_to_generator(candidate_records: list[dict]) -> Generator[dict, None, None]:
    for row in candidate_records:
        yield row


@lru_cache
def get_api_record(record_id: str) -> dict | None:
    api_query = f"{DISCOVERY_API_URI}/records/v1/details/{record_id}"
    result = requests.get(api_query)
    if result.status_code == 204:
        print(f"ERROR: Record not found - incorrect record ID {record_id}")
        return

    record = result.json()
    return record


def get_record_from_local_cache(local_cache: list[dict], record_id: str) -> dict | None:
    if local_record := [record for record in local_cache if record['id'] == record_id]:
        print(f"\t\tLOCAL record retrieved: {record_id}")
        return local_record[0]
    return


def get_record_with_lineage(cached_records: dict, record_id: str, total_candidates_processed: int) -> tuple[dict, list, int]:
    new_records_retrieved = {}
    lineage = []
    while True:
        if record := cached_records.get(record_id, None):
            print(f"{' '*4}{'.'*20}record {record_id} retrieved from cache")
            lineage.append(record_id)

        elif record := new_records_retrieved.get(record_id, None):
            print(f"{' '*4}{'.'*20}record {record_id} retrieved this session")
            lineage.append(record_id)

        elif record := get_api_record(record_id):
            lineage.append(record_id)
            print(f"{' '*4}{'.'*20}record {record_id} retrieved from API")
            new_records_retrieved[record_id] = record                

        else:
            continue

        if record['catalogueLevel'] == 1:
            total_candidates_processed += 1
            break
        record_id = record['parentId']
    
    return new_records_retrieved, lineage, total_candidates_processed


def add_lineage_to_taxonomy(taxonomy: Tree, lineage: list[str]) -> Tree:

    for index, record_id in enumerate(lineage):
        current_parent = lineage[index - 1] if index > 0 else "root"
        if record_id not in taxonomy:
            taxonomy.create_node(record_id, record_id, parent=current_parent)

    return taxonomy


def process_candidate_records(candidate_records: Iterator[dict], total_candidates: int, shelf: shelve) -> None:
    total_candidates_processed = 0

    for index, candidate in enumerate(candidate_records, start=1):
        cached_taxonomy = shelf['taxonomy']
        cached_records = shelf['records']

        record_id = candidate['id'] if 'id' in candidate else candidate['ID']
        print(f"{'='*4}processing candidate {index} of {total_candidates}")

        new_records_retrieved, lineage, total_candidates_processed = get_record_with_lineage(cached_records, record_id, total_candidates_processed)

        reached_end_of_page = ((index) % PAGE_SIZE == 0)
        if reached_end_of_page: 
            sleep(PAUSE_IN_SECONDS)

        print(f"{' '*54}Lineage for candidate {lineage[0]}: {lineage[1:]}")
        
        cached_taxonomy = add_lineage_to_taxonomy(cached_taxonomy, lineage[::-1])
        cached_records.update(new_records_retrieved)

        shelf['taxonomy'] = cached_taxonomy
        shelf['records'] = cached_records

    print(f"\tTotal candidate records found and processed: {total_candidates_processed}\n")
    

if __name__ == "__main__":
    if not Path(DATA.INPUT):
        print("Invalid location for series.txt")
        exit()

    touch_cache()

    with shelve.open(DATA.CACHE, "c") as shelf:

        for search_file in Path(F"{DATA.INPUT}").glob("*.*"):
            if not (candidate_records := load_data_from_search_file(search_file)):
                print(f"{search_file.name} must be xlsx or txt")
                continue

            total_candidate_records = len(candidate_records)
            candidate_records = convert_records_to_generator(candidate_records)          
            process_candidate_records(candidate_records, total_candidate_records, shelf)          

            shutil.move(search_file, DATA.ARCHIVE / search_file.name)

        shelf['taxonomy'].show() 
        # for page in all_records:
        #     for record in page:
        #         # pretty.pprint(page)
        #         # for record in page:
        #         print(f"{record['id']=}\t{record['scopeContent']['schema']}")

        print("Processing complete.")

