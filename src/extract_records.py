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

from xlreader import read_file

from constants import DISCOVERY_API_URI, PAUSE_IN_SECONDS, PAGE_SIZE, DATA

pretty = pprint.PrettyPrinter(indent=4)


@lru_cache
def get_api_record(record_id: str) -> dict | None:
    api_query = f"{DISCOVERY_API_URI}/records/v1/details/{record_id}"
    result = requests.get(api_query)
    if result.status_code == 204:
        print(f"ERROR: Record not found - incorrect record ID {record_id}")
        return

    record = result.json()
    print(f"\t\tAPI Record retrieved: {record_id}")
    return record


def get_record_from_local_cache(local_cache: dict, record_id: str) -> dict | None:
    if local_record := [record for record in local_cache['shelf'][local_cache['filename']] if record['id'] == record_id]:
        print(f"\t\tLOCAL record retrieved: {record_id}")
        return local_record[0]
    return


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


def get_record_ids_from_api_with_catalogue_lineage(candidate_records: list[dict], local_cache: dict) -> list[list[str]]:
    records_by_lineage = []
    num_records = 0
    print("Retrieving record lineage:")
    for candidate in candidate_records:
        lineage = []
        record_id = candidate['id'] if 'id' in candidate else candidate['ID']
        while True:
            if record := get_record_from_local_cache(local_cache, record_id):
                lineage.append(record_id)
            elif record := get_api_record(record_id):
                lineage.append(record_id)
                local_cache['shelf'][local_cache['filename']].append(record)
            else:
                continue

            num_records += 1
            if record['catalogueLevel'] == 1:
                break

            record_id = record['parentId']

        sleep(PAUSE_IN_SECONDS)
        print(f"\tLineage for candidate {lineage[0]}: {lineage[1:]}")
        records_by_lineage.append(lineage[::-1])  # reverse order to have top-level first
        
    print(f"\tTotal records retrieved: {num_records}\n")
    
    return records_by_lineage


def get_records_from_api(candidate_records: list[dict]) -> list[list[dict]]: 
    """
    Uses list of records proposed for EHRI transfer and retrieves the full JSON records from Discovery.
    The records are collated into smaller groups to ease EHRI import - each group will be transformed into one EAD XML

    Args:
        candidate_records (list[dict]): rows of data extracted from Excel
    Returns:
        records_retrieved (list[list[dict]]): Discovery JSON records collated into smaller groups
    """
    records_retrieved = []
    page_of_records = []
    total_records_retrieved = 0
    for index, candidate in enumerate(candidate_records):
        record_id = candidate['id'] if 'id' in candidate else candidate['ID']
        
        if record := get_api_record(record_id):
            print(f"\tRetrieving record {index + 1} of {len(candidate_records)}: {record_id}")
            page_of_records.append(record)
            total_records_retrieved += 1
        else:
            continue

        reached_end_of_page = ((index + 1) % PAGE_SIZE == 0)
        reached_end_of_records = (index == len(candidate_records) - 1)

        if reached_end_of_page: 
            records_retrieved.extend([page_of_records])
            page_of_records = []
            sleep(PAUSE_IN_SECONDS)

        elif reached_end_of_records:
            records_retrieved.extend([page_of_records])

    print(f"-> Total records retrieved: {total_records_retrieved}\n")
        
    return records_retrieved
       

def add_record_ids_to_taxonomy(taxonomy: Tree, lineage_items: list[list[str]]) -> None:

    for lineage in lineage_items:
        for index, record_id in enumerate(lineage):
            current_parent = lineage[index - 1] if index > 0 else "root"
            if record_id not in taxonomy:
                taxonomy.create_node(record_id, record_id, parent=current_parent)
    

if __name__ == "__main__":
    import pprint

    pretty_output = pprint.PrettyPrinter(indent=4)
    if not Path(DATA.INPUT):
        print("Invalid location for series.txt")
        exit()


with shelve.open(DATA.CACHE, "c") as shelf:
    if 'TNA taxonomy' not in shelf:
        TNA_taxonomy = Tree()
        TNA_taxonomy.create_node("TNA Catalogue", "root") 
        shelf['TNA taxonomy'] = TNA_taxonomy
    else:
        TNA_taxonomy = shelf['TNA taxonomy']

    for search_file in Path(F"{DATA.INPUT}").glob("*.*"):
        if search_file.suffix == ".xlsx":
            print(f"Processing file: {search_file.name}")
            Discovery_records: list[dict] = read_records_from_file(search_file)

        elif search_file.suffix == ".txt":           
            print(f"Processing file: {search_file.name}")
            Discovery_records: list[dict] = get_series_from_api(search_file.stem)

        else:
            print(f"{search_file.name} must be xlsx or txt")
            continue

        if search_file.name not in shelf:
            shelf[search_file.name] = []

        records_for_EHRI: list[dict] = get_records_from_api(Discovery_records)
        local_records_cache: dict = {'shelf': shelf, 'filename': search_file.name}
        individual_records_with_lineage: list[list[str]] = get_record_ids_from_api_with_catalogue_lineage(Discovery_records, local_records_cache)
        add_record_ids_to_taxonomy(TNA_taxonomy, individual_records_with_lineage)

        shutil.move(search_file, DATA.ARCHIVE / search_file.name)

    shelf['TNA taxonomy'] = TNA_taxonomy

    TNA_taxonomy.show() 

    # for page in all_records:
    #     for record in page:
    #         # pretty.pprint(page)
    #         # for record in page:
    #         print(f"{record['id']=}\t{record['scopeContent']['schema']}")

    print("Processing complete.")

