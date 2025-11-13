"""
Version 2.0 of TNA to EHRI ETL pipeline

This will extract sets of individual records from Discovery and save them for later transformation into EAD XML to load into the EHRI Portal 

"""
import requests
from pathlib import Path
from time import sleep

from xlreader import read_file


SERIES_PATH = Path(r"C:\Users\rbruno\OneDrive - The National Archives\Projects\EHRI\Data")

DISCOVERY_API_URI = r"https://discovery.nationalarchives.gov.uk/API"


def read_records_from_file(tna_file: Path) -> list[dict]:  
    """reads data from file as a dictionary where each key, value refers to one sheet and its rows (a list of tuples) 
    converts to a list of dictionaries, using the first row as the header

    Args:
        tna_file (Path): input Excel created by Archives Sector Leadership (at time of writing Caroline Catchpole)

    Returns:
        list[dict]: 
    """
    file_data = read_file(tna_file)
    sheet_name = list(file_data.keys())[0]
    data_rows = file_data[sheet_name]
    return [
        dict(zip(data_rows[0], row))
        for row in data_rows[1:]
        if row[1]
    ]


def get_records_from_api(candidate_records: list[dict]) -> list[list[dict]]: 
    """
    Uses list of records proposed for EHRI transfer and retrieves the full JSON records from Discovery.
    The records are collated into smaller groups to ease EHRI import - each group will be transformed into one EAD XML

    Args:
        candidate_records (list[dict]): rows of data extracted from Excel
    Returns:
        records_out (list[list[dict]]): Discovery JSON records collated into smaller groups
    """
    records_out = []
    records_group = []
    total_records_retrieved = 0
    for index, candidate in enumerate(candidate_records):
        api_query = f"{DISCOVERY_API_URI}/records/v1/details/{candidate['ID']}"
        result = requests.get(api_query)

        if result.status_code == 204:
            print(f"ERROR: Record not found - incorrect record ID {candidate['ID']}")
            continue
        
        print(f"\tRetrieving record {index + 1} of {len(candidate_records)}: {candidate['ID']}")
        records_group.append(result.json())
        total_records_retrieved += 1

        group_size = 1000
        reached_group_size = ((index + 1) % group_size == 0)
        reached_end_of_records = (index == len(candidate_records) - 1)

        if reached_group_size:
            records_out.extend([records_group])
            records_group = []
            sleep(2)

        elif reached_end_of_records:
            records_out.extend([records_group])

    print(f"-> Total records retrieved: {total_records_retrieved}\n")
        
    return records_out
       

if __name__ == "__main__":
    import pprint

    pretty_output = pprint.PrettyPrinter(indent=4)
    if not Path(SERIES_PATH):
        print("Invalid location for series.txt")
        exit()

    for tna_file in Path(F"{SERIES_PATH}").glob("*.xlsx"):
        print(f"Processing file: {tna_file.name}")
        tna_records: list[dict] = read_records_from_file(tna_file)
        records_for_EHRI = get_records_from_api(tna_records)
