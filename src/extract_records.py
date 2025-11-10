"""
Version 2.0 of TNA to EHRI ETL pipeline

This will extract sets of individual records from Discovery and save them for later transformation into EAD XML to load into the EHRI Portal 

"""
import requests
from pathlib import Path
from time import sleep

from xlreader import read_file


SERIES_PATH = Path(r"C:\Users\rbruno\OneDrive - The National Archives\Projects\EHRI\Data")

DISCOVERY_API_URI = r"https://discovery.nationalarchives.gov.uk/API/"


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
    for index, candidate in enumerate(candidate_records):
        api_query = f"{DISCOVERY_API_URI}records/v1/details/{candidate['ID']}"
        result = requests.get(api_query)

        if result.status_code == 204:
            print(f"ERROR: Record not found - incorrect record ID {candidate['ID']}")
            continue
        
        records_group.append(result.json())

        group_size = 10
        reached_group_size = ((index + 1) % group_size == 0)
        reached_end_of_records = (index == len(candidate_records) - 1)

        if reached_group_size:
            records_out.extend([records_group])
            records_group = []
            sleep(10)

        elif reached_end_of_records:
            records_out.extend([records_group])
        
    print(f"\tResult: {len(candidate_records)} records retrieved.")

    return records_out
       

def create_series_links(series: str, records_from_api: list[dict]) -> list[str]:
    """
    Each record from the API is returned as a url along with the catalog id 
    e.g., https://discovery.nationalarchives.gov.uk/API/records/v1/details/C9295	C9295
    This is the format required for bulk import into EHRI portal

    Args:
        series (str): reference of the series e.g. "PREM 8" 
        records_from_api (list[dict]): the json results from the api query

    Returns:
        list[str]: a list of urls with its record id
    """    

    return [
        f"{DISCOVERY_API_URI}records/v1/details/{record['id']}\t{record['id']}"
        for record in records_from_api
    ]


def write_tsv(series: str, series_links: list) -> None:
    """
    Write all the urls extractef from Discovery into a tsv-format file whgich will be used to bulk import into EHRI

    Args:
        series (str): reference of the series e.g. "PREM 8" 
        series_links (list): a list of urls with its record id
    """           
    series_file_name = series.replace(" ", "_")

    with open(SERIES_PATH / "output" / f"{series_file_name}.tsv", "w") as output:
        output.write("\n".join(series_links))
        print(f"\tData output to {series_file_name}.tsv\n")
    

if __name__ == "__main__":
    import pprint

    pretty_output = pprint.PrettyPrinter(indent=4)
    if not Path(SERIES_PATH):
        print("Invalid location for series.txt")
        exit()

    for tna_file in Path(F"{SERIES_PATH}").glob("*.xlsx"):
        tna_records: list[dict] = read_records_from_file(tna_file)
        records_for_EHRI = get_records_from_api(tna_records)
        print(f"{len(records_for_EHRI)=}")
