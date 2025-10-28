''' Assumes a series.txt file in the active directory which contains a list of series to be processed (one on each line) and an output folder within the active directory.'''

import requests
from pathlib import Path
from time import sleep

SERIES_PATH = Path(r"C:\Users\rbruno\OneDrive - The National Archives\Projects\EHRI\Data")
SERIES_FILE = SERIES_PATH / "series.txt"

DISCOVERY_API_URI = r"https://discovery.nationalarchives.gov.uk/API/"


def get_records_from_api(series: str) -> list[dict]:
    """
    Queries the Discovery API for all the records in a given series, in order, 
    uses nextBatchMark value to determine whether more records need to be retrieved. 

    Args:
        series (str): reference of the series e.g. "PREM 8"

    Returns:
        list[dict]: results in json
    """    

    batch_mark="*"
    records = []
    while batch_mark:
        api_query = f"{DISCOVERY_API_URI}search/records?sps.recordSeries={series}&sps.searchQuery=*&sps.sortByOption=REFERENCE_ASCENDING&sps.resultsPageSize=1000&sps.batchStartMark={batch_mark}"   
        result = requests.get(api_query)
        if result.status_code != 200:
            return

        data = result.json()          
        records.extend(data['records'])
        batch_mark = data['nextBatchMark']
        sleep(1)
        
    print(f"Results for {series} retrieved with {len(records)} records.")

    return records
       

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
    

if __name__ == "__main__":
    ''' reads the series.txt file and takes the value on each line as a series and processes it '''
    with open(SERIES_FILE, "r") as input:
        for series in input.read().splitlines():
            json_results = get_records_from_api(series)
            links = create_series_links(series, json_results)
            write_tsv(series, links)