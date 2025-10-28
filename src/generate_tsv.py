''' Assumes a series.txt file in the active directory which contains a list of series to be processed (one on each line) and an output folder within the active directory.'''

import requests
from pathlib import Path
from time import sleep

SERIES_PATH = Path(r"C:\Users\rbruno\OneDrive - The National Archives\Projects\EHRI\Data")
SERIES_FILE = SERIES_PATH / "series.txt"

DISCOVERY_API_URI = r"https://discovery.nationalarchives.gov.uk/API/"


def get_records_from_api(series: str) -> list[dict]:
    """Queries the Discovery API for all the records in a given series, in order, 
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
       

def get_url_tsv(series):
    ''' For a given series, it queries the Discovery API, extracts the id and generates the tsv file for that series which can be copied and pasted into the EHRI portal for processing.
    
        Keyword arguments:
            series - string with the reference of the series e.g. "PREM 8"
            
        Outputs:
            Saves a tsv file to the output folder with the filename being the series name with the spaces replaced by underscores. The TSV contains the url of the record file and then a tab and then id (as a filename) on each line.
    '''
    
    json_results = get_records_from_api(series)
    print(f"Total records found for {series}: {len(json_results)}")
    links = []
    
    for record in json_results:
        url = f"{DISCOVERY_API_URI}records/v1/details/{record['id']}"
        links.append(f"{url}\t{record['id']}")
        
    series_file_name = series.replace(" ", "_")

    with open(Path(SERIES_PATH / "output" / f"{series_file_name}.tsv"), "w") as output:
        output.write("\n".join(links))


if __name__ == "__main__":
    ''' reads the series.txt file and takes the value on each line as a series and processes it '''
    with open(SERIES_FILE, "r") as input:
        for ref in input.read().splitlines():
            get_url_tsv(ref)