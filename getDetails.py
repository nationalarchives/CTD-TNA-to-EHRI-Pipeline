import requests
from pathlib import Path
from time import sleep


# To rather than getting XML files of details - needs to create TSV file with the URLS.

def get_details_XML(reference):
    '''Queries Discovery API for the details of a record for a given record id
    
        Keyword arguments:
            reference - string id of the record
            
        Outputs:
            Writes XML file of results
    
    '''

    url = 'https://discovery.nationalarchives.gov.uk/API/records/v1/details/' + reference
    #print(url)
    header = {"Accept": "application/xml"}
    res = requests.get(url, headers=header)
    
    if res.status_code == 200:
        with open(Path("output", reference+".xml"), "w") as output:
            output.write(res.text)
        return (reference + " details saved to output folder") 
    else:
        return ("Error: " + reference + " details not retrieved")
    

def get_records_from_api(series, results=[], batchmark="*"):
    '''Queries the Discovery API for all the records in a given series, in order, using recursion is there are multiple pages of records
    
        Keyword arguments: 
            series - string with the reference of the series e.g. "PREM 8"
            results - list of existing results, default is empty list
            batchmark - string argument for use with the Discovery query. Specifies the start point of the results when querying multiple pages. Default is "*". 
  
        Output:
            Returns list of results in json 
    '''
    url = "https://discovery.nationalarchives.gov.uk/API/search/records?sps.recordSeries=" + series + "&sps.searchQuery=*&sps.sortByOption=REFERENCE_ASCENDING&sps.resultsPageSize=1000&sps.batchStartMark=" + batchmark   
    res = requests.get(url)
    
    if res.status_code == 200:
        data = res.json()
        
        #with open(Path("output", series +".txt"), "w") as output:
        #    output.write(str(data))
            
        new_results = data["records"]
        num_results = data["count"]
        next_batch_mark = data["nextBatchMark"]
        
        print("Results retrieved with batchmark: " + batchmark + " with " + str(len(new_results)) + " results.")
        
        if len(new_results) == 0:
            return results
        elif len(results) == 0 and num_results < 1000:
            return new_results
        else:
            combined_results = results + new_results
            return get_records_from_api(series, combined_results, next_batch_mark)

def get_url_tsv(series):
    ''' For a given series, it queries the Discovery API, extracts the id and generates the tsv file for that series which can be copied and pasted into the EHRI portal for processing.
    
        Keyword arguments:
            series - string with the reference of the series e.g. "PREM 8"
            
        Outputs:
            Saves a tsv file to the output folder with the filename being the series name with the spaces replaced by underscores. The TSV contains the url of the record file and then a tab and then id (as a filename) on each line.
    '''
    json_results = get_records_from_api(series)
    links = []
    
    for record in json_results:
        url = 'https://discovery.nationalarchives.gov.uk/API/records/v1/details/' + record["id"]
        links.append(url + "\t" + record["id"])
        
    series_file_name = series.replace(" ", "_")

    with open(Path("output", series_file_name +".tsv"), "w") as output:
        output.write("\n".join(links))

    

'''
for file in Path("input").glob("*.txt"):  
    links = []  
    series = file.stem
    with open(file, "r") as input:
        lines = input.readlines()
        for count, line in enumerate(lines):
            #print(line)
            if line.strip() != "":
                if ":" in line:
                    ref = line.split(":")[1]
                else:
                    ref = line    
                
                #print(str(count+1) + "/" + str(len(lines)) + ": " + get_details_XML(ref.strip()))
                url = 'https://discovery.nationalarchives.gov.uk/API/records/v1/details/' + ref.strip()
                links.append(url + "\t" + ref.strip())
                #sleep(3)
                
    with open(Path("output", series +".tsv"), "w") as output:
        output.write("\n".join(links))
'''

''' reads the series.txt file and takes the value on each line as a series and processes it '''
with open(Path("input", "series.txt"), "r") as input:
    series = input.read().splitlines()
    print(series)
    
    for ref in series:
        get_url_tsv(ref)