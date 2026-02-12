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
       

