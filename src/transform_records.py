import dbm
import pickle

from constants import DATA


def read_records_from_dbcache() -> list[dict]:
    """
    Script to read contents of the api_cache.db file
    created by create_api_cache.py

    Returns:
        list[dict]: list of the key-value pairs stored in the dbm cache - keys = search sets, values = pages of JSON records, up to 1000 records each page
    """

    with dbm.open(DATA.CACHE, "r") as db:
        return [
            {search_name.decode('utf-8'): pickle.loads(value)}
            for search_name, value in db.items()
        ]
    

def transform_record_to_ead(record: dict) -> dict:
    """
    transform each record in the pages into an EAD dictionary
    
    Args:
        record (dict): JSON record from the API

    Returns:
        dict: EAD-structured dictionary
    """
    return {
            'archdesc': {
                'id': record['id'],
                'level': "file",
                'did': {
                    'unitid': {
                        'label': "Reference Code",
                        'encodinganalog': "3.1.1",
                        'id': record['id'],
                        '#text': record['citableReference']
                    },
                    'unittitle': {
                        'encodinganalog': "3.1.2",
                        '#text': record['title']
                    },
                    'unitdate': {
                        'encodinganalog': "3.1.2",
                        'normal': f"{record['coveringFromDate']}/{record['coveringToDate']}",
                        '#text': record['coveringDates']
                    },
                    'origination': {
                        'encodinganalog': "3.2.1",
                        'persname' : "",
                        '#text': record['creatorName'][0]['xReferenceName']
                    },
                    'physdesc': {
                        'encodinganalog': "3.1.5",
                        'label': 'extent',
                        'extent': {},
                        'genreform': record['physicalDescription']
                    },
                    'langmaterial': {
                        'language': record['language']
                    },
                    'note': {
                        'language': record['publicationNote'][0]
                    },
                    'materialspec': {
                        'encodinganalog': "3.1.5",
                        'label': "Web Source",
                        'extptr': f"https://discovery.nationalarchives.gov.uk/details/r/{record['id']}",
                        'genreform': record['physicalDescription']
                    }
                },
                'scopecontent': {
                    'encodinganalog': "3.3.1",
                    'p': record['scopeContent']['description']
                },
                'accessrestrict': {
                    'encodinganalog': "3.3.1",
                    'p': record['accessConditions']
                },
                'relatedmaterial': {
                    'encodinganalog': "3.5.3",
                    '#text': record['detailedRelatedMaterial'][0]['description']
                }
            }
        }

