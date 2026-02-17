"""
Script to read and summarize contents of the api_cache.db file
created by create_api_cache.py
Must be run in the same directory where api_cache.db is located
"""
import shelve

from constants import DATA


with shelve.open(DATA.CACHE, "r") as db:
    print(db.keys())
    for key, value in db.items():
        # print(f"Series: {key.decode('utf-8')}")
        # records = pickle.loads(value)
        # total_records = 0
        # for group in records:
        #     total_records += len(group)
        #     for record in group:
        #         print(f"\t{record['id']=}\t\t{record['catalogueLevel']=}\t\t{record['parentId']=}")
        if key.decode('utf-8') == 'TNA taxonomy':
            taxonomy: Tree = pickle.loads(value)
            print("Taxonomy structure:")
            taxonomy.show()
            print(f"Total nodes in taxonomy: {len(taxonomy)}")
            formatted_json = json.dumps(json.loads(taxonomy.to_json()), indent=4)
            with open(DATA.OUTPUT / "tna_taxonomy.json", "w", encoding="utf-8") as json_file:
                json_file.write(formatted_json)    
            taxonomy.save2file(DATA.OUTPUT / "tna_taxonomy.txt")