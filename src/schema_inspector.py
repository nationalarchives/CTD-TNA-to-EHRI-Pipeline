import shelve
import pprint

from constants import DATA


pretty = pprint.PrettyPrinter(indent=4)

with shelve.open(DATA.CACHE, "r") as shelf:
    schemas_found = set()
    for count, record in enumerate(shelf['records'].values(), start=1):
        if schema := record['scopeContent']['schema']:
            schemas_found.add(schema)
            print(f"{count=:<5}{' '*15}{record['id']=}\t{schema=}")

    print(f"Total records in cache: {count=:<5}")
    pretty.pprint(schemas_found)
    print(f"Unique schemas found: {len(schemas_found)}")
