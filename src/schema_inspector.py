import shelve
import pprint
import re

from constants import DATA


pretty = pprint.PrettyPrinter(indent=4)


regex = re.compile(r"""<colltype id="(?P<schema_name>.*?)">,<\/colltype>""")

with shelve.open(DATA.CACHE, "r") as shelf:
    schemas_found = set()
    for count, record in enumerate(shelf['records'].values(), start=1):
        if schema := record['scopeContent']['schema']:
            schema_name = regex.match(schema)['schema_name']
            schemas_found.add(schema_name)
            print(f"{count=:<5}{' '*15}{record['id']=}\t{schema_name=}")

    pretty.pprint(schemas_found)
    print(f"Total records in cache: {count=:<5}\nUnique schemas found: {len(schemas_found)}")
