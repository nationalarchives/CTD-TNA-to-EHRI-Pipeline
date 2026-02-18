import shelve
import pprint
import re
from collections import Counter
import json

from constants import DATA


pretty = pprint.PrettyPrinter(indent=4)

regex = re.compile(r"""<colltype id="(?P<schema_name>.*?)">,<\/colltype>""")


def report_schema_statistics() -> list:
    with shelve.open(DATA.CACHE, "r") as shelf:
        schemas_found = []
        for count, record in enumerate(shelf['records'].values(), start=1):
            if schema := record['scopeContent']['schema']:
                schema_name = regex.match(schema)['schema_name']
                schemas_found.append(schema_name)
                print(f"{count=:<5}{' '*15}{record['id']=}\t{schema_name=}")

        counter = Counter(schemas_found)
        print(
            f"Total records in cache: {count=:<5}\n"
            f"Total schemas found: {len(counter.values())}\n"
            f"Total records with schema: {counter.total()}\n"
            f"Schemmas found", end=': \n'
        )
        pretty.pprint(dict(counter))
    
    return list(counter.keys())


def report_records_with_specific_schemas() -> None:
    schemas_to_find = ["EdenPaper", "FOI", "Miscellaneous", ]

    with shelve.open(DATA.CACHE, "r") as shelf:
        for record in shelf['records'].values():
            if not (schema := record['scopeContent']['schema']):
                continue

            schema = record['scopeContent']['schema']
            schema_name = regex.match(schema)['schema_name']
            if schema_name not in schemas_to_find:
                continue      

            with open(f"{record['id']}_{schema_name}.json", "a") as output:
                print(f"{record['id']=}\t{schema_name=}")
                json.dump(record, output, indent=4)


if __name__ == "__main__":
    # report_schema_statistics()
    report_records_with_specific_schemas()


