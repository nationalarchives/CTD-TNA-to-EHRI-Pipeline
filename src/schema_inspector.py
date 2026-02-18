import shelve
import pprint
import re
from collections import Counter

from constants import DATA


pretty = pprint.PrettyPrinter(indent=4)


regex = re.compile(r"""<colltype id="(?P<schema_name>.*?)">,<\/colltype>""")


def report_schema_statistics() -> None:
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


