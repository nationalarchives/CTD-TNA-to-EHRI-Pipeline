import shelve
import pprint
import re
import json

from constants import DATA


pretty = pprint.PrettyPrinter(indent=4)

regex = re.compile(r"""<colltype id="(?P<schema_name>.*?)">,<\/colltype>""")


def add_schema_statistics_to_cache() -> None:
    with shelve.open(DATA.CACHE, "c") as shelf:
        if 'schemas' not in shelf:
            shelf['schemas'] = {}
        current_schemas = shelf['schemas']

        for count, record in enumerate(shelf['records'].values(), start=1):
            if not (schema := record['scopeContent']['schema']):
                continue

            schema_name = regex.match(schema)['schema_name']
            if (schema_name in current_schemas) and (record['id'] not in current_schemas[schema_name]):
                current_schemas[schema_name].append(record['id'])
            elif record['id'] in current_schemas[schema_name]:
                continue
            else:
                current_schemas[schema_name] = [record['id']]
            print(f"{count=:<5}{' '*15}{record['id']=}\t{schema_name=}")

        print(
            f"Total records in cache: {count=:<5}\n"
            f"Total schemas found: {len(current_schemas.keys())}\n"
            f"Total records with schemas: {sum(len(ids) for ids in current_schemas.values())}\n"
            f"Schemmas found", end=': \n'
        )

        shelf['schemas'] = current_schemas
        pretty.pprint(shelf['schemas'])

def report_records_with_specific_schemas(schema_names: list[str]) -> None:
    schemas_reported = set()

    with shelve.open(DATA.CACHE, "r") as shelf:
        for record in shelf['records'].values():
            if not (schema := record['scopeContent']['schema']):
                continue

            schema = record['scopeContent']['schema']
            schema_name = regex.match(schema)['schema_name']
            if schema_name not in schema_names or schema_name in schemas_reported:
                continue      

            with open(f"{record['id']}_{schema_name}.json", "a") as output:
                print(f"{record['id']=}\t{schema_name=}")
                json.dump(record, output, indent=4)
            schemas_reported.add(schema_name)


if __name__ == "__main__":
    add_schema_statistics_to_cache()
    # report_records_with_specific_schemas(schemas_found)


