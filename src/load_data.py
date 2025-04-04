#
# This module will load transformed data from database to local database
#
import polars as pl
import communicate_db
import transform_data
from os.path import join
import json
from table_order_and_keys import get_order


def load_table(data: pl.DataFrame, name: str, credentials) -> None:
    with open(credentials) as json_credentials:
        creds: dict = json.load(json_credentials)
    uri = f"mysql://{creds["user"]}:{creds["passwd"]}@localhost:3306/bikes"
    data.write_database(table_name=name, connection=uri, if_table_exists="append")


def populate_tables(order: list[str]):
    creds = join("Data", "my_db.json")
    tables = transform_data.main()

    for name in order:
        load_table(tables[name], name, creds)

def main():
    order = get_order()
    populate_tables(order)

if __name__ == "__main__":
    main()
