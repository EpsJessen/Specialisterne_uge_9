#
# This module will load transformed data from database to local database
#
import polars as pl
import transform_data
from get_path import my_creds_path
import json
from table_order_and_keys import get_order


def load_table(data: pl.DataFrame, name: str, credentials) -> None:
    with open(credentials) as json_credentials:
        creds: dict = json.load(json_credentials)
    uri = f"mysql://{creds["user"]}:{creds["passwd"]}@localhost:3306/bikes"
    data.write_database(table_name=name, connection=uri, if_table_exists="append")


def populate_tables(order: list[str], tables: dict[str, pl.DataFrame]):
    creds = my_creds_path()
    for name in order:
        load_table(tables[name], name, creds)


def main():
    order = get_order()
    tables = transform_data.main()
    populate_tables(order, tables)


if __name__ == "__main__":
    main()
