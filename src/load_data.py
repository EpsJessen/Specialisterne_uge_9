#
# This module will load transformed data from database to local database
#
import polars as pl
import communicate_db
import transform_data
from os.path import join
import json


def load_table(data: pl.DataFrame, name: str, credentials) -> None:
    with open(credentials) as json_credentials:
        creds: dict = json.load(json_credentials)
    uri = f"mysql://{creds["user"]}:{creds["passwd"]}@localhost:3306/bikes"
    data.write_database(table_name=name, connection=uri, if_table_exists="append")


def main():
    creds = join("Data", "my_db.json")
    tables = transform_data.main()
    order = [
        "customers",
        "brands",
        "categories",
        "stores",
        "products",
        "stocks",
        "staffs",
        "orders",
        "order_items",
    ]

    for name in order:
        load_table(tables[name], name, creds)


if __name__ == "__main__":
    main()
