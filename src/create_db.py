#
# This module will create a database following given instructions.
# Based on corresponding module from week 4.
#

from communicate_db import Connector
from os.path import join
import csv
from datetime import datetime
import polars as pl
import transform_data


# Class for creating a database from a polars df using an instance of the
# Connector class
class DBFromPl:
    _connector: Connector

    def __init__(self, credentials_file: str, schema: str):
        # Creates a fresh db using the standard
        # settings defined by Connector
        self._connector = Connector(
            credentials_file=credentials_file, dbname=schema, exists=False
        )

    # Adds table to schema
    def add_table(
        self,
        table_name: str,
        headers: list[str],
        datatypes: list[pl.DataType],
        fk_dicts=None,
        pks: list[str] = ["ID"],
    ) -> None:
        fields = ""
        # Adds each field one by one to query
        for i, name in enumerate(headers):
            fields += f"`{name}` {self.pldt_to_sql_type(datatypes[i])},"
        # Adds primary key
        fields += "PRIMARY KEY ("
        for pk in pks:
            fields += f"`{pk}`, "
        fields = fields[0:-2] + ")"
        if fk_dicts != None:
            for fk in fk_dicts:
                fields += f""", FOREIGN KEY (`{fk["fk"]}`) 
                            REFERENCES `{fk["table"]}`(`{fk["key"]}`)"""
                # if row in foreign table is deleted, corresponding
                # rows in current table is too
                fields += " ON DELETE CASCADE"
                # if row in foreign table is updated, corresponding
                # rows in current table is too
                fields += " ON UPDATE CASCADE"

        # Creates full query
        create_table_stmnt = f"CREATE TABLE {table_name}({fields});"
        print(create_table_stmnt)
        # Drops table if it already exists in the schema, and then adds
        # current version
        self._connector.executeCUD(f"DROP TABLE IF EXISTS {table_name}")
        self._connector.executeCUD(create_table_stmnt)

    # Determine which sql type corresponds to data
    def pldt_to_sql_type(self, pldt: pl.DataType) -> str:
        match pldt:
            case (
                pl.UInt8
                | pl.UInt16
                | pl.UInt32
                | pl.UInt64
                | pl.Int8
                | pl.Int16
                | pl.Int32
                | pl.Int64
                | pl.Int128
            ):
                return "int"
            case pl.String | pl.Categorical | pl.Enum:
                return "VARCHAR(250)"
            case pl.Float32 | pl.Float64 | pl.Decimal:
                return "float"
            case pl.Datetime:
                return "DATETIME"
            case pl.Date:
                return "DATE"
            case _:
                return "VARCHAR(250)"


def main():
    creds = join("Data", "my_db.json")
    db = DBFromPl(credentials_file=creds, schema="bikes")
    # db.make_populated_table("Orders_combined", "orders_combined.csv")
    # os_path = join("data", "orders_combined.csv")
    # with open(os_path, "r") as csv_file:
    #    csv_reader = csv.reader(csv_file)
    #    _ = next(csv_reader)
    #    vals = next(csv_reader)
    #    for val in vals:
    #        print(f"{val} is of sql_type {db.item_to_sql_type(val)}")
    # db.make_populated_tables(
    #    ["customers", "products", "orders"],
    #    ["customers.csv", "products.csv", "orders.csv"],
    #    [
    #        None,
    #        None,
    #        [
    #            {"table": "customers", "key": "id", "fk": "customer"},
    #            {"table": "products", "key": "id", "fk": "product"},
    #        ],
    #    ],
    # )
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
    pks = {
        "customers": ["customer_id"],
        "brands": ["brand_id"],
        "categories": ["category_id"],
        "stores": ["id"],
        "products": ["product_id"],
        "stocks": ["store", "product"],
        "staffs": ["id"],
        "orders": ["order_id"],
        "order_items": ["order", "item_nr"],
    }
    fks = {
        "customers": None,
        "brands": None,
        "categories": None,
        "stores": None,
        "products": [
            {"table": "brands", "key": "brand_id", "fk": "brand_id"},
            {"table": "categories", "key": "category_id", "fk": "category_id"},
        ],
        "stocks": [
            {"table": "stores", "key": "id", "fk": "store"},
            {"table": "products", "key": "product_id", "fk": "product"},
        ],
        "staffs": [
            {"table": "stores", "key": "id", "fk": "store"},
            {"table": "staffs", "key": "id", "fk": "manager_id"},
        ],
        "orders": [
            {"table": "stores", "key": "id", "fk": "store"},
            {"table": "staffs", "key": "id", "fk": "staff"},
            {"table": "customers", "key": "customer_id", "fk": "customer_id"},
        ],
        "order_items": [
            {"table": "orders", "key": "order_id", "fk": "order"},
            {"table": "products", "key": "product_id", "fk": "product_id"},
        ],
    }
    for name in order:
        df = tables[name]
        print(df.head(2))
        db.add_table(name, df.columns, df.dtypes, pks=pks[name], fk_dicts=fks[name])


if __name__ == "__main__":
    main()
