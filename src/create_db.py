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
import table_order_and_keys


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
                fields += f", FOREIGN KEY (`{fk["fk"]}`) "
                fields += f"REFERENCES `{fk["table"]}` ("
                for key in fk["key"]:
                    fields += f"`{key}`, "
                fields = fields[0:-2] + ")"
                # if row in foreign table is deleted, corresponding
                # rows in current table is too
                fields += " ON DELETE CASCADE"
                # if row in foreign table is updated, corresponding
                # rows in current table is too
                fields += " ON UPDATE CASCADE"

        # Creates full query
        create_table_stmnt = f"CREATE TABLE {table_name}({fields});"
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


def create_db(tables: list[pl.DataFrame], order: list[str], pks:dict[str,list[str]], fks:dict[None|list[dict]]):
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
    for name in order:
        df = tables[name]
        db.add_table(name, df.columns, df.dtypes, pks=pks[name], fk_dicts=fks[name])

def create_my_db(tables: list[pl.DataFrame]) -> None:
    order = table_order_and_keys.get_order()
    pks = table_order_and_keys.get_pks()
    fks = table_order_and_keys.get_fks()
    create_db(tables, order,pks,fks)

def main():
    tables = transform_data.main()
    create_my_db(tables)

if __name__ == "__main__":
    main()
