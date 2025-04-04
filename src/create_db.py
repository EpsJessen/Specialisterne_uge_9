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

    # Adds rows from csv file to table with corresponding headers
    def populate_table(self, table_name: str, headers, rows):
        sql_insert = "INSERT INTO %s ("
        # Adds each field to query
        for header in headers:
            sql_insert += f"`{header}`, "
        sql_insert = sql_insert[:-2]
        # DETERMINEs WHETHER EACH VALUE SHOULD
        # BE EXPLICITLY MARKED AS STRING
        sql_insert += f") VALUES ({self.stringy_substitude(rows[0])})"

        # Determine which rows contain datetimes
        datetime_columns = self.datetime_columns(rows[0])
        # Adds each row to the table
        for row in rows:
            row = [table_name] + row
            # Ensures that datetimes have proper format
            for i in datetime_columns:
                row[i + 1] = datetime.fromisoformat(row[i + 1])
            self._connector.executeCUD(sql_insert % tuple(row))

    # Make and populate a table with data from csv file
    def make_populated_table(self, table_name, file_name, fk_dicts=None):
        # Get data from csv file
        os_path = join("data", file_name)
        with open(os_path, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            rows = list(csv_reader)
        self.add_table(table_name, headers, rows[0], fk_dicts)
        self.populate_table(table_name, headers, rows)

    # Make and populate multiple tables
    def make_populated_tables(self, table_names, table_files, lists_fk_dicts):
        # Make each table sequentially
        for i, name in enumerate(table_names):
            self.make_populated_table(name, table_files[i], lists_fk_dicts[i])

    # Convert csv data to correct format if it is not meant to be string
    def try_convert(self, input):
        try:
            dt_obj = datetime.fromisoformat(input)
            return dt_obj
        except:
            pass
        try:
            return int(input)
        except:
            pass
        try:
            return float(input)
        except:
            return input

    # Determine which columns contain datetime data
    def datetime_columns(self, row):
        columns = []
        for i, item in enumerate(row):
            type_item = self.try_convert(item)
            if isinstance(type_item, datetime):
                columns.append(i)
        return columns

    # Determine if data should be sent to database explicitly as string
    def stringy_substitude(self, row):
        substitudes = ""
        for item in row:
            type_item = self.try_convert(item)
            if isinstance(type_item, (int, float)):
                substitudes += "%s, "
            else:
                substitudes += "'%s', "
        # remove last ,_ from string
        return substitudes[:-2]

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
