#
# This module will create a database following given instructions.
# Based on corresponding module from week 4.
#

from communicate_db import Connector
from get_path import my_creds_path
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

    def pks_string(self, pks: list[str] = ["ID"]) -> str:
        # Generates the SQL for adding primary key constraint to table
        pk_string = "PRIMARY KEY ("
        for pk in pks:
            pk_string += f"`{pk}`, "
        return pk_string[0:-2] + ")"

    def fks_string(self, fks: list[dict[str, str | list[str]]]) -> str:
        # Generates the SQL for adding foreign key constraint to table
        fk_str = ""
        for fk in fks:
            fk_str += f", FOREIGN KEY (`{fk["fk"]}`) "
            fk_str += f"REFERENCES `{fk["table"]}`("
            for key in fk["key"]:
                fk_str += f"`{key}`, "
            fk_str = fk_str[0:-2] + ")"
            # if row in foreign table is deleted, corresponding
            # rows in current table is too
            fk_str += " ON DELETE CASCADE"
            # if row in foreign table is updated, corresponding
            # rows in current table is too
            fk_str += " ON UPDATE CASCADE"
        return fk_str

    # Adds table to schema
    def add_table(
        self,
        table_name: str,
        headers: list[str],
        datatypes: list[pl.DataType],
        fk_dicts: None | list[dict[str, str | list[str]]] = None,
        pks: list[str] = ["ID"],
    ) -> None:
        fields = ""
        # Adds each field one by one to query
        for i, name in enumerate(headers):
            fields += f"`{name}` {self.pldt_to_sql_type(datatypes[i])},"
        # Adds primary keys
        fields += self.pks_string(pks)
        # Adds foreign keys if any
        if fk_dicts != None:
            fields += self.fks_string(fk_dicts)
        # Creates full query
        create_table_stmnt = f"CREATE TABLE {table_name}({fields});"
        # Drops table if it already exists in the schema, and then adds
        # current version
        self._connector.executeCUD(f"DROP TABLE IF EXISTS {table_name}")
        self._connector.executeCUD(create_table_stmnt)

    # Determine which sql type corresponds to data
    def pldt_to_sql_type(self, pldt: pl.DataType) -> str:
        # Returns a string representation of a datatype matching a
        # polars datatype
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


def create_db(
    tables: list[pl.DataFrame],
    order: list[str],
    pks: dict[str, list[str]],
    fks: dict[None | list[dict]],
) -> None:
    # Create a schema from polars dataframes and data on table ordering and
    # primary / foreign keys
    creds = my_creds_path()
    db = DBFromPl(credentials_file=creds, schema="bikes")
    for name in order:
        df = tables[name]
        db.add_table(name, df.columns, df.dtypes, pks=pks[name], fk_dicts=fks[name])


def create_my_db(tables: list[pl.DataFrame]) -> None:
    # Create database specific to task
    order = table_order_and_keys.get_order()
    pks = table_order_and_keys.get_pks()
    fks = table_order_and_keys.get_fks()
    create_db(tables, order, pks, fks)


def main():
    tables = transform_data.main()
    create_my_db(tables)


if __name__ == "__main__":
    main()
