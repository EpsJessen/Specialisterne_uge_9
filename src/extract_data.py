import polars as pl
from extract_api import extract_api
from extract_db import extract_db
from extract_csv import extract_csv
from enum import Enum
import get_path


class TableTypes(Enum):
    CSV = 1
    API = 2
    DB = 3
    NOT_SET = 4


def extract(
    table: str, type: TableTypes = TableTypes.NOT_SET, **kwargs
) -> pl.DataFrame:
    """Extracts table from either csv, db or api in the for of a dataframe

    Args:
        table (str): the name of the table
        type (TableType): the type of the table (if not one of the standard tables)
        kwargs:
            location(str): alternative location of csv

            credentials(str): alternative location of credentials


    Returns:
        pl.DataFrame: The extracted table
    """
    if type == TableTypes.CSV or (
        table in ["staffs", "stores"] and type == TableTypes.NOT_SET
    ):
        return extract_csv(table, path=kwargs.get("location", None))

    elif type == TableTypes.API or (
        table in ["customers", "order_items", "orders"] and type == TableTypes.NOT_SET
    ):
        return extract_api(table, path=kwargs.get("credentials", None))

    elif type == TableTypes.DB or (
        table in ["brands", "categories", "products", "stocks"]
        and type == TableTypes.NOT_SET
    ):
        return extract_db(table, path=kwargs.get("credentials", None))

    else:
        raise ValueError


def extract_fallback(table: str):
    if table in ["staffs", "stores"]:
        return extract(table)
    elif table in ["customers", "order_items", "orders"]:
        try:
            return extract(table)
        except:
            print(f"Filling in local version of {table}")
            return extract(table, TableTypes.CSV, location=get_path.api_path(table))
    elif table in ["brands", "categories", "products", "stocks"]:
        try:
            return extract(table)
        except:
            print(f"Filling in local version of {table}")
            return extract(table, TableTypes.CSV, location=get_path.db_path(table))


def main():
    staff_df = extract("staffs")
    print(staff_df.head())

    brands_df = extract("brands")
    print(brands_df.head())


if __name__ == "__main__":
    main()
