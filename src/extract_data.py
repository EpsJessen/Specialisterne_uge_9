import polars as pl
from extract_api import extract_api
from extract_db import extract_db
from extract_csv import extract_csv
from enum import Enum
import get_path
from exceptions import TableNotDefinedError, DictNotFoundError, ExtractionError
from requests.exceptions import Timeout


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
    match type:
        case TableTypes.CSV:
            return extract_csv(table, path=kwargs.get("location", None))
        case TableTypes.API:
            return extract_api(table, path=kwargs.get("credentials", None))
        case TableTypes.DB:
            return extract_db(table, path=kwargs.get("credentials", None))
        case _:
            raise ValueError


def extract_predefined(table: str, **kwargs):
    match table:
        case "staffs" | "stores":
            return extract(table, TableTypes.CSV, **kwargs)
        case "customers" | "order_items" | "orders":
            return extract(table, TableTypes.API, **kwargs)
        case "brands" | "categories" | "products" | "stocks":
            return extract(table, TableTypes.DB, **kwargs)
        case _:
            raise TableNotDefinedError


def extract_predefined_local(table: str, **kwargs):
    match table.lower():
        case "staffs" | "stores":
            return extract(table, TableTypes.CSV, **kwargs)
        case "customers" | "order_items" | "orders":
            return extract(table, TableTypes.CSV, location=get_path.api_path(table))
        case "brands" | "categories" | "products" | "stocks":
            return extract(table, TableTypes.CSV, location=get_path.db_path(table))
        case _:
            raise TableNotDefinedError


def extract_with_fallback(table: str, **kwargs):
    """
    Extract table, preferably at correct location,
    but otherwise from local file
    """
    try:
        return extract_predefined(table, **kwargs)
    except TableNotDefinedError:
        print(f"Table named {table} not defined")
    except FileNotFoundError:
        print(f"No file matching table found!")
        raise FileNotFoundError
    except Timeout | ConnectionError | DictNotFoundError:
        try:
            return extract_predefined_local(table, **kwargs)
        except:
            print(f"Could not find table {table} locally")
            raise ExtractionError


def main():
    staff_df = extract_with_fallback("staffs")
    print(staff_df.head())

    brands_df = extract_with_fallback("brands")
    print(brands_df.head())


if __name__ == "__main__":
    main()
