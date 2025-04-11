#
# This module will load transformed data from database to local database
#
import polars as pl
import transform_data
from get_path import my_creds_path
import json
from table_order_and_keys import get_order
from exceptions import DictNotFoundError


def load_table(data: pl.DataFrame, name: str, credentials: dict[str:str]) -> None:
    """Loads a table in db with values from polars dataframe

    Args:
        data (pl.DataFrame): The dataframe with the data matching a table in db
        name (str): The name of the target table
        credentials (dict[str:str]): credentials for using db
    """
    uri = f"mysql://{credentials["user"]}:{credentials["passwd"]}@localhost:3306/bikes"
    data.write_database(table_name=name, connection=uri, if_table_exists="append")


def load_tables(order: list[str], tables: dict[str, pl.DataFrame], creds: str|None = None):
    """Sequentially adds data to tables in db

    Args:
        order (list[str]): The order the tables should be loaded
        tables (dict[str, pl.DataFrame]): The dataframes containing the data by name
        creds (str): Path to json file containing credentials for using db
    """
    if creds is None:
        creds = my_creds_path()
    try:
        with open(creds) as json_credentials:
            creds: dict = json.load(json_credentials)
    except FileNotFoundError:
        raise DictNotFoundError
    try:
        for name in order:
            load_table(tables[name], name, creds)
    except:
        raise ConnectionError


def main():
    order = get_order()
    tables = transform_data.main()
    load_tables(order, tables)


if __name__ == "__main__":
    main()
