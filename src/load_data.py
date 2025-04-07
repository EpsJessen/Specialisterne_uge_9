#
# This module will load transformed data from database to local database
#
import polars as pl
import transform_data
from get_path import my_creds_path
import json
from table_order_and_keys import get_order


def load_table(data: pl.DataFrame, name: str, credentials) -> None:
    """Loads a table in db with values from polars dataframe

    Args:
        data (pl.DataFrame): The dataframe with the data matching a table in db
        name (str): The name of the target table
        credentials (_type_): Path to json file containing credentials for using db
    """
    with open(credentials) as json_credentials:
        creds: dict = json.load(json_credentials)
    uri = f"mysql://{creds["user"]}:{creds["passwd"]}@localhost:3306/bikes"
    data.write_database(table_name=name, connection=uri, if_table_exists="append")


def load_tables(order: list[str], tables: dict[str, pl.DataFrame]):
    """Sequentially adds data to tables in db

    Args:
        order (list[str]): The order the tables should be loaded
        tables (dict[str, pl.DataFrame]): The dataframes containing the data by name
    """
    creds = my_creds_path()
    for name in order:
        load_table(tables[name], name, creds)


def main():
    order = get_order()
    tables = transform_data.main()
    load_tables(order, tables)


if __name__ == "__main__":
    main()
