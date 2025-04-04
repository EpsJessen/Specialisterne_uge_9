#
# This module will extract data from provided local
#  CSV file
#

from get_path import csv_path
from csv import reader
import polars as pl
from os.path import join


def extract_csv(table: str, path: str|None = None) -> pl.DataFrame:
    """Extracts data from csv file returning extracted data as a list
    of lists of strings

    Args:
        table (str): the name of the csv file containing the table
        path (str): the path to the folder where the csv file is stored

    Returns:
        list[list[str]]: A list where each sublist is a row of the file
        with each string corresponding to an entry in the file

    Exceptions:
        FileNotFoundError: If the file is not located at the specified
        path
    """
    if path is None:
        path = csv_path(f"{table}")
    else:
        path = join(path, f"{table}.csv")
    return pl.read_csv(path)


def main():
    path = csv_path("stores")
    try:
        staffs = extract_csv("staffs")
        print(staffs.head())
    except FileNotFoundError:
        print(f"Could not find {path}")
    


if __name__ == "__main__":
    main()
