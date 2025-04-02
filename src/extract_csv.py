#
# This module will extract data from provided local
#  CSV file
#
from os.path import join
from csv import reader
import polars as pl


def extract_csv(path: str) -> pl.DataFrame:
    """Extracts data from csv file returning extracted data as a list
    of lists of strings

    Args:
        path (str): the location of the csv file (should match system)

    Returns:
        list[list[str]]: A list where each sublist is a row of the file
        with each string corresponding to an entry in the file

    Exceptions:
        FileNotFoundError: If the file is not located at the specified
        path
    """
    return pl.read_csv(path)


def main():
    path = join("Data CSV", "staffs.csv")
    try:
        staffs = extract_csv(path)
        print(staffs.head())
    except FileNotFoundError:
        print(f"Could not find {path}")
    


if __name__ == "__main__":
    main()
