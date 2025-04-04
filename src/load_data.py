#
# This module will load transformed data from database to local database
#
import polars as pl


def create_table(
    name: str, columns: list[str],
    pks: list[str], fks: list[dict[str, str]]
) -> None:
    pass


def load_table(data: pl.DataFrame, name: str) -> None:
    pass


def main():
    pass


if __name__ == "__main__":
    main()
