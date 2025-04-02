#
# This module will extract data from database given
#  credentials
#
from get_path import credentials_path as cred_path
import json
import mysql.connector
import mysql.connector.cursor
import polars as pl


def db_connection(
    credentials_path,
) -> mysql.connector.pooling.PooledMySQLConnection | mysql.connector.MySQLConnection:
    """Return a connction to the MySQL database

    Args:
        credentials_path (str): path to credentials file

    Returns:
        MySQL connection: Connection to the database
    """
    with open(credentials_path) as json_credentials:
        credentials: dict = json.load(json_credentials)
    connection = mysql.connector.connect(
        host=credentials["IP"],
        port=credentials["SQL"]["PORT"],
        user=credentials["SQL"]["USER"],
        passwd=credentials["SQL"]["PW"],
        database="ProductDB",
    )
    return connection


def extract_db(credentials_path: str, table: str) -> pl.DataFrame:
    """Returns DataFrame of table from database

    Args:
        credentials_path (str): path to credentials file
        table (str): name of the table to extract

    Returns:
        pl.DataFrame: Polars DataFrame of the extracted data
    """

    connection = db_connection(credentials_path)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    headers = list(cursor.column_names)
    df = pl.DataFrame(cursor.fetchall(), orient="row")
    df.columns = headers
    return df


def main():
    credentials_path = cred_path()

    res = extract_db(credentials_path, "PRODUCTS")
    print(res.head(10))


if __name__ == "__main__":
    main()
