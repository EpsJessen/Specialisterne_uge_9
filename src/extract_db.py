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
    try:
        connection = mysql.connector.connect(
            host=credentials["IP"],
            port=credentials["SQL"]["PORT"],
            user=credentials["SQL"]["USER"],
            passwd=credentials["SQL"]["PW"],
            database="ProductDB",
            connection_timeout = 10
        )
    except:
        raise ConnectionError
    return connection


def extract_db(table: str, path: str|None = cred_path()) -> pl.DataFrame:
    """Returns DataFrame of table from database

    Args:
        credentials_path (str|None): path to credentials file
        table (str): name of the table to extract

    Returns:
        pl.DataFrame: Polars DataFrame of the extracted data
    """
    if path == None:
        path = cred_path()

    try:
        connection = db_connection(path)
    except:
        print(f"Could not connect to DB {table}")
        raise ConnectionError
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    headers = list(cursor.column_names)
    df = pl.DataFrame(cursor.fetchall(), orient="row")
    df.columns = headers
    return df


def extract_db_polars(table:str, path: str|None = cred_path()) -> pl.DataFrame:
    if path == None:
        path = cred_path()
    with open(path) as json_credentials:
        creds: dict = json.load(json_credentials)
    uri = f"mysql://{creds["SQL"]["USER"]}:{creds["SQL"]["PW"]}@{creds["IP"]}:{creds["SQL"]["PORT"]}/ProductDB"
    query = f"SELECT * FROM {table}"
    return pl.read_database_uri(query=query, uri=uri, engine="connectorx")

def main():
    credentials_path = cred_path()

    res = extract_db_polars("PRODUCTS")
    print(res.head(10))
    res = extract_db_polars("STOCKS")
    print(res.head(5))
    res = extract_db_polars("CATEGORIES")
    print(res.head(5))
    res = extract_db_polars("BRANDS")
    print(res.head(5))


if __name__ == "__main__":
    main()
