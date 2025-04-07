#
# This module will extract data by API calls
#

import requests
import polars as pl
import json
from get_path import credentials_path as cred_path


def api_get(ip: str, port: int, dataset: str) -> pl.DataFrame:
    """Create a polars DateFrame from the response at the given API

    Args:
        ip (str): IP address of the API
        port (int): Port number of the API
        dataset (str): Endpoint of the dataset on the API

    Returns:
        pl.DataFrame: polaris DataFrame of the dataset

    Exceptions:
        ConnectionError: If the connection does not result
        in a 200 status code
    """
    
    try:
        connection = f"http://{ip}:{port}/{dataset}"
        response = requests.get(connection, timeout=1)
    except:
        print(f"Connection for {dataset=} timed out!")
        raise requests.exceptions.Timeout
    if response.status_code == 200:
        # print("Success:", response.status_code)
        return pl.DataFrame(json.loads(response.json()))
    else:
        print(f"Could not connect to {connection}, status code: {response.status_code}")
        raise ConnectionError


def extract_api(table: str, path: str|None = cred_path()) -> pl.DataFrame:
    """Gets credentials and uses them to extract dataset from API

    Args:
        credentials_path (str|None): Path to credentials
        dataset (str): Endpoint of the dataset on the the API

    Returns:
        pl.DataFrame: polars Dataframe with the extracted data
    """
    if path == None:
        path = cred_path()

    with open(path) as credentials_file:
        json_credentials = credentials_file.read()
    credentials = json.loads(json_credentials)
    return api_get(credentials["IP"], credentials["API"]["PORT"], table)


def main():
    creds = cred_path()
    try:
        customers = extract_api("orders")
        print(customers.head())
    except:
        print(f"Could not connect to server")


if __name__ == "__main__":
    main()
