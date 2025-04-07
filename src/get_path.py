"""
    Get paths to files 
"""
from os.path import join

def credentials_path() -> str:
    """
    OS specific path to communications/credentials
    """
    return join("Data", "communication.json")

def csv_path(table:str) -> str:
    """
    OS specific path to csv table
    """
    return join("Data CSV", f"{table}.csv")

def api_path() -> str:
    """
    OS specific path to API csv tables
    """
    return join("Data API", "data")

def db_path() -> str:
    """
    OS specific path to DB csv tables
    """
    return join("Data DB")

def main():
    pass

if __name__ == "__main__":
    main()
