#
# Module for EXTRACTING, TRANSFORMING, and LOADING data.
# Will communicate with a DATABASE, LOCAL CSV, and API.
# CREDENTIALS will be stored in seperate file,
#  and should be changed to match your credentials.
#
import extract_data
import transform_data
import load_data
import create_db
import table_order_and_keys

def main():
    order = table_order_and_keys.get_order()
    tables = {}
    try:
        for table in order:
            tables[table] = extract_data.extract_fallback(table)
    except:
        print("COULD NOT GATHER TABLE DATA, ABORTING PROGRAM!")
        raise RuntimeError
    tables = transform_data.transform_all(tables)
    try:
        create_db.create_my_db(tables)
    except:
        print("COULD NOT CREATE DATABASE, ABORTING PROGRAM!")
        raise RuntimeError
    try:
        load_data.populate_tables(order, tables)
    except:
        print("COULD NOT POPULATE DATABASE, ABORTING PROGRAM!")
        raise RuntimeError

if __name__ == "__main__":
    main()
