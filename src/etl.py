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
    """
    Extracts, Transforms, and Loads data from this week's task

    Raises:
        RuntimeError: Something went wrong during the execution, as described 
        in the print 
    """
    order = table_order_and_keys.get_order()
    tables = {}

    # EXTRACT all tables
    try:
        for table in order:
            tables[table] = extract_data.extract_with_fallback(table)
    except:
        print("COULD NOT GATHER TABLE DATA, ABORTING PROGRAM!")
        raise RuntimeError
    
    # TRANSFORM tables in order
    tables = transform_data.transform_all(tables)

    # Create database based on tables and analysis
    try:
        create_db.create_my_db(tables)
    except:
        print("COULD NOT CREATE DATABASE, ABORTING PROGRAM!")
        raise RuntimeError
    
    #LOAD transformed tables into database
    try:
        load_data.load_tables(order, tables)
    except:
        print("COULD NOT POPULATE DATABASE, ABORTING PROGRAM!")
        raise RuntimeError
    print("ETL Completed!")


if __name__ == "__main__":
    main()
