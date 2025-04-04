#
# This module will transform data received from API calls
#
import polars as pl
import extract
from os.path import join

def main():
    # EXTRACT
    staffs = extract.extract("staffs")
    stores = extract.extract("stores")
    brands = extract.extract("brands", type=extract.TableTypes.CSV, location="Data DB")
    categories = extract.extract(
        "categories", type=extract.TableTypes.CSV, location="Data DB"
    )
    products = extract.extract(
        "products", type=extract.TableTypes.CSV, location="Data DB"
    )
    stocks = extract.extract("stocks", type=extract.TableTypes.CSV, location="Data DB")
    customers = extract.extract(
        "customers", type=extract.TableTypes.CSV, location=join("Data API", "data")
    )
    order_items = extract.extract(
        "order_items", type=extract.TableTypes.CSV, location=join("Data API", "data")
    )
    orders = extract.extract(
        "orders", type=extract.TableTypes.CSV, location=join("Data API", "data")
    )
    # TRANSFORM

if __name__ == "__main__":
    main()
