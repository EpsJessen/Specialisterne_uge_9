#
# This module will transform data received from API calls
#
import polars as pl
import extract
from os.path import join


def transform_customers(customers: pl.DataFrame) -> pl.DataFrame:
    return customers


def transform_order_items(
    order_items: pl.DataFrame, orders: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    order_items = remove_column(order_items, "list_price")
    return order_items


def transform_staffs(staffs: pl.DataFrame, stores: pl.DataFrame) -> pl.DataFrame:
    staffs = add_ID(staffs)
    staffs = change_to_foreign_ID(staffs, stores, "store_name", "store", "name", "id")
    staffs = remove_column(staffs, "street")
    staffs = replace_values_in_column(staffs, "manager_id", 7, 8)
    staffs = change_data_type(staffs, "manager_id", pl.UInt32)
    staffs = change_column_name(staffs, "name", "first_name")
    return staffs


def transform_stores(stores: pl.DataFrame) -> pl.DataFrame:
    stores = add_ID(stores)
    return stores


def transform_brands(brands: pl.DataFrame) -> pl.DataFrame:
    return brands


def transform_categories(categories: pl.DataFrame) -> pl.DataFrame:
    return categories


def transform_products(
    products: pl.DataFrame, brands: pl.DataFrame, categories: pl.DataFrame
) -> pl.DataFrame:
    return products


def transform_stocks(
    stocks: pl.DataFrame, products: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    stocks = change_to_foreign_ID(stocks, stores, "store_name", "store", "name")
    return stocks


def add_ID(table: pl.DataFrame) -> pl.DataFrame:
    return table.with_row_index("id", offset=1)


def remove_column(table: pl.DataFrame, row: str) -> pl.DataFrame:
    return table.drop(row)


def change_to_foreign_ID(
    table: pl.DataFrame,
    foreign_table: pl.DataFrame,
    old_column: str,
    new_column: str,
    old_foreign_column: str,
    new_foreign_column: str = "id",
) -> pl.DataFrame:
    limit_ft = foreign_table[[old_foreign_column, new_foreign_column]]
    limit_t = table[[old_column]]
    matches = limit_t.join(
        limit_ft, how="left", left_on=old_column, right_on=old_foreign_column
    )[new_foreign_column]
    table = table.with_columns(matches.alias(new_column))
    if old_column != new_column:
        table = remove_column(table, old_column)
    return table


def replace_values_in_column(table: pl.DataFrame, column: str, old, new) -> pl.DataFrame:
    new_column = table[column].replace(old, new)
    index = table.get_column_index(column)
    return table.replace_column(index, new_column)


def change_data_type(
    table: pl.DataFrame, column: str, type: pl.DataType
) -> pl.DataFrame:
    return table.with_columns(pl.col(column).cast(type, strict=False))


def change_column_name(
    table: pl.DataFrame, old_name: str, new_name: str
) -> pl.DataFrame:
    return table.rename({old_name: new_name})


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
