#
# This module will transform data received
#
import polars as pl
import extract_data
import transform_table as tt
from table_order_and_keys import get_order


def transform_customers(customers: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on customers"""
    # No transformations at this time
    return customers


def transform_order_items(
    order_items: pl.DataFrame, orders: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    """Performs transformation on order_items using related tables"""
    # List_price is duplicated from products
    order_items = tt.remove_column(order_items, "list_price")
    # Better naming for ids
    order_items = tt.change_column_name(order_items, "order_id", "order")
    order_items = tt.change_column_name(order_items, "item_id", "item_nr")
    return order_items


def transform_orders(
    orders: pl.DataFrame, staffs: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    """Performs transformation on orders using related tables"""
    # Refer to related tables by id
    orders = tt.change_to_foreign_ID(orders, staffs, "staff_name", "staff", "first_name")
    orders = tt.change_to_foreign_ID(orders, stores, "store", "store", "name")
    return orders


def transform_staffs(staffs: pl.DataFrame, stores: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on order_staffs using related table store"""
    # Add id column
    staffs = tt.add_ID(staffs)
    # Refer to store by id
    staffs = tt.change_to_foreign_ID(staffs, stores, "store_name", "store", "name", "id")
    # Street is duplicated from stores
    staffs = tt.remove_column(staffs, "street")
    # The manager of employees 9, 10 is 8 not 7
    staffs = tt.replace_values_in_column(staffs, "manager_id", 7, 8)
    # Change manager_id datatype to match id
    staffs = tt.change_data_type(staffs, "manager_id", pl.UInt32)
    # Change name of column name to first_name, as this is what is represented
    staffs = tt.change_column_name(staffs, "name", "first_name")
    return staffs


def transform_stores(stores: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on stores"""
    # Add id column
    stores = tt.add_ID(stores)
    return stores


def transform_brands(brands: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on brands"""
    # No transformations at this time
    return brands


def transform_categories(categories: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on categories"""
    # No transformations at this time
    return categories


def transform_products(
    products: pl.DataFrame, brands: pl.DataFrame, categories: pl.DataFrame
) -> pl.DataFrame:
    """Performs transformation on products using related tables"""
    # Round list_price to avoid float artefacts from extraction
    products = tt.round_floats(products, "list_price")
    return products


def transform_stocks(
    stocks: pl.DataFrame, products: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    """Performs transformation on stocks using related tables"""
    # Use id from stores
    stocks = tt.change_to_foreign_ID(stocks, stores, "store_name", "store", "name")
    # Change to better name for product column
    stocks = tt.change_column_name(stocks, "product_id", "product")
    return stocks


def transform_all(tables: dict[str : pl.DataFrame]) -> dict[str : pl.DataFrame]:
    """Transform all tables given in this weeks task

    Args:
        tables (dict[str : pl.DataFrame]): The tables to be transformed 

    Returns:
        dict[str : pl.DataFrame]: The transformed tables
    """
    # TRANSFORM
    stores = transform_stores(tables["stores"])
    staffs = transform_staffs(tables["staffs"], stores)
    customers = transform_customers(tables["customers"])
    orders = transform_orders(tables["orders"], staffs, stores)
    brands = transform_brands(tables["brands"])
    categories = transform_categories(tables["categories"])
    products = transform_products(tables["products"], brands, categories)
    stocks = transform_stocks(tables["stocks"], products, stores)
    order_items = transform_order_items(tables["order_items"], orders, stores)
    # print(orders)
    return {
        "stores": stores,
        "staffs": staffs,
        "customers": customers,
        "orders": orders,
        "brands": brands,
        "categories": categories,
        "products": products,
        "stocks": stocks,
        "order_items": order_items,
    }


def main():
    # EXTRACT
    table_dict = {}
    tables = get_order()
    for name in tables:
        table_dict[name] = extract_data.extract_fallback(name)

    # TRANSFORM
    t_dict = transform_all(table_dict)

    # INSPECT
    #print(t_dict["order_items"].head(10))
    return t_dict


if __name__ == "__main__":
    main()
