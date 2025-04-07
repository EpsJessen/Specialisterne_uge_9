#
# This module will transform data received from API calls
#
import polars as pl
import extract_data
import transform_table as tt
from table_order_and_keys import get_order


def transform_customers(customers: pl.DataFrame) -> pl.DataFrame:
    return customers


def transform_order_items(
    order_items: pl.DataFrame, orders: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    order_items = tt.remove_column(order_items, "list_price")
    order_items = tt.change_column_name(order_items, "order_id", "order")
    order_items = tt.change_column_name(order_items, "item_id", "item_nr")
    return order_items


def transform_orders(
    orders: pl.DataFrame, staffs: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    orders = tt.change_to_foreign_ID(orders, staffs, "staff_name", "staff", "first_name")
    orders = tt.change_to_foreign_ID(orders, stores, "store", "store", "name")
    return orders


def transform_staffs(staffs: pl.DataFrame, stores: pl.DataFrame) -> pl.DataFrame:
    staffs = tt.add_ID(staffs)
    staffs = tt.change_to_foreign_ID(staffs, stores, "store_name", "store", "name", "id")
    staffs = tt.remove_column(staffs, "street")
    staffs = tt.replace_values_in_column(staffs, "manager_id", 7, 8)
    staffs = tt.change_data_type(staffs, "manager_id", pl.UInt32)
    staffs = tt.change_column_name(staffs, "name", "first_name")
    return staffs


def transform_stores(stores: pl.DataFrame) -> pl.DataFrame:
    stores = tt.add_ID(stores)
    return stores


def transform_brands(brands: pl.DataFrame) -> pl.DataFrame:
    return brands


def transform_categories(categories: pl.DataFrame) -> pl.DataFrame:
    return categories


def transform_products(
    products: pl.DataFrame, brands: pl.DataFrame, categories: pl.DataFrame
) -> pl.DataFrame:
    products = tt.round_floats(products, "list_price")
    return products


def transform_stocks(
    stocks: pl.DataFrame, products: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    stocks = tt.change_to_foreign_ID(stocks, stores, "store_name", "store", "name")
    stocks = tt.change_column_name(stocks, "product_id", "product")
    return stocks


def transform_all(tables: dict[str : pl.DataFrame]) -> dict[str : pl.DataFrame]:
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
    print(t_dict["brands"].head(10))
    return t_dict


if __name__ == "__main__":
    main()
