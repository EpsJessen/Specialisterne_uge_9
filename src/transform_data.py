#
# This module will transform data received
#
import polars as pl
import extract_data
import transform_table as tt
import table_order_and_keys as t_ord
from exceptions import WrongTablesError

pks = t_ord.get_pks()
fks = t_ord.get_fks()

def transform_customers(customers: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on customers"""
    # Remove surrounding whitespace
    customers = tt.remove_surrounding(customers, "street")
    # Splits street into street_nr and street
    customers = tt.split_prepended(customers, "street", "street_nr")
    return customers


def transform_order_items(
    order_items: pl.DataFrame, orders: pl.DataFrame, products: pl.DataFrame
) -> pl.DataFrame:
    """Performs transformation on order_items using related tables"""
    # List_price is duplicated from products
    order_items = tt.remove_column(order_items, "list_price")
    # Better naming for item element of pk
    order_items = tt.change_column_name(order_items, "item_id", pks["order_items"][1])
    return order_items


def transform_orders(
    orders: pl.DataFrame, staffs: pl.DataFrame, stores: pl.DataFrame
) -> pl.DataFrame:
    """Performs transformation on orders using related tables"""
    # Refer to related tables by id
    orders = tt.change_to_foreign_ID(orders, staffs, "staff_name", pks["staffs"][0], "first_name", pks["staffs"][0])
    orders = tt.change_to_foreign_ID(orders, stores, "store", pks["stores"][0], "name", pks["stores"][0])
    return orders


def transform_staffs(staffs: pl.DataFrame, stores: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on order_staffs using related table store"""
    # Add id column
    staffs = tt.add_ID(staffs, pks["staffs"][0])
    # Refer to store by id
    staffs = tt.change_to_foreign_ID(staffs, stores, "store_name", pks["stores"][0], "name", pks["stores"][0])
    # Street is duplicated from stores
    staffs = tt.remove_column(staffs, "street")
    # The manager of employees 9, 10 is 8 not 7
    staffs = tt.replace_values_in_column(staffs, "manager_id", 7, 8)
    # Change manager_id datatype to match id
    staffs = tt.change_data_type(staffs, "manager_id", pl.UInt32)
    # Change name of column name to first_name, as this is what is represented
    staffs = tt.change_column_name(staffs, "name", "first_name")
    # Change active to be of type boolean
    staffs = tt.change_data_type(staffs, "active", pl.Boolean)
    return staffs


def transform_stores(stores: pl.DataFrame) -> pl.DataFrame:
    """Performs transformation on stores"""
    # Add id column
    stores = tt.add_ID(stores, pks["stores"][0])
    # Remove surrounding whitespace from street
    stores = tt.remove_surrounding(stores, "street")
    # Splits street into street_nr and street
    stores = tt.split_prepended(stores, "street", "street_nr")
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
    stocks = tt.change_to_foreign_ID(stocks, stores, "store_name", pks["stores"][0], "name", pks["stores"][0])
    return stocks


def transform_all(tables: dict[str : pl.DataFrame]) -> dict[str : pl.DataFrame]:
    """Transform all tables given in this weeks task

    Args:
        tables (dict[str : pl.DataFrame]): The tables to be transformed 

    Returns:
        dict[str : pl.DataFrame]: The transformed tables
    """
    # TRANSFORM
    try:
        assert(tables.keys()==set(t_ord.get_order()))
    except:
        raise WrongTablesError
    stores = transform_stores(tables["stores"])
    staffs = transform_staffs(tables["staffs"], stores)
    customers = transform_customers(tables["customers"])
    orders = transform_orders(tables["orders"], staffs, stores)
    brands = transform_brands(tables["brands"])
    categories = transform_categories(tables["categories"])
    products = transform_products(tables["products"], brands, categories)
    stocks = transform_stocks(tables["stocks"], products, stores)
    order_items = transform_order_items(tables["order_items"], orders, products)
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
    tables = t_ord.get_order()
    for name in tables:
        table_dict[name] = extract_data.extract_with_fallback(name)

    # TRANSFORM
    t_dict = transform_all(table_dict)

    # INSPECT
    #print(t_dict["customers"].head(10))
    return t_dict


if __name__ == "__main__":
    main()
