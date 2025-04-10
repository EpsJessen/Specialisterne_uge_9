def get_order() -> list[str]:
    """
    Give the tables in an order such that all tables a given table 
    depends on is placed before that table
    - can be done since we have no circular dependencies
    """
    return [
        "customers",
        "brands",
        "categories",
        "stores",
        "products",
        "stocks",
        "staffs",
        "orders",
        "order_items",
    ]


def get_pks() -> dict[str, list[str]]:
    """Dictionary of primary keys of tables"""
    return {
        "customers": ["customer_id"],
        "brands": ["brand_id"],
        "categories": ["category_id"],
        "stores": ["store_id"],
        "products": ["product_id"],
        "stocks": ["store_id", "product_id"],
        "staffs": ["staff_id"],
        "orders": ["order_id"],
        "order_items": ["order_id", "item_nr"],
    }


def get_fks() -> dict[str, list[dict[str, str | list[str]]] | None]:
    """Dictionary of foreign keys of tables, as well as the tables where 
    the keys are found"""
    pks = get_pks()
    return {
        "customers": None,
        "brands": None,
        "categories": None,
        "stores": None,
        "products": [
            {"table": "brands", "key": pks["brands"], "fk": "brand_id"},
            {"table": "categories", "key": pks["categories"], "fk": "category_id"},
        ],
        "stocks": [
            {"table": "stores", "key": pks["stores"], "fk": "store_id"},
            {"table": "products", "key": pks["products"], "fk": "product_id"},
        ],
        "staffs": [
            {"table": "stores", "key": pks["stores"], "fk": "store_id"},
            {"table": "staffs", "key": pks["staffs"], "fk": "manager_id"},
        ],
        "orders": [
            {"table": "stores", "key": pks["stores"], "fk": "store_id"},
            {"table": "staffs", "key": pks["staffs"], "fk": "staff_id"},
            {"table": "customers", "key": pks["customers"], "fk": "customer_id"},
        ],
        "order_items": [
            {"table": "orders", "key": pks["orders"], "fk": "order_id"},
            {"table": "products", "key": pks["products"], "fk": "product_id"},
        ],
    }
