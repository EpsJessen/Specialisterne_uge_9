# Database overview

## Schemas

### API

|CUSTOMERS|
|---------|
|customer_ID*|
|first_name|
|last_name|
|phone|
|email|
|street|
|city|
|state|
|zip_code|

| ORDER_ITEMS|
|-|
|order_id* *fk (ORDERS)*|
|item_id*|
|product_id *fk (PRODUCTS)*|
|quantity|
|list_price|
|discount|

| ORDERS|
| - |
|order_id*|
|customer_id *fk (ORDERS)*|
|order_status|
|order_date|
|required_date|
|shipped_date|
|store|
|staff_name|

### CSV

|STAFFS|
|-|
|name|
|last_name|
|email*|
|phone|
|active|
|store_name *fk (STORES)*|
|street|
|manager|

|STORES|
|-|
|name*|
|phone|
|email|
|street|
|city|
|state|
|zip_code|
### DB

|BRANDS|
|-|
| brand_id* |
| brand_name |

|CATEGORIES|
|-|
| category_id* |
| category_name |

|PRODUCTS|
|-|
| product_id* |
| product_name |
| brand_id *fk (BRANDS)* |
| category_id *fk (CATEGORIES)* |
| model_year |
| list_price |

|STOCKS|
|-|
| store_name* *fk (STORES)*|
| product_id* *fk (PRODUCTS)*|
| quantity |

## CONNECTIONS
v = from column to row  
\> = from row to column
|      |CUST|ORD_IT|ORD|STAFF|STORE|BRAND|CAT|PROD|STOCK|
|------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|CUST  |\ |-|v|-|-|-|-|-|-|
|ORD_IT|-|\ |>|-|-|-|-|>|-|
|ORD   |>|v|\ |-|-|-|-|-|-|
|STAFF |-|-|-|\ |>|-|-|-|-|
|STORE |-|-|-|v|\ |-|-|-|v|
|BRAND |-|-|-|-|-|\ |-|v|-|
|CAT   |-|-|-|-|-|-|\ |v|-|
|PROD  |-|v|-|-|-|>|>|\ |v|
|STOCK |-|-|-|-|>|-|-|>|\ |


CUST <- ORD <- ORD_IT -> PROD (-> BRAND), (-> CAT)  
STAFF -> STORE <- STOCK ----^
