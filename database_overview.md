# Database overview

## Schemas

### API

(-) => Not done  
All values are allowed to be null, should probably not be the case

|CUSTOMERS|type |notes
|---------|-| - |
|customer_ID*| INT |
|first_name|STR | 
|last_name|STR|
|phone| STR|
|email| STR| check that it conforms (-)|
|street_nr| STR | must be a string as some addresses are like '25C'|
|street| STR | remove surrounding whitespace|
|city| STR | |
|state| STR | |
|zip_code| INT | |

| ORDER_ITEMS| type | note |
|-|-|-|
|order_id* *fk (ORDERS)*| INT |
|item_id*| INT |
|product_id *fk (PRODUCTS)*| INT |
|quantity| INT |
|list_price| FLOAT | should be removed since it is duplication |
|discount| FLOAT | 0 < discount < 1, suspicious if too large, needs not be equal for order (-)|

| ORDERS| type | note |
|-|-|-|
|order_id*| INT|
|customer_id *fk (CUSTOMERS)*| INT |
|order_status|INT | code in [1,2,3,4] (-)|
|order_date|DATE|
|required_date|DATE|should not be before order_date (-) |
|shipped_date|DATE|should not be before order_date (-)|
|store *fk (STORES)*| INT | change to store_id |
|staff_name *fk (STAFFS)*| INT | change to staff_id |

### CSV

|STAFFS| type | note |
|-|-|-|
| ID* | INT | new primary key |
|name| STR | currently treated as pk, should be first_name |
|last_name| STR |
|email| STR | ensure fmt (-) |
|phone| STR | |
|active| BOOL (TINYINT(1)) |  |
|store_name *fk (STORES)*| INT| changed to store_id|
|street| STR | should be removed as it is data duplication with STORES|
|manager| INT | Can be null, reference to staff_id, correct id should be ensured|

|STORES| type | note |
|-|-|-|
|id|INT|new pk|
|name| STR | previous pk|
|phone| STR | |
|email| STR | ensure fmt, UNIQUE (-)|
|street| STR | UNIQUE, fmt: nr, name (-)|
|city| STR | |
|state| STR |  |
|zip_code| INT | |
### DB

|BRANDS| type | note |
|-|-|-|
| brand_id* | INT | |
| brand_name | STR | |

|CATEGORIES| type | note |
|-|-|-|
| category_id* | INT | kept as separate table to avoid storing string in each row of product|
| category_name | STR | kept as separate table to avoid storing string in each row of product|

|PRODUCTS| type | note |
|-|-|-|
| product_id* | INT | |
| product_name | STR | Not neccessarily unique since type of bike is not necessarily part of name|
| brand_id *fk (BRANDS)* | INT | |
| category_id *fk (CATEGORIES)* | INT | |
| model_year | INT | |
| list_price | FLOAT | POSITIVE (-)|

|STOCKS| type | note |
|-|-|-|
| store_name* *fk (STORES)*| INT | Change to store_id |
| product_id* *fk (PRODUCTS)*| INT | |
| quantity | INT | NOT NEGATIVE (-) |

## CONNECTIONS
v = from column to row  
\> = from row to column
|      |CUST|ORD_IT|ORD|STAFF|STORE|BRAND|CAT|PROD|STOCK|
|------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|CUST  |\ |-|v|-|-|-|-|-|-|
|ORD_IT|-|\ |>|-|-|-|-|>|-|
|ORD   |>|v|\ |>|>|-|-|-|-|
|STAFF |-|-|v|\ |>|-|-|-|-|
|STORE |-|-|v|v|\ |-|-|-|v|
|BRAND |-|-|-|-|-|\ |-|v|-|
|CAT   |-|-|-|-|-|-|\ |v|-|
|PROD  |-|v|-|-|-|>|>|\ |v|
|STOCK |-|-|-|-|>|-|-|>|\ |


### ER before

![ER before](Data/ER-before.png "ER before")

### ER Goal

Better naming of columns (same columns, same name in different tables)  
Added ID to staff, store  
Removed duplicate data list_price

![ER Goal](Data/ER_goal.png "ER goal")


### Flow of ETL process

![ETL Process](Data/ETL-flow.png "ER Flow")


