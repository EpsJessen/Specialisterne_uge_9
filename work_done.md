## Workflow

- Made overview of existing tables showing fields, pk, and fk
- Ensured connectivity between tables, and made table illustrating
- Column types and analysis
- Created ER diagrams
- Decided on transformation of data
  - Better naming of columns (avoid id/name appended)
  - Add ID to staff, store
    - use this when refering to them in other tables
  - Remove duplicate data list_price
  - remove street from staffs
  - remove list_price from order_items
  - fix manager_id
  - change staffs to have column first_name instead of name
- Built skeleton for files
- Added credentials file
- To streamline, extract all files as polars DataFrames
- All extractions done
- Refactored to use one method to get paths
- Refactored to have general extract function
- Transformation must be table specific
  - However, certain information (e.g. new IDs) must be kept
  - done by keeping reference until all data is transformed
    - could be limited by only keeping while needed
- Decided how the flow of information should be through the system
  - represented in diagram `Data/ETL-flow.png`
- Although not present in the database, one order could hold several items of the same product, since a discount might only apply to a certain nr of items (e.g. 50% of headlights, limit 3 per customer)
  - Thus we choose to not use order+product_it as composite key in order_items
- Found 62 instances of products matching on all parameters except product_id and category
  - Will assume that this is due small build differences (e.g. saddle, fenders) and thus represents a legitimate difference


- [x] extract
  - [x] csv
  - [x] api
  - [x] db
- [x] transform
- [x] load
  - [x] Establish communication
  - [x] Create db
  - [x] csv
  - [x] api
  - [x] db
- [x] Collect all in `etl.py`


  