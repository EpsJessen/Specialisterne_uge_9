## Workflow

- Made overview of existing tables showing fields, pk, and fk
- Ensured connectivity between tables, and made table illustrating
- Column types and analysis
- Created ER diagrams
- Decided on transformation of data
  - General:
    - Better naming of columns (avoid id/name appended)
  - Add ID:
    - Staff
    - Store
    - Use this when refering to them in other tables
  - Order_items:
    - Remove duplicate data list_price
  - Staffs:
    - Remove duplicate data street
    - Fix manager_id (both values and type)
    - Change staffs to have column first_name instead of name
    - Change active to type boolean
  - Customers:
    - Split street into street_nr and street
    - Remove whitespace surrounding street values
  - Products:
    - List_price rounded as error was introduced during extraction
- Built skeleton for files
- Added credentials files
  - API+SQL
  - New Database
- To streamline, extract all files as polars DataFrames
- All extractions done
- Refactored to use one method to get paths
- Refactored to have general extract function
  - Call subroutines for each source
    - Recognizes tables defined in task as belonging to different sources
  - Tries to extract API+SQL using network.
    - If resources unavailable, use local files.
- Transformation is table specific
  - However, certain information (e.g. new IDs) must be kept
    - Done by keeping reference until all data is transformed
    - Could be limited by only keeping while needed
  - Use common functions for transforming tables
    - Defined one subrouting for dealing with each predefined table
- Decided how the flow of information should be through the system
  - Represented in diagram `Data/ETL-flow.png`
- Although not present in the database, one order could hold several items of the same product, since a discount might only apply to a certain nr of items (e.g. 50% of headlights, limit 3 per customer)
  - Thus we choose to not use order+product_it as composite key in order_items
- Found 62 instances of products matching on all parameters except product_id and category
  - Will assume that this is due small build differences (e.g. saddle, fenders) and thus represents a legitimate difference

## ToDo

- [x] Extract
  - [x] CSV
  - [x] API
  - [x] DB
- [x] Transform
  - [x] Add IDs
  - [x] Remove duplicates
  - [x] Ensure datatypes match
  - [ ] Common naming scheme
  - [x] General Clean-up
- [x] Load
  - [x] Establish communication
  - [x] Create new DB
  - [x] Load tables
- [x] Collect all in `etl.py`


  