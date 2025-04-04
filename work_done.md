## Workflow

- Made overview of existing tables showing fields, pk, and fk
- Ensured connectivity between tables, and made table illustrating
- Column types and analysis
- Created ER diagrams
- Decided on transformation of data
  - Better naming of columns (no id/name appended)
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


- [x] extract
  - [x] csv
  - [x] api
  - [x] db
- [x] transform
- [ ] load
  - [ ] Establish communication
  - [ ] Create db
  - [ ] csv
  - [ ] api
  - [ ] db
- [ ] Collect all in `etl.py`

Decide how the flow of information should be through the system
  