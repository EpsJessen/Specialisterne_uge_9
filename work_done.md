## Workflow

- Made overview of existing tables showing fields, pk, and fk
- Ensured connectivity between tables, and made table illustrating
- Column types and analysis
- Created ER diagrams
- Decided on transformation of data
  - Better naming of columns (no id/name appended)
  - Add ID to staff, store
  - Remove duplicate data list_price
- Built skeleton for files
- Added credentials file
- To streamline, extract all files as polars DataFrames
- All extractions done
- Refactored to use one method to get paths
- Transformation must be table specific
  - However, certain information (e.g. new IDs) must be kept

- [x] extract
  - [x] csv
  - [x] api
  - [x] db
- [ ] transform
  - [ ] csv
  - [ ] api
  - [ ] db
- [ ] load
  - [ ] Establish communication
  - [ ] Create db
  - [ ] csv
  - [ ] api
  - [ ] db
- [ ] Collect all in `etl.py`

Decide how the flow of information should be through the system
  