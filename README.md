# Specialisterne_uge_6
 ETL task for interaction wirh API, DB, & CSV

### To Run
Run `src/etl.py`.  
To run properly, ensure that `Data/communication.json` contains the data of for communicating with the server (both SQL and API), and that `Data/my_db.json` contains the data for communicating with your MySQL server.  
In addition, the CSV data should be located in `Data CSV/*table*.csv` where table is the name of each csv table.
Further, if the program is not able to connect to the server, it expects to be able to find the data for the API calls in CSV files in `Data API/dat/*table*.csv`, and for the SQL calls in CSV files in `Data DB/*table*.csv`.
The tables should be named as described in the task.


### Work done

An overview of the work performed during the solving of this task is presented in `work_done.md`

### Database Schema
An overview of the database in its final form is presented in `database_overview.md`.