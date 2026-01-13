## Data Warehouse

- **Data Warehouse**
  - Extracts, Transforms and Load data from different sources into a PostgresSQL database.
- **Main folders**
  - `dags` - DAG files for Airflow (in progress).
  - `script_connectors` folder - Different source connectors (in progress).
  - `utilities` folder - Utilities files (in progress).
  - `logs` - logs of each project's run.
  - `tests` folder - Tests (in progress).
  - `warehouse` folder - dim and fact tables (in progress).
  - `data_quality_checks` folder - Custom data quality checks (in progress).
  - `aggregations` - Data aggregations (in progress).
- **Main files**
  - `etl_audit_manager.py` - audit table class (in progress).
  - `etl_utils.py` - general ETL utils functions.
  - `script_worker.py` - custom functions for a given project. 
    - connect to source, transform, load.
  - `sql_queries.py` - SQL queries that are parametrized.
  - `alter_tables.sql` - To track executed SQL queries. 
  - `script_factory.py` - A file that builds the tasks for the project.
  - `script_runner.py` - Run the tasks in script_factory.
  - `.bat/.sh` files to run script_runner.

___
## 📝 Project To‑Do Plan

- **Partial Task Functions**
  - Add parameter‑accepting partial functions inside `script_factory.py` .✔️
  - Improve modularity and reusability of task definitions. ✔️

- **ETL Audit Manager**
  - Create an audit table to track project's run metadata. ✔️
  - Capture `start/end_load_date`, `start/end_script_execution_time`,` data_min/max_dates`, `status`, `number_of_records`, `environment`, `script_version`, `load_type`, `previous_max_date`, `target_database`, `target_table`. ✔️ 
  - Create a `create_etl_runs_table` function. ✔️
  - Create an `insert_etl_runs_record` function. ✔️
  - Create an `update_etl_runs_record` function. ✔️
  
- **ETL Utilities**
  - Add general ETL helper functions.
  - Optionally create a `utilities/` folder with separate modules.

- **Incremental & Full Load**
  - Implement logic for both incremental (I) and full (F) load modes. ✔️
  - Override internal defaults with values from `.bat file.` ✔️

- **Logging**
  - Create a logging_manager.py to standardize log format. ✔️
  - Create a `logs/` folder to store logs for each project's run. ✔️
  - Automatically delete logs older than 7 days. ✔️
  - Automatically delete logs older than N runs.
  
- **Utilities folder**
  - Create utilities .py files for ETL processes (e.g. file_utils.py, argument_parser.py, errors_utils, db_utils.py)️

- **Output folder**
  - Create an `output/` folder to store generated files.
  - Control with a boolean operator, whether an output file will be saved to the folder.

- **Email Notifications (SMTP)**
  - Implement e-mail success/failure alerts after each project's run..
  - Include run summary and error details.

- **Dockerization**
  - Add a `Dockerfile` for containerized execution.
  - Ensure compatibility with Windows/Linux runners.

- **Launcher Scripts**
  - Update `.bat`, `.sh`, `_docker.bat`, `_docker.sh`
  - Add parameter parsing, environment selection, and error handling

- **Additional Connectors**
  - PostgreSQL (implemented) 
  - Oracle
  - MySQL
  - Salesforce
  - MSSQL
  - Snowflake
  - Google BigQuery
  - S3
