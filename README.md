## Data Warehouse

- **Data Warehouse**
  - Extract, transform and load data from different sources into a PostgresSQL database.
- **Main folders**
  - `dags` - DAG files for Airflow (in progress).
  - `connectors` folder - Connectors to different DB and non-DB sources (in progress).
  - `utilities` folder - Utilities files (in progress).
  - `metadata/logs` - logs generated for each project's run.
  - `metadata/output` - files generated for each project's run.
  - `tests` folder - Unit tests (in progress).
  - `warehouse` folder - dim and fact tables (in progress).
  - `data_quality_checks` folder - Custom data quality checks (in progress).
  - `aggregations` - Data aggregations (in progress).
  - `docker` - Dockerfile, requirements.txt and .sh run files.
  - `customer_code` - Custom code for each project.
  - `sript_factory` - Central assembly factory, take info from all other files to create the tasks for execution.
  - `script_runner` - Files (`.bat / .sh`) that run `script_runner.py` which initializes the `script_factory.py`.
- **Main files**
  - `etl_audit_manager.py` - audit table.
  - `etl_utils.py` - general ETL utils functions.
  - `script_worker.py` - custom functions for a given project. 
  - `sql_queries.py` - SQL queries that are parametrized.
  - `alter_tables.sql` - Track executed SQL queries. 
  - `script_factory.py` - Assemble the tasks for the project.
  - `script_runner.py` - Run the tasks in `script_factory.py`.
  - `.bat/.sh` files to run `script_runner.py`.

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

- **Incremental and full Load**
  - Implement logic for both incremental (I) and full (F) load modes. ✔️
  - Override internal defaults with values from `.bat file.` ✔️

- **Logging**
  - Create a logging_manager.py to standardize log format. ✔️
  - Create a `logs/` folder to store logs for each project's run. ✔️
  - (Optionally) Automatically delete .logs older than 7 days. ✔️
  - (Optionally) Automatically delete .logs older than N runs. ✔️
  
- **Utilities folder**
  - Create utilities .py files for ETL processes (e.g. file_utils.py, argument_parser.py, errors_utils, db_utils.py)️

- **Output folder**
  - Create an `output/` folder to store generated files. ✔️
  - Format `output/file_name_timestamp.csv`. ✔️
  - Control with a boolean operator, whether the file will be deleted from the folder. ✔️

- **Email Notifications (SMTP)**
  - Implement e-mail success/failure alerts after each project's run.
  - Include run summary and error details.

- **Dockerization**
  - Add a `Dockerfile` for containerized execution.
  - Ensure compatibility with Windows/Linux runners.

- **Launcher scripts**
  - Update `.bat`, `.sh`, `_docker.bat`, `_docker.sh`
  - Add parameter parsing, environment selection, and error handling.

- **Backfill**
  - Implement backfill loading option in `.bat`, `.sh`, `_docker.bat`, `_docker.sh` files.
  - Create a separate backfill project that accepts `project_name`, `start_date`, `end_date`, `load_days` to load data incrementally.
- **Additional Connectors**
  - PostgreSQL (implemented) 
  - Oracle
  - MySQL
  - Salesforce
  - MSSQL
  - Snowflake
  - Google BigQuery
  - S3