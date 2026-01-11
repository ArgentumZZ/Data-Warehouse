## Data Warehouse

- **Data Warehouse**
  - Extracts, Transforms and Load data from different source into a PostgresSQL database.
  - Data quality checks
- **Main folders**
  - dags - DAG files for Airflow (in progress).
  - script_connectors folder - Different source connectors (in progress).
  - utilities folder - Utilities files (in progress).
  - tests folder - Tests (in progress).
  - warehouse folder - dim and fact tables (in progress).
  - data_quality_checks folder - Custom data quality checks (in progress).
  - aggregations - Data aggregations (in progress).
- **Main files**
  - etl_audit_manager - audit table (in progress).
  - etl_utils - general ETL utils functions.
  - script_worker - custom functions for a given project. 
    - connect to source, transform, load.
  - sql_queries - SQL queries that are parametrized.
  - alter_tables.sql - To track executed SQL queries. 
  - script_factory - A file that builds the tasks for the project.
  - script_runner - Run the tasks in script_factory.
  - .bat/.sh files to run script_runner.

___
## 📝 Project To‑Do Plan

- **Partial Task Functions**
  - Add parameter‑accepting partial functions inside `script_factory.py`
  - Improve modularity and reusability of task definitions

- **ETL Audit Manager**
  - Create an audit table to track run metadata
  - Capture start/end time of script, data min/max date, status, row counts, errors, source, environment info, etc.

- **ETL Utilities**
  - Add general ETL helper functions
  - Optionally create a `utilities/` folder with separate modules

- **Incremental & Full Load (I/F)**
  - Implement logic for both incremental and full refresh modes
  - Add configuration flags and metadata tracking

- **Logging**
  - Create a dedicated `logs/` folder
  - Standardize log format

- **Email Notifications (SMTP)**
  - Add success/failure email alerts
  - Include run summary and error details

- **Dockerization**
  - Add a `Dockerfile` for containerized execution
  - Ensure compatibility with Windows/Linux runners

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
