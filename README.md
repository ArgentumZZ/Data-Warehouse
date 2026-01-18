## Data Warehouse

- **Data Warehouse**
  - Extract, transform and load data from different sources into a PostgresSQL database.
- **Main folders**
  - `dags` - DAG files for Airflow.
  - `connectors` - Connectors to different DB and non-DB sources.
  - `utilities` - Utilities files.
  - `metadata/logs` - Logs generated for each project's run.
  - `metadata/output` - Files generated for each project's run.
  - `tests` - Unit tests.
  - `warehouse` - Dim and fact tables.
  - `views` - Custom views.
  - `data_quality_checks` - Custom data quality checks.
  - `aggregations` - Data aggregations.
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
-------------------------------
## General structure
```
datawarehouse/
├── .gitignore
├── README.md
├── venv/
│
├── config/
│   ├── local/
│   │   ├── db_config.cfg
│   │   ├── keyfile_1.pem
│   │   ├── keyfile_2.pkk
│   │   ├── setenv.bat
│   │   ├── setenv.sh
│   │   └── ... other pem/pkk files ...
│
├── dags/
│   ├── dwh_main_dag.py
│   └── ... other DAG .py files ...
│
├── etls/
│   ├── _templates/
│   │   └── (general template files)
│   │
│   ├── aggregations/
│   │   ├── aggregation_1_revenue/
│   │   └── ... other aggregation projects ...
│   │
│   ├── connectors/
│   │   ├── postgresql_connector.py
│   │   ├── mysql_connector.py
│   │   ├── oracle_connector.py
│   │   └── ... other connectors...
│   │
│   ├── data_quality_checks/
│   │   ├── dqc_1_calculate_record_discrepancies/
│   │   └── ... other DQC projects ...
│   │
│   ├── datastore/
│   │   ├── alpaca_1_revenue/
│   │   ├── crypto_1_transactions/
│   │   ├── financial_data_1_ethereum/
│   │   └── ... other project folders ...
│   │
│   ├── utilities/
│   │   ├── argument_parser.py
│   │   ├── email_manager.py
│   │   ├── etl_audit_manager.py
│   │   ├── etl_utils.py
│   │   ├── date_utils.py
│   │   ├── db_utils.py
│   │   ├── dq_utils.py
│   │   ├── file_utils.py
│   │   └── ... other utils files ...
│   │
│   └── warehouse/
│       ├── dim_1_dim_crypto_transactions/
│       ├── dim_1_staging_crypto_transactions/
│       ├── fact_1_fact_shares_revenue/
│       ├── fact_1_staging_shares_revenue/
│       └── views/
```
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
  - Add custom ETL transformation functions. ✔️
  - Add a single `transform_dataframe` function that applies transformations. ✔️
  - Add `process_dataframe_date_ranges` function to calculate `data_min_date` and `data_max_date`. ✔️

- **Incremental and full Load**
  - Implement logic for both incremental (I) and full (F) load modes. ✔️
  - Override internal defaults with values from `.bat file.` ✔️

- **Logging**
  - Create a logging_manager.py to standardize log format. ✔️
  - Create a `logs/` folder to store logs for each project's run. ✔️
  - (Optionally) Automatically delete .logs older than 7 days. ✔️
  - (Optionally) Automatically delete .logs older than N runs. ✔️
  
- **Utilities folder**
  - Create utilities .py files for ETL processes (e.g. file_utils.py, argument_parser.py, errors_utils, db_utils.py)️. ✔️

- **Output folder**
  - Create an `output/` folder to store generated files. ✔️
  - Format `output/file_name_timestamp.csv`. ✔️
  - Control with a boolean operator, whether the file will be deleted from the folder. ✔️

- **Containerization** (in progress)
  - Add a `Dockerfile` for containerized execution. ✔️
  - Update `_docker.bat` to run the container. ✔️
  - Update `_docker.sh` to run the container.
  - Ensure compatibility with Windows/Linux.

- **Launcher scripts** (in progress)
  - Update `.bat` ✔️, `_docker.bat` ✔️, `.sh`, `_docker.sh`.
  - Add parameter parsing and variable definitions. ✔️
  - Add logs and error handling. ✔️

- **Email Notifications (SMTP)**
  - Implement e-mail success/failure alerts after each project's run.
  - Include run summary and error details.

- **Backfill**
  - Implement backfill loading option in `.bat` ✔️, `_docker.bat` ✔️, `.sh`, `_docker.sh` files.
  - Create a separate backfill project that accepts `project_name`, `start_date`, `end_date`, `load_days` to run a given project and load data incrementally.

- **Connectors**
  - Add more connectors:
  - PostgreSQL (implemented) ✔️  
  - Oracle
  - MySQL
  - Salesforce
  - MSSQL
  - Snowflake
  - Google BigQuery
  - S3