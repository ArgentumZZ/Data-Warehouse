## 🛢️Data Warehouse

- **Data Warehouse**
  - A personal data warehouse ecosystem project that will implement modern data‑engineering patterns. 
  - The goal is to build a scalable, maintainable platform for analytics.
  - The project will include a modular ETL framework, Dockerized execution environment, Airflow orchestration, configuration‑based execution. 
  - It will provide reusable connectors and utilities, a structured warehouse layer with dimensional and fact tables, views, email notifications, data quality checks, data warehouse process monitoring and backfilling for historical data recovery.
  
- **Main folders**
  - `dags` - DAG files for Airflow.
  - `connectors` - Connectors to different DB and non-DB sources.
  - `utilities` - Utilities files.
  - `metadata/logs` - Logs generated for each project's run.
  - `metadata/output` - Files generated for each project's run.
  - `tests` - Unit tests.
  - `warehouse` - Dimensional and fact tables.
  - `views` - Custom views.
  - `data_quality_checks` - Custom data quality checks (record discrepancies, stale projects, missing keys, null values, duplicates).
  - `aggregations` - Data aggregations - windowing (time-based), dimensional grouping (bucketing), change detection (delta aggregations), cumulative sum (running totals).
  - `docker` - Dockerfile, requirements.txt and .sh run files.
  - `custom_code` - Custom code (.py files) for each project.
  - `sript_factory` - A central assembly factory that builds the tasks for execution.
  - `script_runner` - Files (`.bat / .sh`) that run `run_script.py`.
- **Main files**
  - `etl_audit_manager.py` - An audit table that keeps track of project's run metadata.
  - `etl_utils.py` - ETL transformation functions.
  - `script_worker.py` - Custom functions for a given project. 
  - `sql_queries.py` - Parametrized SQL queries.
  - `alter_tables.sql` - History of executed SQL queries. 
  - `script_factory.py` - Assemble the tasks for the project.
  - `script_parameters.py` - Custom project parameters (script_name, version, load type, etc.)
  - `run_script.py` - Runs the tasks in `script_factory.py`.
  - `.bat/.sh` - Files used to run `run_script.py`.
  - `Dockerfile` + `_docker.bat`/`_docker.sh`  - Defines the docker metadata, builds an image and runs the container. 
-------------------------------
## 📁 Folder structure
```
datawarehouse/
├── .gitignore
├── README.md
├── venv/
├── requirements_python_3_14.txt
│
├── orchestration
│   ├── logs/
│   │     ├── dag_id = dwh_main_dag
│   │     │     ├── run_id=manual__2026-01-27T092954.691723+0000
│   │     │     └── other run_ids ...
│   │     ├── dag_processor_manager
│   │     └── scheduler
│   ├── plugins/
│   ├── .env
│   ├── docker_compose.yaml
│   └── requirements.txt
│
├── config/
│   └── local/
│       ├── db_config.cfg
│       ├── keyfile_1.pem
│       ├── keyfile_2.pkk
│       ├── api_credentials.json
│       ├── setenv.bat
│       ├── setenv.sh
│       └── ... other credential files ...
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
│   │   ├── financial_data_1_ethereum/
│   │   │       ├── custom_code/
│   │   │       │      ├── alter_tables.sql
│   │   │       │      ├── script_factory.py
│   │   │       │      ├── script_parameters.py
│   │   │       │      ├── script_worker.py
│   │   │       │      └── sql_queries.py
│   │   │       ├── docker/
│   │   │       │      ├── Dockerfile
│   │   │       │      ├── requirements.txt
│   │   │       │      ├── run_financial_data_1_ethereum_docker.bat
│   │   │       │      └── run_financial_data_1_ethereum_docker.sh
│   │   │       ├── metadata/
│   │   │       │      ├── logs/
│   │   │       │      │      ├── 2026-01-18_11-36-19_etl.log
│   │   │       │      │      ├── 2026-01-18_11-40-17_etl.log
│   │   │       │      │      └── ... other log _etl.log files ...
│   │   │       │      ├── output/
│   │   │       │      │      ├── 2026-01-18_11-36-19.csv
│   │   │       │      │      ├── 2026-01-18_11-40-17.csv
│   │   │       │      │      └── ... other output .csv files ...
│   │   │       │      ├── dictionary.yaml
│   │   │       │      ├── mapping.yaml
│   │   │       │      ├── schema.yaml
│   │   │       │      ├── validation.yaml
│   │   │       │      └── ... other .yaml files ...
│   │   │       ├── script_runner/
│   │   │       │      ├── __init__.py
│   │   │       │      ├── run_financial_data_1_ethereum.bat
│   │   │       │      ├── run_financial_data_1_ethereum.sh
│   │   │       │      └── run_script.py
│   │   │       └──  __init__.py
│   │   ├── alpaca_1_revenue/
│   │   ├── crypto_1_transactions/
│   │   └── ... other datastore project folders ...
│   │
│   ├── tests/
│   │   ├── __init__.py
│   │   ├── test_connectors.py
│   │   ├── test_utils.py
│   │   ├── test_worker.py
│   │   └── ... other test .py files ...
│   │
│   ├── utilities/
│   │   ├── __init__.py 
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
│       ├── ... other warehouse project folders ...
│       │  
│       └── views/
│             ├── view_1_revenue.py
│             └── ... other view files ...
```
___
## 📝 Project To‑Do Plan

- **Partial task functions**
  - Add parameter‑accepting partial functions inside `script_factory.py`. ✔️
  - Improve modularity and reusability of task definitions. ✔️
  - Add task name, description, retries, is_enabled and dependency parameters. ✔️
  - Add retry, enabled and dependency checks in `run_script.py`. ✔️

- **ETL audit manager**
  - Create an audit table to track project's run metadata. ✔️
  - Capture `start/end_load_date`, `start/end_script_execution_time`,` data_min/max_dates`, `status`, `number_of_records`, `environment`, `script_version`, `load_type`, `previous_max_date`, `target_database`, `target_table`. ✔️ 
  - Create a `create_etl_runs_table` function. ✔️
  - Create a `_calculate_etl_window` internal function to calculate sdt and edt. ✔️
  - Create an `insert_etl_runs_record` function. ✔️
  - Create an `update_etl_runs_record` function. ✔️
  
- **ETL utilities**
  - Add custom ETL transformation functions. ✔️
  - Add a single `transform_dataframe` function that applies transformations. ✔️
  - Add a `process_dataframe_date_ranges` function to calculate `data_min_date` and `data_max_date`. ✔️
  - Add a `check_source_date_range` function to determine whether the source data is up to date.
  - Add a `run_data_quality_check` function to run custom data quality checks.
  - Add a `delete_target_dates` function for partition-based deletion for fact tables.
  - Add a `set_reference_page` function to create a link to the corresponding ETL reference page in Confluence. 
  
- **Incremental and full Load**
  - Implement logic for incremental (I) and full (F) load modes. ✔️
  - Override internal defaults with values from `.bat file.` ✔️

- **Logging**
  - Create a logging_manager.py to standardize log format. ✔️
  - Create a `logs/` folder to store logs for each project's run. ✔️
  - Add a `cleanup_old_logs` function that automatically deletes .logs older than N days ot older than N runs. ✔️
  - Add a `get_current_log_content` function to read the current log. ✔️
  - Add a `get_current_log_size` function that acts as a pointer and returns the current log size. ✔️

- **Utilities folder**
  - `argument_parser.py` - Reads the arguments from .bat <param_1> <param_2> ... <param_n>. ✔️
  - `config_utils.py` - Reads the credentials in configuration files (e.g., _.cfg). ✔️
  - `db_utils.py` - Database utilities.
  - `dq_utils.py` - Data quality utilities.
  - `email_manager.py` - Create and send e-mails. ✔️
  - `error_utils.py` - Custom classes for error handling. ✔️
  - `file_utils.py` - File path and folder utility functions. ✔️
  - `etl_audit_manager.py` - Custom audit table. ✔️
  - `etl_utils.py` - Custom ETL transformations. ✔️
  - `logging_manager.py` - Custom logging handlers, formatters, traceback and stack level. ✔️

- **Output folder**
  - Create an `output/` folder to store generated files. ✔️
  - Store output in format `output/file_name_timestamp.csv`. ✔️
  - Add a boolean operator to control whether the file will be deleted from the folder. ✔️

- **Containerization**
  - Add a `Dockerfile` for containerized execution. ✔️
  - Update `_docker.bat` to run the container. ✔️
  - Update `_docker.sh` to run the container.
  - Ensure compatibility with Windows/Linux.

- **Launcher scripts**
  - Update `.bat` ✔️, `_docker.bat` ✔️, `.sh`, `_docker.sh`.
  - Add parameter parsing and variable definitions. ✔️
  - Add echoes and error handling. ✔️

- **Email notifications**
  - Implement e-mail success/failure alerts after each project's run. ✔️
  - Include ETL run summary, logs and error details.
  - Add business, admin and error recipients. ✔️
  - Add a boolean operators to control whether the recipients should receive an e-mail. ✔️
  - Add a `load_smtp_config` function to read e-mail credentials (moved to config_utils.py in utilities). ✔️ 
  - Add a `add_task_result_to_email` function to build task execution log incrementally. ✔️ 
  - Add a `add_log_block_to_email` function to build technical log details incrementally. ✔️
  - Add a `prepare_emails` function to build e-mails based on general info, task execution log and technical log details. ✔️
  - Add a `send_emails` function that sends the prepared e-mails. ✔️
  - Add a `smtp_send` function that executes the technical transmission of an email via SMTP. ✔️

- **Backfill**
  - Implement backfill loading option in `.bat` ✔️, `_docker.bat` ✔️, `.sh`, `_docker.sh` files.
  - Create a separate backfill project that accepts `project_name`, `start_date`, `end_date`, `load_days` to run a given project and load data incrementally.

- **Fact and dimensional tables**
  - Add staging and normal fact and dimensional tables (staging.dim → warehouse.dim → staging.fact → warehouse.fact). 
  - Build a staging.dim table to truncate staging area, extract data from the source and add checks for null values, duplicates, missing data.
  - In warehouse.dim, implement slowly changing dimensions - type 1 and type2 updates - close SCD2 rows, insert new SCD2 rows, apply SCD1 updates and insert new rows.
  - Add indices for SCD detection and ETL MERGE/UPDATEs, partial unique index, fast fact and point-in-time lookups, prevent SCD2 ranges overlapping.
  - Implement staging.fact table to truncate staging area, extract data from the source, build the correct fact grain and data quality checks.
  - In warehouse.fact, enforce referential integrity to prevent orphaned surrogate keys, partition the table by date/date_key, add partition-based deletion and build an index strategy.

- **Orchestration**
  - Add an `orchestration` folder with `logs` and `plugins` folders, `.env`, `docker-compose.yaml` and `requirements.txt`
  - Implement orchestration with Airflow and document the steps. ✔️
  - Add retries, SLA levels, backfilling.
  - Add parametrization for dynamic data handling ({{ ds }})
  - Dependency management - sensors/external task markers and branching.
  - Monitor and maintain with XComs, logs, alerts, and task groups.
  - Build DAGs for specific cases (daily, 30 min , weekly, monthly DAGs).

- **Monitoring**
  - Implement data warehouse health monitoring with dashboards in Apache Superset.
  - Monitor index efficiency and usage, dead tuple counts (vacuum and bloat monitoring).
  - Monitor blocked queries, long-running queries (wait event analysis, query plan regressions, temporary database spills to disk).
  - Monitor buffer cache and I/O performance, connection pool health, add transaction and throughput metrics.
  - Monitor data quality metrics - freshness/latency, volume anomalies, schema evolution, null and uniqueness checks.
  - Monitor cost and resource governance - cost per query, user/role resource consumption, storage growth trends.

- **Data quality checks**
  - Add general data quality checks that run after the DAGs.
  - Monitor record discrepancies (row counts), null values, referential integrity (foreign key checks).
  - Monitor SLA checks (arrival delays), schema drift (new/missing columns), gaps (missing data per day).
  - Late-arriving data handling.

- **Documentation**
  - Add documentation in Confluence for the different processes in the data warehouse.
  - Overall data architecture (data lineage map + entity relationship diagrams).
  - Developer guide: How to build a project.
  - Tech stack (name + version).
  - ETL reference pages.
  - Data governance.

- **Connectors**
  - Add more connectors:
  - Relational databases (OLTP): PostgreSQL ✔️, MySQL, MSSQL, Oracle
  - Cloud data warehouses (OLAP): Snowflake, Google BigQuery
  - Time-series databases: kdb+
  - Object storage (data lakes): S3 (AWS), Azure Blob Storage, GCS
  - SaaS/API connectors: Salesforce, REST APIs (with requests)
  - SFTP and local files (file based ingestion): SFTP (with pysftp/paramiko), pandas (for local files)

- **Other**
  - Apache Spark - distributed processing system used for big data workloads.
  - Apache Kafka - streaming and event processing.
  - DBT - SQL-based transformation framework that automates the building, testing, and documenting of modular data pipelines.
___
## 💻 Environment setup
- **I. Create a virtual environment (Windows)**
  - Go to your project folder (e.g., cd C:\Users\Mihail\PycharmProjects\datawarehouse)
  - Create the environment → python -m venv venv

- **II. Activate the virtual environment**
  - Activate the environment -> .\venv\Scripts\activate
  - where python → the first path should point to ...\datawarehouse\venv\Scripts\python.exe
- **III. Check locally installed dependencies**
  - pip list

- **IV. Install the dependencies**
  - pip install -r <file_path>\requirements.txt

- **V. Final check**
  - pip list
  - where python → the first path should point to ...\datawarehouse\venv\Scripts\python.exe