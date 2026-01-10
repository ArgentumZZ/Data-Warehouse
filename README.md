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
