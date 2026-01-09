"""
Purpose:
    Centralized configuration file for the ETL script. A static configuration.
    Contains:
        - script metadata
        - environment-specific settings
        - ingestion frequency
        - database/schema/table names
        - email routing configuration
        - ETL behavior flags (load type, batch size, query names)

    This file should contain ONLY configuration and constants.
    No logic should be implemented here.
"""

import os

# ===========================================================
# 1. Script metadata
# ===========================================================

script_name = "orders_ingestion"
script_version = "1.0.0"
script_description = "ETL pipeline for ingesting and transforming order data"


# ===========================================================
# 2. Environment configuration
# ===========================================================

# 2.1 Options: "local", "development", "staging", "production"
# Determined strictly by setenv.bat — no default fallback
environment = os.environ.get("SCRIPTRUNNER_ENV")

# 2.2 Frequency of ingestion: "hourly", "daily", "weekly", "monthly"
frequency = "daily"

# 2.3 Machine and runtime environment identifiers (from setenv.bat)
machine_id = os.environ.get("ENV")                  # e.g. developer name or server name
script_runner_env = os.environ.get("SCRIPT_RUNNER_ENV")  # legacy / additional env flag if used

# 2.4 SMTP server configuration
smtp_server = os.environ.get("SCRIPT_RUNNER_SMTP_SERVER")

# 2.5 Project root and ETLs root (paths should not be in Git)
base_dir = os.environ.get("BASEDIR")               # e.g. C:\Users\Mihail\PycharmProjects\datawarehouse
etls_dir = os.environ.get("ETLS")                  # e.g. %BASEDIR%\ETLs

# 2.6 Virtual environment activation/deactivation commands
venv_activate = os.environ.get("SCRIPT_VIRTUAL_ENV")
venv_deactivate = os.environ.get("SCRIPT_VIRTUAL_ENV_DEACTIVATE")

# 2.7 Oracle client / SQL Server driver configuration
oracle_path = os.environ.get("ORACLE_PATH")        # e.g. set PATH=C:\Oracle\instantclient_19_23;%PATH%
sqlserver_driver = os.environ.get("SQLSERVER_DRIVER")  # e.g. ODBC+Driver+13+for+SQL+Server

# 2.8 Optional paths: logs, working directory, config directory
logs_dir = os.environ.get("LOGS")                  # e.g. %BASEDIR%\logs
work_dir = os.environ.get("WORKDIR")              # e.g. %BASEDIR%\work
config_dir = os.environ.get("CONFIGDIR")          # e.g. %BASEDIR%\config

# 2.9 Logging level
log_level = os.environ.get("LOG_LEVEL")           # e.g. INFO / DEBUG / WARNING / ERROR

# 2.10 Proxy settings
http_proxy = os.environ.get("HTTP_PROXY")
https_proxy = os.environ.get("HTTPS_PROXY")

# 2.11 Custom tools path
custom_tools_path = os.environ.get("CUSTOM_TOOLS_PATH")


# ===========================================================
# 3. Email routing configuration
# ===========================================================

email_recipients = {
    "admin": ["admin-team@company.com"],
    "business": ["business-owner@company.com"],
    "error_only": ["etl-alerts@company.com"]
}

# 3.1 Flattened email routing (for convenience)
recipients_list_admin = email_recipients["admin"]
recipients_list_business = email_recipients["business"]
error_recipients = email_recipients["error_only"]


# ===========================================================
# 4. Database configuration (production)
# ===========================================================

prod_database = "dwh_prod"
prod_schema = "dwh"
prod_table = "fact_orders"


# ===========================================================
# 5. Database configuration (development)
# ===========================================================

dev_database = "dwh_dev"
dev_schema = "dwh_dev"
dev_table = "fact_orders_dev"


# ===========================================================
# 6. ETL behavior configuration
# ===========================================================

# 6.1 Default load type (I = incremental, F = full)
load_type_default = "I"

# 6.2 Maximum number of days allowed for incremental loads
max_days_to_load = 365

# 6.3 Load mode for this pipeline
# Options: "full", "incremental"
load_type = "incremental"

# 6.4 Batch size for chunked extraction or processing
batch_size = 50000

# 6.5 SQL query identifier used by ScriptWorker
sql_query_name = "get_orders"

# 6.6 Source table for extraction
source_table = "staging.stg_orders"

# 6.7 Target table for loading
target_table = "dwh.fact_orders"


# ===========================================================
# 7. Project options
# ===========================================================

delete_log = True
delete_mail_logfile = True
delete_output = True

# Whether to send summary report emails
send_mail_report = False

# Conditions for sending log emails
send_mail_log_report = {
    "success": False,
    "fail": True
}


# ===========================================================
# 8. Output folder configuration
# ===========================================================

output_folder_base = "output"


# ===========================================================
# 9. Active environment database/schema/table selection
# (ScriptFactory will use these based on `environment`)
# ===========================================================

# These are placeholders; ScriptFactory will assign the correct ones at runtime
target_database = None
schema = None
table_name = None
