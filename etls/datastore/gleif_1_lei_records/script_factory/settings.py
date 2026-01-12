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

script_name = 'GLEIF 1 ORDERS'
script_version = '1.0'
script_description = 'ETL pipeline for ingesting and transforming order data'
script_frequency = "daily"  # "hourly", "daily", "weekly", "monthly"

# ===========================================================
# 2. Environment configuration
# ===========================================================

# 2.1 SCRIPT_RUNNER_ENV configuration
# Determined strictly by setenv.bat — no default fallback
# Options: "DEV", "PRODUCTION", "UAT", "TEST"
environment = os.environ.get("SCRIPT_RUNNER_ENV")

# 2.2 Machine and runtime environment identifiers
machine_env = os.environ.get("MACHINE_ENV")  # e.g. developer or server name
machine_script_runner_env = os.environ.get("MACHINE_SCRIPT_RUNNER_ENV")  # additional env flag if need

# 2.3 SMTP server configuration
smtp_server = os.environ.get("SCRIPT_RUNNER_SMTP_SERVER")

# 2.4 Project root and ETLs root
base_dir = os.environ.get("BASEDIR")   # Base directory (e.g. C:\Users\Mihail\PycharmProjects\datawarehouse)
etls_dir = os.environ.get("ETLS")      # Directory of etls folder (e.g. %BASEDIR%\ETLs)

# 2.5 Paths for logs, working directory and configuration file
logs_dir = os.environ.get("LOGS")                  # e.g. %BASEDIR%\logs
work_dir = os.environ.get("WORKDIR")               # e.g. %BASEDIR%\work
config_dir = os.environ.get("CONFIG_DIR")          # Directory of the configuration file

# 2.6 Virtual environment activation/deactivation commands
venv_activate = os.environ.get("SCRIPT_VIRTUAL_ENV")
venv_deactivate = os.environ.get("SCRIPT_VIRTUAL_ENV_DEACTIVATE")

# 2.7 Oracle client / SQL Server driver configuration
oracle_path = os.environ.get("ORACLE_PATH")             # e.g. set PATH=C:\Oracle\instantclient_19_23;%PATH%
sqlserver_driver = os.environ.get("SQLSERVER_DRIVER")   # e.g. ODBC+Driver+13+for+SQL+Server

# 2.8 Logging level
# log_level = os.environ.get("LOG_LEVEL")           # e.g. INFO / DEBUG / WARNING / ERROR

# 2.9 Proxy settings
# http_proxy = os.environ.get("HTTP_PROXY")
# https_proxy = os.environ.get("HTTPS_PROXY")

# 2.10 Custom tools path
# custom_tools_path = os.environ.get("CUSTOM_TOOLS_PATH")

# ===========================================================
# 3. Email routing configuration
# ===========================================================

email_recipients = {
    "admin"         : ["admin-team@company.com"],
    "business"      : ["business-owner@company.com"],
    "error_only"    : ["etl-alerts@company.com"]
}

# 3.1 Flattened email structure
recipients_list_admin = email_recipients["admin"]
recipients_list_business = email_recipients["business"]
error_recipients = email_recipients["error_only"]


# ===========================================================
# 4. Database configuration (production)
# ===========================================================

prod_database = 'postgresql: prod'
prod_schema = 'gleif'
prod_table = 'lei_records'
prod_file_name = f'_{prod_table}.csv'


# ===========================================================
# 5. Database configuration (development)
# ===========================================================

dev_database = 'postgresql: dev'
dev_schema = 'gleif'
dev_table = 'lei_records'
dev_file_name = f'_{prod_table}.csv'

# ===========================================================
# 6. ETL behavior configuration
# ===========================================================

# 6.1 Default load type (I = incremental, F = full)
load_type = "I"

# 6.2 Maximum number of days allowed for incremental loads
max_days_to_load = 365

# 6.3 Source for the project
sources = ['gleif.lei_records']

# ===========================================================
# 7. Project options
# ===========================================================

# Options for log and output folders
delete_log = True
delete_mail_logfile = True
delete_output = True

# Whether to send summary report emails
send_mail_report = True

# Conditions for sending log emails
send_mail_log_report = {
    "success"   : False,
    "fail"      : True
}

# ===========================================================
# 7. Folder base options
# ===========================================================
output_folder_base = 'output'