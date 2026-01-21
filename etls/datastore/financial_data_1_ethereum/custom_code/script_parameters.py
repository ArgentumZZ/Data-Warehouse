"""
    Centralized static configuration file for the ETL script.

    Contains:
        - script metadata
        - environment-specific settings
        - ingestion frequency
        - database/schema/table names
        - email routing configuration
        - ETL behavior flags (load type, batch size, query names)

    This file must contain ONLY configuration and constants. No logic should be implemented here.
"""

import os

# ===========================================================
# 1. Script metadata
# ===========================================================

script_name = 'FINANCIAL DATA 1 ETHEREUM'
script_version = '1.0'
script_description = 'ETL pipeline for ingesting and transforming ethereum data'
script_frequency = "daily"  # "hourly", "daily", "weekly", "monthly"
script_primary_owner = "Mihail Mihaylov"
script_secondary_owner = ""

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
# logs_dir = os.environ.get("LOGS")                  # e.g. %BASEDIR%\logs
# work_dir = os.environ.get("WORKDIR")               # e.g. %BASEDIR%\work
config_dir = os.environ.get("CONFIG_DIR")          # Directory of the configuration file

# 2.6 Virtual environment activation/deactivation commands
# venv_activate = os.environ.get("SCRIPT_VIRTUAL_ENV")
# venv_deactivate = os.environ.get("SCRIPT_VIRTUAL_ENV_DEACTIVATE")

# 2.7 Oracle client / SQL Server driver configuration
oracle_path = os.environ.get("ORACLE_PATH")             # e.g. set PATH=C:\Oracle\instantclient_19_23;%PATH%
sqlserver_driver = os.environ.get("SQLSERVER_DRIVER")   # e.g. ODBC+Driver+13+for+SQL+Server

# 2.8 Proxy settings
# http_proxy = os.environ.get("HTTP_PROXY")
# https_proxy = os.environ.get("HTTPS_PROXY")

# 2.9 Custom tools path
# custom_tools_path = os.environ.get("CUSTOM_TOOLS_PATH")

# ===========================================================
# 3. Email routing configuration (PROD / DEV)
# ===========================================================

prod_email_recipients = {
    "admin"         : ["admin-team@company.com"],
    "business"      : ["business-owner@company.com"],
    "error"         : ["etl-alerts@company.com"]
}

# 3.1 Flattened email structure (PROD)
prod_list_recipients_admin = prod_email_recipients["admin"]
prod_list_recipients_business = prod_email_recipients["business"]
prod_list_recipients_error = prod_email_recipients["error"]

prod_is_email_enabled_dict = {
    "admin"      : True,
    "business"   : False,
    "error"      : True
}

prod_is_admin_email_alert_enabled = prod_is_email_enabled_dict["admin"]
prod_is_business_email_alert_enabled = prod_is_email_enabled_dict["business"]
prod_is_error_email_alert_enabled = prod_is_email_enabled_dict["error"]

###################################################################
dev_email_recipients = {
    "admin"         : ["admin-team@company.com"],
    "business"      : ["liahim13@gmail.com"],
    "error"         : ["liahim13@gmail.com"]
}

# 3.2 Flattened email structure (DEV)
dev_list_recipients_admin = dev_email_recipients["admin"]
dev_list_recipients_business = dev_email_recipients["business"]
dev_list_recipients_error = dev_email_recipients["error"]

dev_is_email_enabled_dict = {
    "admin"      : False,
    "business"   : True,
    "error"      : True
}

dev_is_admin_email_alert_enabled = dev_is_email_enabled_dict["admin"]
dev_is_business_email_alert_enabled = dev_is_email_enabled_dict["business"]
dev_is_error_email_alert_enabled = dev_is_email_enabled_dict["error"]

# ===========================================================
# 4. Database configuration (production)
# ===========================================================

prod_database = 'postgresql: prod'
prod_schema = 'financial_data'
prod_table = 'ethereum'

# ===========================================================
# 5. Database configuration (development)
# ===========================================================

dev_database = 'postgresql: dev'
dev_schema = 'financial_data'
dev_table = 'ethereum'

# ===========================================================
# 6. ETL behavior configuration
# ===========================================================

# 6.1 Default load type (I = incremental, F = full)
load_type = "I"

# 6.2 Maximum number of days allowed for incremental loads
max_days_to_load = 365

# 6.3 Source for the project
sources = ['financial_data.ethereum']

# ===========================================================
# 7. Project options (PROD and DEV)
# ===========================================================

# Options for log and output folders
prod_delete_log = True
prod_delete_output = True

###################################################################
# Options for log and output folders
dev_delete_log = True
dev_delete_output = True
####################################################################