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
environment = "development"

# 2.2 Frequency of ingestion: "hourly", "daily", "weekly", "monthly"
frequency = "daily"


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
