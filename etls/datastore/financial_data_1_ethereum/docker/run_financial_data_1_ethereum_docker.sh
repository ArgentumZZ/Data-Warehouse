#!/bin/bash
# 1. 'Shebang': Tells the operating system to use the Bash interpreter inside the Airflow container.
# located at /bin/bash to execute this script.

# 2. Exit immediately if any command returns a non-zero (error) status (exit on error).
set -e

# 3. Define the project paths and configuration settings
# # ETL_ROOT is the base directory for all connectors and shared utilities.
ETL_ROOT="/opt/airflow/etls"

# PROJ_ROOT is the specific home for this script
PROJ_ROOT="/opt/airflow/etls/datastore/financial_data_1_ethereum"

# CONFIG_DIR points Python to the credentials file.
export CONFIG_DIR="/opt/airflow/config/local/db_config.cfg"

# Set the PYTHONPATH, so Python knows where to look when we use import statements.
# ${ETL_ROOT}: Allows "import connectors.postgresql_connector"
# ${PROJ_ROOT}: Allows "import custom_code.script_factory"
export PYTHONPATH="${ETL_ROOT}:${PROJ_ROOT}"

# 4. Navigate to the execution folder and echo some messages
cd "${PROJ_ROOT}/script_runner"

echo "PYTHONPATH set to: $PYTHONPATH"
echo "Launching run_script.py from script_runner"

# 5. Run the Python script.
python3 run_script.py

# 6. Return the Python exit code to Airflow so the DAG task reflects success/failure.
# '$?' captures the exit status of the python3 command.
# 0 = success (green in Airflow), non-zero = failure (rRed in Airflow).
exit $?