# Import libraries
import configparser, csv, os, requests
import pandas as pd
from typing import Any, Dict, Optional
from pathlib import Path
from datetime import datetime

# Import custom libraries
import utilities.logging_manager as lg
from utilities.etl_utils import EtlUtils
from connectors.postgresql_connector import PostgresConnector
from custom_code.sql_queries import sql_queries, source_columns_unique


class ScriptWorker:
    """
        General-purpose ETL worker skeleton.
        Handles:
        - credentials
        - connection
        - extraction
        - transformation
        - saving
        - uploading
    """


    def __init__(self, sfc: Any):

        self.sfc = sfc   # pointer to the script factory class
        self.num_of_records = None
        self.status = None
        self.data_min_date = None
        self.data_max_date = None
        lg.info("Script worker instantiated!")

    # ---------------------------
    # 1. Credentials
    # ---------------------------
    def get_credentials(self, name: str) -> dict:
        """
        Load credentials from config or external provider.
        Get credentials from .cfg file if there is no custom connector.
        """
        pass

    # ---------------------------
    # 2. Extract, Transform, Save
    # ---------------------------
    def get_data(self, file_path: str):
        """
        Create a DB/API connection using loaded credentials.
        Use the specific .py connector to get the credentials from .cfg file and
        make a connection by passing the schema, db and the credentials

        1. After making a connection, run queries or API calls to fetch raw data.
        (using a parametrized sql_query from sql_queries.py).
        2. Apply custom business logic transformations.
        3. Save transformed data to CSV/Parquet for DB upload.
        """

        # 1. Initiate the Postgres connector
        self.pg_connector = PostgresConnector(credential_name='postgresql: development')

        # 2. Format the query
        query = sql_queries['get_data'].format(
                            sdt=self.sfc.etl_audit_manager.sdt.strftime('%Y-%m-%d %H:%M:%S'),
                            edt=self.sfc.etl_audit_manager.edt.strftime('%Y-%m-%d %H:%M:%S'))

        lg.info(f"The query: {query}")

        # 3. Run the query and assign it to a dataframe
        df = self.pg_connector.run_query(query=query, commit=False, get_result=True)

        '''# The URL
        url = "https://api.gleif.org/api/v1/lei-records/529900W18LQJJN6SJ336"
        lg.info(f"The URL is: {url}")

        # The payload
        payload = {}
        lg.info(f"The payload: {payload}")

        # The headers
        headers = {'Accept': 'application/vnd.api+json'}
        lg.info(f"The headers: {headers}")

        # 1. Call the API, convert to json
        response = requests.request("GET", url, headers=headers, data=payload)
        data_json = response.json()

        # 2. Extract keys from the 'data' object
        # In the JSON, 'data' is a dictionary, not a list
        data_content = data_json.get('data', {})
        data_keys = list(data_content.keys())
        lg.info(f"Keys found in 'data': {data_keys}")

        # 3. Convert to DataFrame
        df = pd.DataFrame(data_content)
        lg.info(f"The df: {df}")'''


        if len(df):

            # Take the etl_runs_key from the audit table and pass it to the dataframe
            df['etl_runs_key'] = self.sfc.etl_audit_manager.etl_runs_key

            # process df and transform
            df = self.sfc.etl_utils.transform_dataframe(
                    df=df,
                    columns_int_list=['raw_number_value', 'transaction_index'],
                    columns_numeric_list=[],
                    # Pass source columns in lowercase
                    columns_str_dict={'value_eth'           : 'value_ethereum',
                                      'tx_hash'             : 'tax_hash',
                                      'blck_nbr_raw_val'    : 'raw_number_value',
                                      'eth_amt_001'         : 'ethereum_amount',
                                      'contract_addr_x'     : 'contract_address',
                                      'f_is_vld_bool'       : 'is_valid'},
                    validate_no_nulls_string=[],
                    columns_replace_backslash_list=[],
                    columns_escape_backslash_list=[],
                    columns_strip_list=[],
                    columns_non_null_list=[],
                    columns_unique_list=[],
                    columns_json_list=['metadata'],
                    columns_lowercase=True
                    )


            # this will be passed to update_etl_runs_table_record
            self.num_of_records = len(df)

            # self.sfc.etl_utils.process_dataframe_date_ranges()
            # if there is a time column in the df, calculate min and max for that specific extract interval
            # df[time_column] = pd.to_datetime(df[time_column])
            # self.data_min_date = df[time_column].min()
            # self.data_max_date = df[time_column].max()

            # if there are no time columns, we can set them manually
            # so these must be datetime objects
            self.sfc.etl_audit_manager.data_min_date = self.sfc.etl_audit_manager.sdt
            self.sfc.etl_audit_manager.data_max_date = self.sfc.etl_audit_manager.edt

            # 1. Set worker values
            # self.data_min_date = self.sfc.etl_audit_manager.sdt
            # self.data_max_date = self.sfc.etl_audit_manager.edt

            # 2. Sync them into the audit manager
            # self.sfc.etl_audit_manager.data_min_date = self.data_min_date
            # self.sfc.etl_audit_manager.data_max_date = self.data_max_date

            # self.data_min_date = datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            # self.data_max_date = datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")

            # 4. Write to CSV
            df.to_csv(
                    path_or_buf=file_path,
                    sep=";",
                    encoding="utf-8",
                    index=False,
                    escapechar="\\",
                    doublequote=False,
                    quoting=csv.QUOTE_NONE,
                    header=True
            )

        else:
            # this will be passed to update_etl_runs_table_record
            self.num_of_records = 0
            self.data_min_date = None
            self.data_max_date = None

            # No records are returned, then leave the min and max dates at the start date
            # ready for the next run to start at the same point
            self.sfc.etl_audit_manager.data_min_date = self.sfc.etl_audit_manager.sdt
            self.sfc.etl_audit_manager.data_max_date = self.sfc.etl_audit_manager.sdt



    # 3. Upload to DWH
    def upload_to_dwh(self,
                      database_connector,
                      etl_audit_manager,
                      delete_output,
                      file_path,
                      schema,
                      table,
                      on_clause,
                      update_clause,
                      insert_columns,
                      insert_values) -> None:
        """
        1. Try to the upload_to_pg function from the postgresql_connector Class
        2. If the upload is ok, set the status of the run to 'Complete'
        3. If the upload isn't ok, set the status of the run to 'Error'
        4. Then, run the update_etl_runs_table_record function to update the audit.etl_runs record
            to status='Error'


        :param database_connector: An instance of self.pg_connector = PostgresConnector(section=self.database)
        :param etl_audit_manager: An instance of self.etl_audit_manager = EtlAuditManager(self, self.script_worker, self.database)
        :param file_path: A parameter of upload_to_pg from PostgresConnector
        :param schema: A parameter of upload_to_pg from PostgresConnector
        :param table: A parameter of upload_to_pg from PostgresConnector
        :param on_clause: A parameter of upload_to_pg from PostgresConnector
        :param update_clause: A parameter of upload_to_pg from PostgresConnector
        :param insert_columns: A parameter of upload_to_pg from PostgresConnector
        :param insert_values: A parameter of upload_to_pg from PostgresConnector
        :param delete_output: A boolean specifying whether we should delete the file after each run.
        :return: None
        """

        # 1. Check if file exists (Fixes the "missing file" issue)
        if not os.path.exists(file_path):
            lg.info(f"No data to upload for {table}. Skipping step.")
            self.status = 'Complete'
            return

        try:
            # 1. Try to the upload_to_pg function from the postgresql_connector Class
            database_connector.upload_to_pg(file_path=file_path,
                                            schema=schema,
                                            table=table,
                                            on_clause=on_clause,
                                            update_clause=update_clause,
                                            insert_columns=insert_columns,
                                            insert_values=insert_values)

            # 2. If the upload is ok, set the status of the run to 'Complete'
            self.status = 'Complete'
        except Exception as e:
            # 3. If the upload isn't ok, set the status of the run to 'Error'
            self.status = 'Error'
            lg.error(f"Upload did not go through. Error: {e}.")

            # 4. Try to run the update_etl_runs_table_record function
            try:
                etl_audit_manager.update_etl_runs_table_record(status=self.status)
            except Exception as ex:
                lg.error(f"Update did not go through. Error: {ex}")
            raise e

        # this will run (deletion still happens on both success/failure or try/except above)
        finally:
            if delete_output and os.path.exists(file_path):
                try:
                    os.remove(path=file_path)
                    lg.info(f"Deleted temporary file: {file_path}")
                except Exception as ex:
                    lg.error(f"Could not delete file {file_path}: {ex}")