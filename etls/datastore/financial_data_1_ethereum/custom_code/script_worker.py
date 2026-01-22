# Import libraries
import csv, os
from typing import Any, Dict, Optional

# Import custom libraries
import utilities.logging_manager as lg
from utilities.etl_utils import EtlUtils
from connectors.postgresql_connector import PostgresConnector
from custom_code.sql_queries import sql_queries, source_columns_unique


class ScriptWorker:
    """
        General-purpose ETL worker skeleton.
        Handles:
        - credentials and connection
        - extraction
        - transformation
        - saving and uploading
    """

    def __init__(self, sfc: 'ScriptFactory'):

        self.sfc = sfc              # pointer to the script factory class
        self.num_of_records = None
        self.status = None
        self.data_min_date = None
        self.data_max_date = None

        lg.info("Initializing ScriptWorker")

    # 1. Extract, transform and load
    def get_data(self, file_path: str) -> None:
        """
        1. Create a DB/API connection using the .cfg credentials to make a connection.
        2. Run SQL queries or API calls to fetch raw data.
        2. Apply custom business logic transformations.
        3. Save transformed data to CSV/Parquet for DB upload.
        """

        # 1. Initialize the Postgres connector
        pg_connector = PostgresConnector(credential_name='postgresql: development')

        # 2. Format the query
        query = sql_queries['get_data'].format(
                            sdt=self.sfc.etl_audit_manager.sdt.strftime('%Y-%m-%d %H:%M:%S.%f%z'),
                            edt=self.sfc.etl_audit_manager.edt.strftime('%Y-%m-%d %H:%M:%S.%f%z'))

        lg.info(f"The query: {query}")

        # 3. Run the query and assign the result to a dataframe
        df = pg_connector.run_query(query=query, commit=False, get_result=True)

        # 4. If there is data, apply transformations
        if len(df) > 0:

            # 4.1. Take the etl_runs_key from the audit table and add it to the dataframe
            df['etl_runs_key'] = self.sfc.etl_audit_manager.etl_runs_key

            # 4.2. Process and transform the DataFrame
            df = self.sfc.etl_utils.transform_dataframe(
                                    df=df,
                                    # Pass source columns in lowercase
                                    columns_str_dict={'tx_hash': 'tax_hash',
                                                      'blck_nbr_raw_val': 'raw_number_value',
                                                      'eth_amt_001': 'ethereum_amount',
                                                      'contract_addr_x': 'contract_address',
                                                      'f_is_vld_bool': 'is_valid'},
                                    columns_lowercase=True,
                                    columns_strip_list=[],
                                    columns_replace_backslash_list=[],
                                    columns_escape_backslash_list=[],
                                    columns_sanitize_list=[],
                                    columns_date_config_dict={},
                                    columns_int_list=['raw_number_value', 'transaction_index'],
                                    columns_numeric_list=[],
                                    columns_json_list=['metadata'],
                                    columns_non_null_list=[],
                                    columns_unique_list=[]
                                    )

            # 4.3. Process the date ranges
            self.data_min_date, self.data_max_date = self.sfc.etl_utils.process_dataframe_date_ranges(
                df=df,
                date_columns=['source_created_at', 'source_updated_at'])

            lg.info(f"The Script Worker data_min_date: {self.data_min_date}")
            lg.info(f"The Script Worker data_max_date: {self.data_max_date}")

            # 4.4. If there are no time columns in the source, we can set data_min/max_dates manually to sdt and edt
            # self.sfc.etl_audit_manager.data_min_date = self.sfc.etl_audit_manager.sdt
            # self.sfc.etl_audit_manager.data_max_date = self.sfc.etl_audit_manager.edt

            # 4.5. This will be passed to update_etl_runs_table_record
            self.num_of_records = len(df)
            lg.info(f"The number of records: {self.num_of_records}")

            # 4.6. Write to CSV
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
            # 4.7. No records are returned, then leave the min and max dates at the start date
            self.num_of_records = 0
            self.data_min_date = None
            self.data_max_date = None

            # 4.9. The next run will start from the same point
            self.sfc.etl_audit_manager.data_min_date = self.sfc.etl_audit_manager.sdt
            self.sfc.etl_audit_manager.data_max_date = self.sfc.etl_audit_manager.sdt

    # 5. Upload to DWH
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
        1. Try to load data using the upload_to_pg function from the postgresql_connector Class
        2. If the upload is ok, set the status of the run to 'Complete'
        3. If the upload isn't ok, set the status of the run to 'Error'
        4. Then, run the update_etl_runs_table_record function to update the audit.etl_runs record
            to status='Error'


        database_connector: An instance of self.pg_connector = PostgresConnector(section=self.database)
        etl_audit_manager: An instance of self.etl_audit_manager = EtlAuditManager(self, self.script_worker, self.database)
        file_path: A parameter of upload_to_pg from PostgresConnector
        schema: A parameter of upload_to_pg from PostgresConnector
        table: A parameter of upload_to_pg from PostgresConnector
        on_clause: A parameter of upload_to_pg from PostgresConnector
        update_clause: A parameter of upload_to_pg from PostgresConnector
        insert_columns: A parameter of upload_to_pg from PostgresConnector
        insert_values: A parameter of upload_to_pg from PostgresConnector
        delete_output: A boolean specifying whether we should delete the file after each run.
        """

        # 1. Check if the file exists
        if not os.path.exists(file_path):
            lg.info(f"No data to upload for {table}. Skipping this step.")
            self.status = 'Complete'
            return

        try:
            # 2. Try to load data using the upload_to_pg function from the postgresql_connector Class
            database_connector.upload_to_pg(file_path=file_path,
                                            schema=schema,
                                            table=table,
                                            on_clause=on_clause,
                                            update_clause=update_clause,
                                            insert_columns=insert_columns,
                                            insert_values=insert_values)

            # 3. If the upload is ok, set the status of the run to 'Complete'
            self.status = 'Complete'
        except Exception as e:
            # 4. If the upload isn't ok, set the status of the run to 'Error'
            self.status = 'Error'
            lg.error(f"Upload did not go through. Error: {e}.")

            # 5. Try to run the update_etl_runs_table_record function
            try:
                etl_audit_manager.update_etl_runs_table_record(status=self.status)
            except Exception as ex:
                lg.error(f"Update did not go through. Error: {ex}")
            raise e

        # 6. This will run (deletion still happens on both success/failure of try/except above)
        finally:
            if delete_output and os.path.exists(file_path):
                try:
                    os.remove(path=file_path)
                    lg.info(f"Deleted temporary file: {file_path}")
                except Exception as ex:
                    lg.error(f"Could not delete file {file_path}: {ex}")