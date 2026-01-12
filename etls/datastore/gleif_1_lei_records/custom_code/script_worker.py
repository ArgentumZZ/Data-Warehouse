# Import libraries
import configparser, csv, os
import pandas as pd
from typing import Any, Dict, Optional
from pathlib import Path
import requests
from datetime import datetime

# Import custom libraries
import utilities.logging_manager as lg

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

        # The URL
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
        lg.info(f"The df: {df}")


        if len(df):
            # process df and transform
            self.num_of_records = len(df)


            # if there is a time column in the df, calculate min and max for that specific extract interval
            # df[time_column] = pd.to_datetime(df[time_column])
            # self.data_min_date = df[time_column].min()
            # self.data_max_date = df[time_column].max()

            # if there are no time columns, we can set them manually
            # so these must be datetime objects
            self.data_min_date = self.sfc.etl_audit_manager.sdt
            self.data_max_date = self.sfc.etl_audit_manager.edt
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
                      file_path,
                      schema,
                      table,
                      on_clause,
                      update_clause,
                      insert_columns,
                      insert_values):
        try:
            database_connector.upload_to_pg(file_path=file_path,
                                            schema=schema,
                                            table=table,
                                            on_clause=on_clause,
                                            update_clause=update_clause,
                                            insert_columns=insert_columns,
                                            insert_values=insert_values)
            self.status = 'Complete'
        except Exception as e:
            self.status = 'Error'
            lg.error(f"Upload did not go through. Error: {e}.")
            # raise Exception(f"Upload did not go through. Error: {e}.")

            try:
                etl_audit_manager.update_etl_runs_table_record(status=self.status)
            except Exception as e:
                lg.error(f"Update did not go through. Error: {e}")
                raise Exception(f"Update did not go through. Error: {e}")