# Import libraries
import configparser, csv, os
import pandas as pd
from typing import Any, Dict, Optional
from pathlib import Path
import utilities.logging_manager as lg

# Import custom libraries

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

        self.sfc = sfc   # pointer to the script factory class (to be implemented)
        self.credentials: Optional[Dict[str, str]] = None
        self.connection = None
        self.data = None
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
    # 2. Connection
    # ---------------------------
    def make_connection(self):
        """
        Create a DB/API connection using loaded credentials.
        Use the specific .py connector to get the credentials from .cfg file and
        make a connection by passing the schema, db and the credentials
        """
        import requests

        url = "https://api.gleif.org/api/v1/lei-records/529900W18LQJJN6SJ336"
        lg.info(f"The URL is: {url}")

        payload = {}
        headers = {'Accept': 'application/vnd.api+json'}
        lg.info(f"The headers: {headers}")

        # 1. Call API
        response = requests.request("GET", url, headers=headers, data=payload)
        data_json = response.json()

        # 2. Extract keys from the 'data' object
        # In your JSON, 'data' is a dictionary, not a list
        data_content = data_json.get('data', {})
        data_keys = list(data_content.keys())

        lg.info(f"Keys found in 'data': {data_keys}")

        # 3. Convert to DataFrame
        df = pd.DataFrame(data_content)
        lg.info(f"The df: {df}")

        # 4. Write to CSV
        df.to_csv(
            self.sfc.csv_path,
            sep=";",
            encoding="utf-8",
            index=False,
            escapechar="\\",
            doublequote=False,
            quoting=csv.QUOTE_NONE,
            header=True
        )

    # ---------------------------
    # 3. Extract, Transform, Save
    # ---------------------------
    def get_data(self):
        """
        1. After making a connection, get the data from the source by
        using a parametrized sql_query from sql_queries.py
        2. Make custom transformations
        3. Save the file to csv/parquet to DB upload
        :return:
        """

        """Run queries or API calls to fetch raw data."""
        """Apply business logic transformations."""
        """Save transformed data to CSV/Parquet."""
        pass

    # ---------------------------
    # 4. Upload
    # ---------------------------
    def upload_to_db(self):
        """Upload saved file to target database."""
        pass