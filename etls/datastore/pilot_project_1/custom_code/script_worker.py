# Import libraries
import configparser
from typing import Any, Dict, Optional
from pathlib import Path
import etls.datastore.pilot_project_1.script_connectors.logging_manager as lg


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
        pass

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