import script_factory.settings as cfg

import os
import psycopg2

from custom_code.etl_utils import EtlUtils
from custom_code.script_worker import ScriptWorker
from custom_code.etl_audit_manager import EtlAuditManager
from custom_code.sql_queries import sql_queries
from utilities.email_manager import EmailManager
from utilities.file_utils import create_folders, generate_random_dir
import utilities.logging_manager as lg


class ScriptFactory:
    """
    - Load/Read settings
    - Determine environment
    - Build a list of tasks (functions)
    - Instantiate ScriptWorker, EtlUtils, EtlAuditManager
    - Expose settings to them
    - Upload CSV to PostgreSQL directly (no DB handler)
    """

    def __init__(self):
        lg.logger.info("Initializing ScriptFactory")

        # 1. Load settings
        self.cfg = cfg

        # 2. Determine environment
        self.environment = cfg.environment

        # 3. Load environment-specific DB/Schema/Table
        if self.environment == 'production':
            self.database = cfg.prod_database
            self.schema = cfg.prod_schema
            self.table = cfg.prod_table
        else:
            self.database = cfg.dev_database
            self.schema = cfg.dev_schema
            self.table = cfg.dev_table

        # 4. Initialize components
        self.etl_utils = EtlUtils(self)
        self.script_worker = ScriptWorker(self)
        self.etl_audit_manager = EtlAuditManager(self, self.script_worker, self.database, "audit")
        self.email = EmailManager(self)

        # 5. Create output folder - Added index [0] to ensure we get the string path
        result = create_folders(
            [cfg.output_folder_base, generate_random_dir()],
            isfolder=True)

        # If it returns a tuple, take the first element; otherwise take the result
        self.output_dir = result[0] if isinstance(result, tuple) else result

        # 6. Build CSV path inside that folder
        self.csv_path = os.path.join(self.output_dir, "api_extract.csv")

        lg.logger.info(f"CSV will be saved to: {self.csv_path}")

    # ----------------------------------------------------------------------
    # NEW: Direct PostgreSQL uploader (no DB handler, no factory)
    # ----------------------------------------------------------------------
    def upload_csv_to_postgres(self):
        """
        Uploads the CSV file produced by ScriptWorker directly into PostgreSQL.
        """
        lg.logger.info("Uploading CSV to PostgreSQL...")

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()

        with open(self.csv_path, "r", encoding="utf-8") as f:
            # We explicitly name the columns here: (lei_id, data_json)
            # This allows 'inserted_at' to be auto-filled by the DB
            cur.copy_expert(
                sql=f"""
                    COPY {self.schema}.{self.table} (lei_id, data_json)
                    FROM STDIN
                    WITH (
                        FORMAT CSV,
                        HEADER,
                        DELIMITER ';',
                        QUOTE '"',
                        ESCAPE '\\'
                    );
                """,
                file=f
            )

        conn.commit()
        cur.close()
        conn.close()

        lg.logger.info("CSV upload to PostgreSQL completed successfully.")

    # ----------------------------------------------------------------------
    # Task list
    # ----------------------------------------------------------------------
    def get_tasks(self):
        """
        Returns the ordered list of ETL tasks to be executed.
        ScriptWorker writes CSV → ScriptFactory uploads CSV.
        """

        return [
            # self.etl_audit_manager.start_audit,
            # self.script_worker.get_credentials,
            self.script_worker.make_connection,
            # self.script_worker.get_data,
            # self.etl_utils.transform_dataframe,

            # NEW: Upload CSV directly to PostgreSQL
            self.upload_csv_to_postgres,

            # self.etl_audit_manager.finish_audit,

            # Email tasks
            # self.email.prepare_mails,
            # self.email.send_mails
        ]
