import script_factory.settings as cfg
import common.logging as lg
import os
from custom_code.etl_utils import EtlUtils
from custom_code.script_worker import ScriptWorker
from custom_code.etl_audit_manager import EtlAuditManager
from custom_code.sql_queries import sql_queries
from script_connectors.email_manager import EmailManager

class ScriptFactory:
    """
    - Load/Read settings
    - Determine environment
    - Build a list of tasks (functions)
    - Instantiate ScriptWorker, EtlUtils, EtlAuditManager
    - Expose settings to them
    - Return tasks toto script_runner

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

        # 3. Build CSV path inside ScriptFactory output folder
        self.csv_path = os.path.join(self.sfc.output_dir, "api_extract.csv")

        # 4. Create output folder
        self.output_dir = create_folders(
            [cfg.output_folder_base, generate_random_dir()],
            isfolder=True )

        # 5. Initialize components
        self.etl_utils = EtlUtils(self)
        self.worker = ScriptWorker(self)
        self.audit = EtlAuditManager(self, self.worker, self.database, "audit")
        self.email = EmailManager(self)

    def init_db(self):
        """
        Initialize database connections and ensure required tables exist.
        This prepares the PostgreSQL environment BEFORE the worker runs.
        """

        lg.logger.info("Initializing database objects")

        # 1. Create DB handler (PostgreSQL)
        self.db = DatabaseHandlerFactory("postgresql")

        # 2. Set credentials (database + schema)
        self.db.set_credentials(
            database=self.database,
            schema=self.schema
        )

        lg.logger.info(
            f"Using database '{self.database}' with schema '{self.schema}'"
        )

        # 3. Set target table
        self.db.set_table(
            table=self.table,
            schema=self.schema
        )

        # 4. Register CREATE TABLE query
        self.db.add_create_table_query(
            sql_queries["create_table"].format(
                schema=self.schema,
                table=self.table
            )
        )

        # 5. Create table if missing
        self.db.check_and_create_table()

        # 6. Create ETL log table (ETL_RUNS)
        self.etl_utils.create_log_table(
            dbase_object=self.db,
            schema=self.schema,
            table=self.table,
            source_cols_unique=sql_queries["source_cols_unique"]
        )

        # 7. Worker will set the CSV file later (after extraction)
        #    so we do NOT set file_to_upload here.
        lg.logger.info("Database initialization complete. Waiting for worker to set CSV file.")

    def get_tasks(self):
        """ Returns the ordered list of ETL tasks to be executed.
        Each task is a callable (function or bound method).
        """

        return [self.audit.start_audit,
                self.worker.get_credentials,
                self.worker.make_connection,
                self.worker.get_data,
                self.etl_utils.transform_dataframe,
                self.worker.upload_to_db,
                self.audit.finish_audit,

                # Email tasks
                self.email.prepare_mails,
                self.email.send_mails
                ]