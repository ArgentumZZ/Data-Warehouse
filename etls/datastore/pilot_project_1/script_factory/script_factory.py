import script_factory.settings as cfg
import common.logging as lg
from custom_code.etl_utils import EtlUtils
from custome_code.script_worker import ScriptWorker
from custome_code.etl_audit_manager import EtlAuditManager
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
        """

        # Create DB handler (PostgreSQL in this example)
        self.db = DatabaseHandlerFactory("postgresql")

        # Set credentials (database + schema)
        self.db.set_credentials(
            database=self.database,
            schema=self.schema
        )

        # Log which DB is being used
        lg.logger.info(f"Using database: {self.database} (schema: {self.schema})")

        # Set target table
        self.db.set_table(self.table_name, self.schema)

        # Add CREATE TABLE query
        self.db.add_create_table_query(
            sql_queries["create_table"].format(
                schema=self.schema,
                table=self.table_name
            )
        )

        # Create table if missing
        self.db.check_and_create_table()

        # Create log table (ETL_RUNS)
        self.etl_utils.create_log_table(
            dbase_object=self.db,
            schema=self.schema,
            table=self.table_name,
            source_cols_unique=sql_queries["source_cols_unique"]
        )

        # Assign file to upload (ScriptWorker will set this later)
        # self.db.set_file_to_upload(self.output_csv_path)

    def get_tasks(
            self): """ Returns the ordered list of ETL tasks to be executed. Each task is a callable (function or bound method). """

    return [self.audit.start_audit,
            self.worker.get_credentials,
            self.worker.make_connection,
            self.worker.get_data,
            self.etl_utils.transform_dataframe,
            self.worker.upload_to_db,
            self.audit.finish_audit

            # Email tasks
            # self.email.prepare_mails,
            # self.email.send_mails
            ]