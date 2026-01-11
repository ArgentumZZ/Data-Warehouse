# import libraries
import script_factory.settings as settings
from functools import partial
import os
import psycopg2

# import custom libraries
from custom_code.etl_utils import EtlUtils
from custom_code.script_worker import ScriptWorker
from custom_code.etl_audit_manager import EtlAuditManager
from custom_code.sql_queries import sql_queries
from utilities.email_manager import EmailManager
from utilities.file_utils import create_folders, generate_random_dir
import utilities.logging_manager as lg
from script_connectors.postgresql_connector import PostgresConnector


class ScriptFactory:
    """
    - Load/Read settings
    - Determine environment
    - Build a list of tasks (functions)
    - Instantiate ScriptWorker, EtlUtils, EtlAuditManager
    - Expose settings to them
    - Upload CSV to PostgresSQL directly (no DB handler)
    """

    def __init__(self):
        lg.logger.info("Initializing ScriptFactory")

        # 1. General script information
        self.info = {
            'script_name'           : settings.script_name,
            'script_version'        : settings.script_version,
            'script_description'    : settings.script_description,
            'script_frequency'      : settings.script_frequency,
            'email_recipients'      : settings.email_recipients
        }

        # 2. Load settings and determine environment
        self.settings = settings
        self.environment = settings.environment

        # 3. Load environment specific parameters
        if self.environment == 'production':
            self.database = settings.prod_database
            self.schema = settings.prod_schema
            self.table = settings.prod_table
            self.file_name = settings.prod_file_name
            delete_log = settings.delete_log
            delete_mail_logfile = settings.delete_mail_logfile
            delete_output = settings.delete_output
            send_mail_report = settings.send_mail_report
            send_mail_log_report = settings.send_mail_log_report
        else:
            self.database = settings.dev_database
            self.schema = settings.dev_schema
            self.table = settings.dev_table
            self.file_name = settings.dev_file_name
            delete_log = settings.delete_log
            delete_mail_logfile = settings.delete_mail_logfile
            delete_output = settings.delete_output
            send_mail_report = settings.send_mail_report
            send_mail_log_report = settings.send_mail_log_report

        # 4. Initialize components
        self.etl_utils = EtlUtils(self)
        self.script_worker = ScriptWorker(self)
        self.etl_audit_manager = EtlAuditManager(self, self.script_worker, self.database, "audit")
        self.email = EmailManager(self)

        # Create an instance of the connector
        self.pg_connector = PostgresConnector(section=self.database)

        # 5. Create an output folder - Added index [0] to ensure we get the string path
        result = create_folders(
            [settings.output_folder_base, generate_random_dir()],
            isfolder=True)

        # If it returns a tuple, take the first element; otherwise take the result
        self.output_dir = result[0] if isinstance(result, tuple) else result

        # 6. Build CSV path inside that folder
        self.file_path = os.path.join(self.output_dir, self.file_name)

        lg.logger.info(f"CSV will be saved to: {self.file_path}")

    # ----------------------------------------------------------------------
    # NEW: Direct PostgresSQL uploader (no DB handler, no factory)
    # ----------------------------------------------------------------------

    def init_db_data(self):
        # 1. Create schema
        self.pg_connector.create_schema(schema=self.schema)

        # 2. Create table (parametrize the query from sql_queries.py)
        self.pg_connector.create_table(query=sql_queries['create_table'].format(schema=self.schema,
                                                                                table=self.table))


    # ----------------------------------------------------------------------
    # Task list
    # ----------------------------------------------------------------------
    def init_tasks(self):
        """
        Initialize a list of parametrized tasks.
        Returns the ordered list of ETL tasks to be executed.
        """

        # Load type
        self.load_type = settings.load_type

        # Maximum number of days to load
        self.max_days_to_load = settings.max_days_to_load

        task_1 = {
            "func"          : partial(self.script_worker.get_data),
            "task_name"     : "get_data",
            "description"   : "Extract data from the source.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        task_2 = {
            "func"          : partial(self.init_db_data),
            "task_name"     : "init_db_data",
            "description"   : "Initialize database data.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        task_3 = {
            "func"          : partial(self.pg_connector.upload_to_pg,
                                file_path=self.file_path,
                                schema=self.schema,
                                table=self.table,
                                on_clause=sql_queries['on_clause'],
                                update_clause=sql_queries['update_clause'],
                                insert_columns=sql_queries['insert_columns'],
                                insert_values=sql_queries['insert_values']
                            ),
            "task_name"     : "upload_to_pg",
            "description"   : "Upload data to Postgres DB.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        return [
            # self.etl_audit_manager.start_audit,
            # self.script_worker.get_credentials,
            task_1,
            # self.script_worker.get_data,
            # self.etl_utils.transform_dataframe,

            # NEW: Upload CSV directly to Postgres,
            task_2,
            task_3
            # self.upload_csv_to_postgres,

            # self.etl_audit_manager.finish_audit,

            # Email tasks
            # self.email.prepare_mails,
            # self.email.send_mails
        ]
