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

        # Create schema and table
        self.pg_connector.init_schema_and_table(query=sql_queries['create_table'],
                                                schema=self.schema,
                                                table=self.table)

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
            "func"          : partial(self.etl_audit_manager.insert_audit_etl_runs_record,
                                script_version=self.info['script_version'],
                                load_type=settings.load_type,
                                max_days_to_load=settings.max_days_to_load,
                                sources=settings.sources,
                                target_database='datastore',
                                target_table=f'{self.schema}.{self.table}',
                                prev_max_date_query=f"""SELECT data_max_date
                                                            FROM audit.etl_runs
                                                            WHERE etl_runs_key=(SELECT max(etl_runs_key) 
                                                                    FROM audit.etl_runs 
                                                                    WHERE target_table='{self.schema}.{self.table}'
                                                                    AND status='Complete')
                                                       """
                            ),
            "task_name"     : "create_etl_runs_record",
            "description"   : "Creating the etl_runs table record for the current execution of the tasks",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        task_2 = {
            "func"          : partial(self.script_worker.get_data,
                                      file_path=self.file_path),
            "task_name"     : "get_data",
            "description"   : "Extract data from the source.",
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

        task_4 = {
            "func"          : partial(self.etl_audit_manager.update_etl_runs_table_record,
                                status= 'Complete',
                                # num_records = self.script_worker.num_of_records,
                                # file_path = self.file_path,
                                # delimiter = ','

                            ),
            "task_name"     : "upload_to_pg",
            "description"   : "Upload data to Postgres DB.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        return [
            task_1,  # self.etl_audit_manager.create_etl_runs_table_record,
            # create_trigger,
            # create_comments,
            task_2,  # self.script_worker.get_data,
            task_3,  # self.pg_connector.upload_to_pg
            task_4   # self.etl_audit_manager.update_etl_runs_table_record
            # Email tasks
            # self.prepare_mails,
            # self.send_all
        ]
