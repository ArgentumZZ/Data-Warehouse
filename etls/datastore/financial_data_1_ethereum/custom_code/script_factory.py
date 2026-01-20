# import libraries
# import script_factory.settings as settings
from functools import partial
import os, sys, psycopg2, datetime
from datetime import datetime

# import custom libraries
from utilities.etl_utils import EtlUtils
from utilities.etl_audit_manager import EtlAuditManager
from utilities.email_manager import EmailManager
from utilities.file_utils import build_output_file_path
import utilities.logging_manager as lg
from utilities.argument_parser import parse_arguments

from custom_code.script_worker import ScriptWorker
from custom_code.sql_queries import sql_queries
from connectors.postgresql_connector import PostgresConnector


class ScriptFactory:
    """
    - Load/Read settings
    - Determine environment
    - Build a list of tasks (functions)
    - Instantiate ScriptWorker, EtlUtils, EtlAuditManager
    - Expose settings to them
    - Upload CSV to PostgresSQL directly (no DB handler)
    """

    def __init__(self,
                 forced_sdt: str,
                 load_type: str,
                 max_days_to_load: int,
                 settings):
        lg.info("Initializing ScriptFactory")

        # 1. General script information
        self.info = {
            'script_name'            : settings.script_name,
            'script_version'         : settings.script_version,
            'script_description'     : settings.script_description,
            'run_environment'        : settings.environment,
            'reference_page'         : "not implemented yet",
            'script_frequency'       : settings.script_frequency,
            'script_primary_owner'   : settings.script_primary_owner,
            'script_secondary_owner' : settings.script_secondary_owner
        }

        # forced_sdt, load_type, max_days_to_load = parse_arguments(sys.argv, settings)
        self.forced_sdt = forced_sdt
        self.load_type = load_type
        self.max_days_to_load = max_days_to_load

        # 2. Load settings and determine environment
        self.settings = settings
        self.environment = settings.environment

        # 3. Load environment specific parameters
        if self.environment == 'production':
            self.database = settings.prod_database
            self.schema = settings.prod_schema
            self.table = settings.prod_table

            self.delete_log = settings.prod_delete_log
            self.delete_output = settings.prod_delete_output
            self.send_mail_etl_summary_report = settings.prod_send_mail_etl_summary_report

            self.list_recipients_admin = settings.prod_list_recipients_admin
            self.list_recipients_business = settings.prod_list_recipients_business
            self.list_recipients_error = settings.prod_list_recipients_error

            self.is_admin_email_enabled = settings.prod_is_admin_email_alert_enabled
            self.is_business_email_enabled = settings.prod_is_business_email_alert_enabled
            self.is_error_email_enabled = settings.prod_is_error_email_alert_enabled
        else:
            self.database = settings.dev_database
            self.schema = settings.dev_schema
            self.table = settings.dev_table

            self.delete_log = settings.dev_delete_log
            self.delete_output = settings.dev_delete_output
            self.send_mail_etl_summary_report = settings.dev_send_mail_etl_summary_report

            self.list_recipients_admin = settings.dev_list_recipients_admin
            self.list_recipients_business = settings.dev_list_recipients_business
            self.list_recipients_error = settings.dev_list_recipients_error

            self.is_admin_email_enabled = settings.dev_is_admin_email_alert_enabled
            self.is_business_email_enabled = settings.dev_is_business_email_alert_enabled
            self.is_error_email_enabled = settings.dev_is_error_email_alert_enabled

        # 4. Initialize components
        self.etl_utils = EtlUtils(self)
        self.script_worker = ScriptWorker(self)
        self.etl_audit_manager = EtlAuditManager(self, self.script_worker, self.database)

        # Create an instance of the connector
        self.pg_connector = PostgresConnector(credential_name=self.database)

        # Create schema and table
        self.pg_connector.init_schema_and_table(query=sql_queries['create_table'],
                                                schema=self.schema,
                                                table=self.table)

        # Build file path for the output folder
        self.file_path = build_output_file_path(table=self.table)
        lg.info(f"The file will be saved to: {self.file_path}")

    # ----------------------------------------------------------------------
    # Task list
    # ----------------------------------------------------------------------
    def init_tasks(self):
        """
        Initialize a list of parametrized tasks.
        Returns the ordered list of ETL tasks to be executed.
        """

        task_1 = {
            "func"          : partial(self.etl_audit_manager.insert_audit_etl_runs_record,
                                      script_version=self.info['script_version'],
                                      load_type=self.load_type,
                                      max_days_to_load=self.max_days_to_load,
                                      sources=self.settings.sources,
                                      target_database='datastore',
                                      target_table=f'{self.schema}.{self.table}',
                                      forced_sdt=self.forced_sdt,
                                      prev_max_date_query=sql_queries['prev_max_date_query'].format(schema=self.schema,
                                                                                                    table=self.table)
                            ),
            "task_name"     : "create_etl_runs_record",
            "description"   : "Create the etl_runs table record for the current execution of the tasks.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        task_2 = {
            "func"          : partial(self.pg_connector.run_query,
                                      query=sql_queries['set_comments'].format(schema=self.schema, table=self.table),
                                      commit=True,
                                      get_result=True
                                      ),
            "task_name"     : "set_comments",
            "description"   : "Set table and column comments for documentation.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        task_3 = {
            "func"          : partial(self.script_worker.get_data,
                                      file_path=self.file_path),
            "task_name"     : "get_data",
            "description"   : "Extract data from the source.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        task_4 = {
            "func"          : partial(self.script_worker.upload_to_dwh,
                                      database_connector=self.pg_connector,
                                      etl_audit_manager=self.etl_audit_manager,
                                      delete_output=self.delete_output,
                                      file_path=self.file_path,
                                      schema=self.schema,
                                      table=self.table,
                                      on_clause=sql_queries['on_clause'],
                                      update_clause=sql_queries['update_clause'],
                                      insert_columns=sql_queries['insert_columns'],
                                      insert_values=sql_queries['insert_values']
                            ),
            "task_name"    : "upload_to_pg",
            "description"  : "Upload data to data warehouse.",
            "enabled"      : True,
            "retries"      : 1,
            "depends_on"   : None
        }

        task_5 = {
            "func"          : partial(self.etl_audit_manager.update_etl_runs_table_record,
                                      status='Complete'
                                      ),
            "task_name"     : "update_etl_runs_table_record",
            "description"   : "Update audit.etl_runs record.",
            "enabled"       : True,
            "retries"       : 1,
            "depends_on"    : None
        }

        return [
                task_1,  # self.etl_audit_manager.create_etl_runs_table_record,
                         # create_trigger,
                task_2,  # create_comments,
                task_3,  # self.script_worker.get_data,
                task_4,  # self.pg_connector.upload_to_pg
                task_5   # self.etl_audit_manager.update_etl_runs_table_record
                ]
