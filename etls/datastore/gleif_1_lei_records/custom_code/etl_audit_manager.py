# import libraries
import os, datetime, csv
import pandas as pd
from typing import List, Tuple, Optional, Any, Union
from datetime import timezone, timedelta

# Custom libraries
import utilities.logging_manager as lg
from script_connectors.postgresql_connector import PostgresConnector

class EtlAuditManager:
    """
    I. Manages auditing for ETL processes by tracking execution metadata.
    II. Create a table: @staticmethod: create_audit_etl_runs_table()
    III. Insert new etl runs records: instance method: insert_audit_etl_runs_record(self, ...)
    IV. Update etl runs records: instance method: update_etl_runs_table_record(self, ...)
    """

    def __init__(self,
                 sfc: Any,
                 swc: Any,
                 credentials: Any,
                 schema: str):
        """
        Initializes the audit manager with script factory and database details.

        Args:
            sfp: Pointer to the script factory class.
            swc: Pointer to the script worker object.
            credentials: DB credentials for the audit table.
            schema: Database schema where the audit table resides.
        """
        self.version: str = '1.0'
        self.sfc = sfc
        self.swc = swc
        self.credentials = credentials
        self.audit_schema: str = schema
        self.audit_table_name: str = 'etl_runs'

        # State tracking for the current ETL run
        self.etl_runs_key: Optional[int] = None
        self.sdt: Optional[datetime.datetime] = None  # Start Date Time
        self.edt: Optional[datetime.datetime] = None  # End Date Time
        self.data_min_date: Optional[datetime.datetime] = None
        self.data_max_date: Optional[datetime.datetime] = None
        self.load_type: Optional[str] = None
        self.num_records: Optional[int] = None
        self.prev_max_date: Optional[datetime.datetime] = None
        self.prev_max_version: str = "0.0"

        # Initialize PostgreConnector
        self.pg_connector = PostgresConnector('postgresql: dev')
        lg.logger.info('Etl Audit Manager object is instantiated')

    @staticmethod
    def create_audit_etl_runs_table() -> str:
        """
        CREATE TABLE statement for the audit table.

        Returns:
            str: String formated SQL query.
        """
        return f"""
                CREATE TABLE IF NOT EXISTS audit.etl_runs (
                    etl_runs_key                    BIGSERIAL PRIMARY KEY,
                    load_type                       TEXT,
                    sources                         TEXT,
                    target_database                 TEXT,
                    target_table                    TEXT,
                    start_load_date                 TIMESTAMPTZ,
                    end_load_date                   TIMESTAMPTZ,
                    data_min_date                   TIMESTAMPTZ,
                    data_max_date                   TIMESTAMPTZ,
                    script_execution_start_time     TIMESTAMPTZ,
                    script_execution_end_time       TIMESTAMPTZ,
                    environment                     TEXT,
                    status                          TEXT DEFAULT NULL,
                    script_version                  TEXT DEFAULT NULL,
                    num_records                     BIGINT DEFAULT NULL,
                    prev_max_date                   TIMESTAMPTZ DEFAULT NULL,
                    created_at                      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    modified_at                     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
                """

    def _calculate_etl_window(self,
                              prev_max_date_query: str,
                              max_days_to_load: int,
                              increment_sdt: bool,
                              load_type: str,
                              forced_sdt: str):
        """
        Helper function used to fetch previous run history and calculate
        the sdt (start date time=start_load_date) and edt (end date time=end_load_date) for the current run.
        """

        # 1. Get the latest successfully loaded data max date
        result_data_max_date = self.pg_connector.run_query(query=prev_max_date_query, commit=False, get_result=True)
        lg.info(f"Result data max date: {result_data_max_date}")

        # 2. Check if the query actually returned a date
        if result_data_max_date and result_data_max_date[0][0]:

            # 3. Assign it to a variable
            self.prev_max_date = result_data_max_date[0][0]
            lg.info(f"The Previous max date: {self.prev_max_date}")
            # If the query also returns a version string, store it; otherwise default to "0.0"
            # self.prev_max_version = str(prev_rslt[0][1] if len(prev_rslt[0]) > 1 else "0.0")
        else:
            # 4. If no previous records exist (first time running), set to None
            self.prev_max_date = None
            lg.info(f"The Previous max date: {self.prev_max_date}")
            # self.prev_max_version = "0.0"

        # 5. Generate a current reference timestamp
        now_utc = datetime.datetime.now(timezone.utc)
        lg.info(f"The NOW UTC: {now_utc}")

        lg.info(f"Running in {load_type} mode.")
        # 6. Determine sdt (start date time) for F and I modes
        if load_type == 'F':
            # if we pass a date in .bat file (=forced_sdt)
            if forced_sdt:
                forced_sdt = datetime.datetime.strptime(forced_sdt, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                lg.info(f"Forced sdt (F mode): {forced_sdt}")

                self.sdt = forced_sdt # this should become the new start date
                lg.info(f"The sdt (F mode): {self.sdt}")
            elif self.prev_max_date:  # if not start from the previous max_date
                self.sdt = self.prev_max_date
                lg.info(f"The sdt (F mode): {self.sdt}")
            else:
                # elif self.prev_max_date is None:
                raise ValueError("F mode requires either a forced_sdt or a previous max date.")

            # Determine edt (F mode loads exactly X days)
            self.edt = self.sdt + timedelta(days=max_days_to_load)
            lg.info(f"The edt (F mode): {self.edt}")

        elif load_type == 'I' and self.prev_max_date:
            # Incremental load: Start from where we left off last time.
            # If increment_sdt is True, we add 1 day (standard if data is daily-partitioned, e.g. for ftp projects).
            self.sdt = self.prev_max_date + timedelta(days=1) if increment_sdt else self.prev_max_date
            lg.info(f"The sdt (I mode): {self.sdt}")

            # 6D. Determine EDT (incremental cannot load future data)
            max_window_end = self.sdt + timedelta(days=max_days_to_load)
            self.edt = min(now_utc, max_window_end)
            lg.info(f"The edt (I mode): {self.edt}")
        return self.sdt, self.edt

    def insert_audit_etl_runs_record(
            self,
            load_type: str,
            sources: List[str],
            target_database: str,
            target_table: str,
            prev_max_date_query: str,
            script_version: str,
            forced_sdt,
            max_days_to_load: int = 365,
            increment_sdt: bool = False
    ) -> None:
        """Insert a new record in the audit table."""

        # 1. If running for a first time, create the schema and the table
        self.pg_connector.create_schema(schema='audit')
        self.pg_connector.run_query(query=EtlAuditManager.create_audit_etl_runs_table())

        #
        self.load_type = load_type.upper()
        sources_string = ", ".join(sources)

        # Use the single consolidated helper
        self.sdt, self.edt = self._calculate_etl_window(prev_max_date_query=prev_max_date_query,
                                                        max_days_to_load=max_days_to_load,
                                                        increment_sdt=increment_sdt,
                                                        load_type=self.load_type,
                                                        forced_sdt=forced_sdt)

        # Insert record
        insert_query = f"""
            INSERT INTO audit.etl_runs (
                load_type, 
                sources, 
                target_database, 
                target_table, 
                start_load_date, 
                end_load_date, 
                script_execution_start_time, 
                environment, 
                status, 
                script_version
            )
            VALUES (
                '{self.load_type}', 
                '{sources_string}', 
                '{target_database}', 
                '{target_table}', 
                '{self.sdt.strftime("%Y-%m-%d %H:%M:%S")}', 
                '{self.edt.strftime("%Y-%m-%d %H:%M:%S")}', 
                CURRENT_TIMESTAMP, 
                '{os.environ.get('MACHINE_SCRIPT_RUNNER_ENV')}', 
                'In Progress', 
                '{script_version}'
            )
            RETURNING etl_runs_key;
        """

        self.etl_runs_key = self.pg_connector.run_query(query=insert_query, commit=True, get_result=True)[0][0]

    def update_etl_runs_table_record(self, status: str):
        """
        Fetches the count from the worker object at the moment
        this method is called, then updates the DB.
        """
        # 1. Pull the count from the worker dynamically
        # This ensures we get '17' (after the run) instead of '0' (before the run)
        self.num_records = getattr(self.swc, 'num_of_records', 0)

        lg.logger.info(f"Final count pulled from worker: {self.num_records}")

        # 2. Format dates for SQL
        prev_date_val = f"'{self.prev_max_date.strftime('%Y-%m-%d %H:%M:%S')}'" if self.prev_max_date else "NULL"

        # 3. Update Query
        query = f"""
            UPDATE audit.etl_runs SET  
                data_min_date = '{self.data_min_date.strftime("%Y-%m-%d %H:%M:%S")}',
                data_max_date = '{self.data_max_date.strftime("%Y-%m-%d %H:%M:%S")}',
                script_execution_end_time = CURRENT_TIMESTAMP,
                status = '{status}',
                num_records = {self.num_records},
                prev_max_date = {prev_date_val},
                modified_at = CURRENT_TIMESTAMP
            WHERE etl_runs_key = {self.etl_runs_key}
        """
        self.pg_connector.run_query(query, commit=True)