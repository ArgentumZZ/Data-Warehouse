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
                    sources                         TEXT[],
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

    def _calculate_etl_window(self, prev_max_date_query: str, max_days_to_load: int, increment_sdt: bool):
        """
        Consolidated helper to fetch previous run history and calculate
        the SDT (Start) and EDT (End) for the current run.
        """
        # 1. FETCH PREVIOUS HIGH-WATER MARK
        # We query the audit table or target table to find the latest date we successfully loaded.
        prev_rslt = self.pg_connector.run_query(prev_max_date_query, get_result=True)

        # Check if the query actually returned a date
        if prev_rslt and prev_rslt[0][0]:
            self.prev_max_date = prev_rslt[0][0]
            # If the query also returns a version string, store it; otherwise default to "0.0"
            self.prev_max_version = str(prev_rslt[0][1] if len(prev_rslt[0]) > 1 else "0.0")
        else:
            # If no previous records exist (first time running), set to None
            self.prev_max_date = None
            self.prev_max_version = "0.0"

        # 2. REFERENCE CURRENT TIME
        # We generate "now" in UTC to ensure consistency across different servers/timezones.
        now_utc = datetime.datetime.now(timezone.utc)

        # 3. DETERMINE SDT (Start Date Time)
        # This logic decides where the "bottom" of our data window is.
        if self.load_type == 'I' and self.prev_max_date:
            # INCREMENTAL LOAD: Start exactly where we left off last time.
            # If increment_sdt is True, we add 1 day (standard if data is daily-partitioned).
            self.sdt = self.prev_max_date + timedelta(days=1) if increment_sdt else self.prev_max_date
        else:
            # FULL LOAD (or first run): We don't care about history.
            # We "generate" a start date of today at 00:00:00 (Midnight).
            self.sdt = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)

        # 4. DETERMINE EDT (End Date Time)
        # This logic decides the "top" of our data window.

        # First, calculate the furthest possible date we are allowed to load based on 'max_days_to_load'
        max_window_end = self.sdt + timedelta(days=max_days_to_load)

        # Use min() to pick the earlier of "Right Now" or "Max Allowed Window".
        # This prevents the script from trying to load data from the future.
        self.edt = min(now_utc, max_window_end)

    def insert_audit_etl_runs_record(
            self,
            load_type: str,
            sources: List[str],
            target_database: str,
            target_table: str,
            prev_max_date_query: str,
            script_version: str,
            max_days_to_load: int = 365,
            increment_sdt: bool = False
    ) -> None:
        """Main entry point to start an audit record."""
        # Setup table
        self.pg_connector.create_schema(schema='audit')
        self.pg_connector.run_query(query=EtlAuditManager.create_audit_etl_runs_table())

        self.load_type = load_type.upper()
        sources # _str = '","'.join(sources)

        # Use the single consolidated helper
        self._calculate_etl_window(prev_max_date_query, max_days_to_load, increment_sdt)

        # Sync actuals
        self.data_min_date, self.data_max_date = self.sdt, self.edt

        # Insert record
        insert_query = f"""
            INSERT INTO audit.etl_runs (
                load_type, sources, target_database, target_table, 
                start_load_date, end_load_date, script_execution_start_time, 
                environment, status, script_version
            )
            VALUES (
                '{self.load_type}', '{{"{sources}"}}', '{target_database}', '{target_table}', 
                '{self.sdt.strftime("%Y-%m-%d %H:%M:%S")}', '{self.edt.strftime("%Y-%m-%d %H:%M:%S")}', 
                CURRENT_TIMESTAMP, '{os.environ.get('MACHINE_SCRIPT_RUNNER_ENV', 'DEV')}', 
                'In Progress', '{script_version}'
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