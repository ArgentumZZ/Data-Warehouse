import os
import datetime
import csv
import pandas as pd
from typing import List, Tuple, Optional, Any, Union
from datetime import timezone, timedelta

# Custom libraries

class EtlAuditManager:
    """
    Manages auditing for ETL processes by tracking execution metadata.

    This manager handles the creation and updating of an audit table ('etl_runs')
    to track timestamps, data ranges, record counts, and job status.
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
            scop: Pointer to the script custom object.
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

        # Initialize specialized database handler for Postgres
        self.dbase_postgres_audit = DatabaseHandlerFactory('postgresql')
        lg.logger.info('Etl_runs object is instantiated')

    def etl_runs_table_create_table_query(self) -> str:
        """
        Generates the SQL DDL for the etl_runs audit table.

        Returns:
            str: The SQL string for table creation and column commenting.
        """
        return f"""
                CREATE TABLE IF NOT EXISTS {self.audit_schema}.etl_runs (
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

    def init_db_data(self) -> None:
        """
        Initializes the database connection and ensures the audit table exists.
        """
        # 1. Configure credentials for the audit schema
        self.dbase_postgres_audit.set_credentials(self.credentials, self.audit_schema)

        # 2. Register audit table name
        # self.audit_table_name = 'etl_runs'

        # 2. Configure the audit table in the database helper
        self.dbase_postgres_audit.set_table(self.audit_table_name, self.audit_schema)

        # 3. Provide the DDL and ensure table exists
        self.dbase_postgres_audit.add_create_table_query(self.etl_runs_table_create_table_query())
        self.dbase_postgres_audit.check_and_create_table()

    def get_number_of_records(self,
                              dbase_object: Any,
                              delimiter: str) -> None:
        """
        Counts the number of records in the target CSV file and stores the result
        in `self.num_records`.

        Args:
            dbase_object: Object containing database-related metadata, including the
                file path under `engine.db_data['file']`.
            delimiter: The character used to separate values in the CSV file.
        """

        # 1. Extract the file path from the database object's metadata
        file_path = dbase_object.engine.db_data['file']

        # 2. Load the CSV using pandas to ensure accurate row counting,
        # especially when dealing with escape characters and complex quoting rules
        df = pd.read_csv(
                        file_path,
                        delimiter=delimiter,
                        escapechar='\\',
                        doublequote=False,
                        quoting=csv.QUOTE_NONE
                    )

        # 3. Store the number of records for audit tracking
        self.num_records = len(df)

        # 4. Log the result for visibility in ETL execution logs
        # gl.logger.info(f"ETL record count from {file_path}: {self.num_records}")

    def create_etl_runs_table_record(
            self,
            load_type: str,
            sources: List[str],
            target_database: str,
            target_table: str,
            prev_max_date_query: str,
            max_days_to_load: int = 365,
            staging_date_query: str = '',
            increment_sdt: bool = False
    ) -> None:
        """
        Creates a new entry in the audit table to mark the start of an ETL run.
        Determines the date window for the run (either from staging tables or
        historical audit data), inserts an initial 'In Progress' record, and
        stores the generated etl_runs_key.

        Args:
            load_type: Type of ETL load (e.g., 'FULL', 'INCREMENTAL').
            sources: List of source system identifiers.
            target_database: Name of the target database.
            target_table: Name of the target table.
            prev_max_date_query: SQL query used to determine the previous max date.
            max_days_to_load: Maximum number of days to load when calculating ranges.
            staging_date_query: Optional SQL query to fetch explicit min/max dates.
            increment_sdt: Whether to increment the start date by one day.
        """

        # 1. Normalize load type and ensure audit table is initialized
        self.load_type = load_type.upper()
        self.init_db_data()

        # 2. Determine the ETL date window
        if staging_date_query:
            # 2a. Use explicit staging table timestamps when provided
            # gl.logger.info('Using staging_date_query to determine min/max dates.')
            rslt = self.sfc.dbase_postgres.engine.execute_sql_query(staging_date_query, get_result=True)
            self.sdt, self.edt = rslt[0][0], rslt[0][1]
            self.data_min_date, self.data_max_date = self.sdt, self.edt
        else:
            # 2b. Otherwise derive the date window from audit history
            self.get_query_daterange(prev_max_date_query, max_days_to_load, increment_sdt)

        # 3. Prepare environment and source list formatting
        envir = os.environ.get('SCRIPT_RUNNER_ENV')
        sources_str = '","'.join(sources)

        # 4. Insert the initial audit record and retrieve the run key
        query = f"""
            INSERT INTO {self.audit_schema}.etl_runs (
                load_type, 
                sources, 
                target_database, 
                target_table, 
                start_load_date, 
                end_load_date, 
                script_execution_start_time, 
                environment, 
                status, 
                script_version)
            VALUES 
            ('{self.load_type}', 
            '{{"{sources_str}"}}', 
            '{target_database}', 
            '{target_table}', 
            '{self.sdt.strftime("%Y.%m.%d %H:%M:%S")}', 
            '{self.edt.strftime("%Y.%m.%d %H:%M:%S")}', 
             current_timestamp, 
             '{envir}', 
             'In Progress', 
             {'version'}
             -- '{gl.user_data['version']}'
             )
            RETURNING etl_runs_key;
        """

        # 5. Execute the insert and store the generated primary key
        self.etl_runs_key = self.dbase_postgres_audit.engine.execute_sql_query(query, get_result=True)[0][0]

    def update_etl_runs_table_record(self, dbase_object_upload: Any, delimiter: str) -> None:
        """
        Finalizes the audit record for the current ETL run.
        Ensures the record count is available, updates min/max dates, marks the run
        as Complete or Error, and sets the final timestamps.

        Args:
            dbase_object_upload: Object containing metadata for the uploaded file.
            delimiter: CSV delimiter used for counting records if needed.
        """
        # 1. Ensure record count is available (compute if missing)
        if self.num_records is None:
            self.get_number_of_records(dbase_object_upload, delimiter)

        # 2. Format previous max date for SQL (NULL-safe)
        p_max_dt = (
            f"'{self.prev_max_date.strftime('%Y-%m-%d %H:%M:%S')}'"
            if self.prev_max_date
            else "Null"
        )

        # 3. Build the final update statement with all ETL run metadata
        query = f"""
            UPDATE {self.audit_schema}.etl_runs SET  
                data_min_date = '{self.data_min_date.strftime("%Y.%m.%d %H:%M:%S")}',
                data_max_date = '{self.data_max_date.strftime("%Y.%m.%d %H:%M:%S")}',
                script_execution_end_time = current_timestamp,
                status = '{self.scop.status}',
                num_records = {self.num_records},
                prev_max_date = {p_max_dt},
                modified_at = current_timestamp
            WHERE etl_runs_key = {self.etl_runs_key}
        """

        # 4. Execute the update (no result expected)
        self.dbase_postgres_audit.engine.execute_sql_query(query, get_result=False)

    def get_max_dates(self, dbase_object: Any, query: str) -> Optional[Tuple[datetime.datetime, str]]:
        """
        Retrieves the latest processed date and script version from the target table.

        Args:
            dbase_object: Database object used to execute the query.
            query: SQL query expected to return (data_max_date, script_version).

        Returns:
            Optional[Tuple[datetime.datetime, str]]:
                A tuple of (max_date, version_string), or None if no rows exist.
        """
        # 1. Ensure script_version is included in the SELECT if missing
        if "script_version" not in query.lower():
            if "interval" not in query.lower():  # avoid breaking interval expressions
                query = query.replace(
                    "data_max_date",
                    "data_max_date, script_version::text"
                )

        # 2. Execute the query and return the first row if available
        max_dates_version = dbase_object.engine.execute_sql_query(query, get_result=True)
        if not max_dates_version:
            return None

        return max_dates_version[0][0], max_dates_version[0][1]

    def get_query_daterange(self, prev_max_date_query: str, max_days_to_load: int, increment_sdt: bool) -> None:
        """
        Calculates the SDT (Start Date Time) and EDT (End Date Time) for the ETL run.
        Uses audit history when available, otherwise defaults to a full-load window.

        Args:
            prev_max_date_query: SQL query to retrieve the previous max processed date.
            max_days_to_load: Maximum allowed extraction window in days.
            increment_sdt: Whether to advance SDT by one day to avoid overlap.
        """
        # 1. Attempt to retrieve the previous max date and version from audit history
        if self.sfp.dbase_postgres.engine.check_exists():
            max_info = self.get_max_dates(self.sfp.dbase_postgres, prev_max_date_query)
            if max_info:
                self.prev_max_date, self.prev_max_version = max_info

        # 2. Establish current time context (UTC)
        script_dt = gl.get_date()
        utc_now = datetime.datetime.now(timezone.utc)

        # 3. Determine Start Date (SDT)
        if self.load_type == 'F' or self.prev_max_date is None:
            # Full load: start at midnight of script execution date
            self.sdt = datetime.datetime(
                script_dt.year, script_dt.month, script_dt.day,
                0, 0, 0, tzinfo=timezone.utc
            )
        else:
            # Incremental load: start from previous max date
            self.sdt = self.prev_max_date
            if increment_sdt:
                # Optionally skip forward one day to avoid overlap
                self.sdt += timedelta(days=1)

        # 4. Determine End Date (EDT), capped by max_days_to_load and current time
        limit_date = self.sdt + timedelta(days=max_days_to_load)
        self.edt = min(utc_now, limit_date)

        # gl.logger.info(f"Calculated Time Range: SDT: {self.sdt} | EDT: {self.edt}")