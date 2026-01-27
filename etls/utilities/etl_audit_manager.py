# import libraries
import os, datetime
from typing import List, Tuple, Optional, Any, Union
from datetime import timezone, timedelta

# Custom libraries
import utilities.logging_manager as lg
from connectors.postgresql_connector import PostgresqlConnector

class EtlAuditManager:
    """
    I. Manages auditing for ETL processes by tracking execution metadata.
    II. Create a table: @staticmethod: create_audit_etl_runs_table()
    III. Insert new etl runs records: instance method: insert_audit_etl_runs_record(self, ...)
    IV. Update etl runs records: instance method: update_etl_runs_table_record(self, ...)

    Algorithm: ETL Run Auditing and Tracking

    Step 1. Verify audit infrastructure.
        1.1. If the audit schema does not exist, create it.
        1.2. If the audit.etl_runs table does not exist, create it with columns:
            – a surrogate primary key identifying the ETL run.
            – load metadata (load type, sources, target database, target table).
            – load window boundaries (start_load_date, end_load_date).
            – processed data time boundaries (data_min_date, data_max_date).
            – script execution timestamps (start and end).
            – environment, execution status, and script version.
            – operational metrics (num_records, prev_max_date).
            – audit timestamps (created_at, modified_at) with default values.

    Step 2. Register a new ETL execution.
        2.1. At job start, insert a new record into audit.etl_runs and return etl_runs_key.
        2.2. Determine whether a previous run exists for the same target table.
            - If no previous run exists, enforce Full (F) mode.
            - If a previous run exists, allow Full (F) or Incremental (I) mode based on configuration.

    Step 3. Initialize execution parameters.
        3.1. For Full (F) mode:
            - Interpret internal configuration parameters (=default load_type) and/or external parameters (set in .bat).
            - Set start_load_date to the previous data_max_date or the provided execution date.
            - Set end_load_date to start_load_date plus the configured maximum load window (= default max_days_to_load).
            - Set execution start time, creation and modification timestamps to the current time.
            - Set status to "In Progress" and populate static configuration fields.

        3.2. For Incremental (I) mode:
            - Set start_load_date to the previous data_max_date.
            - Set end_load_date to start_load_date plus the default load window (=max_days_to_load).
            - Set execution start time, creation and modification timestamps to the current time.
            - Set status to "In Progress" and populate static configuration fields.
            - Initialize data_min_date and data_max_date from the previous run.

    Step 4. Finalize the ETL run.
        4.1. Update the etl_runs record using etl_runs_key.
        4.2. Set status to "Complete" on success or "Error" on failure.
        4.3. Set execution end time and modification timestamp to the current time.
        4.4. Set num_of_records to the number of processed records.
        4.5. Update data_min_date and data_max_date using the minimum and maximum timestamps from the processed
             dataset’s time column.
    """

    def __init__(self,
                 sfc: "ScriptFactory",
                 swc: "ScriptWorker",
                 credential_name: str) -> None:
        """
        Initializes the audit manager with script factory, script worker and database credential names.

        Args:
            sfp: Pointer to the script factory class.
            swc: Pointer to the script worker object.
            credential_name: DB credentials where to create the audit table (DWH DEV or PROD credentials).
        """
        self.version: str = '1.0'                                               # Version of EtlAuditManager
        self.sfc = sfc                                                          # An instance of ScriptFactory class
        self.swc = swc                                                          # An instance of ScriptWorker class
        self.credential_name = credential_name                                  # Name of credential connection

        # State tracking for the current ETL run
        self.etl_runs_key: Optional[int] = None                                   # An ETL run key identifier
        self.sdt: Optional[datetime.datetime] = None                              # Start Date Time
        self.edt: Optional[datetime.datetime] = None                              # End Date Time
        self.data_min_date: Optional[datetime.datetime] = None                    # Minimum timestamp of ETL batch
        self.data_max_date: Optional[datetime.datetime] = None                    # Maximum timestamp of ETL batch
        self.load_type: Optional[str] = None                                      # Increment (I) or full (F) load type
        self.num_of_records: Optional[int] = None                                 # Number of records extracted
        self.prev_max_date: Optional[datetime.datetime] = None                    # Previous data max date
        self.pg_connector = PostgresqlConnector(credential_name=credential_name)  # Initialize PostgresConnector
        lg.info('Etl Audit Manager object is instantiated')

    @staticmethod
    def create_audit_etl_runs_table() -> str:
        """
        Generates the SQL statement to create the audit.etl_runs table.

        The table stores one record per ETL execution and captures load metadata, load window boundaries,
        processed data time ranges, execution timestamps, runtime environment details, execution status,
        and operational metrics. The table is created only if it does not already exist.

        Returns:
            str:
                SQL CREATE TABLE statement for the audit.etl_runs table.
        """
        return f"""
                CREATE TABLE IF NOT EXISTS audit.etl_runs (
                    etl_runs_key                    BIGSERIAL PRIMARY KEY,
                    load_type                       TEXT,
                    sources                         TEXT,
                    script_version                  TEXT DEFAULT NULL,
                    status                          TEXT DEFAULT NULL,
                    environment                     TEXT,
                    num_of_records                  BIGINT DEFAULT NULL,
                    start_load_date                 TIMESTAMPTZ,
                    end_load_date                   TIMESTAMPTZ,
                    prev_max_date                   TIMESTAMPTZ DEFAULT NULL,
                    data_min_date                   TIMESTAMPTZ,
                    data_max_date                   TIMESTAMPTZ,
                    script_execution_start_time     TIMESTAMPTZ,
                    script_execution_end_time       TIMESTAMPTZ,  
                    target_database                 TEXT,
                    target_table                    TEXT,
                    created_at                      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    modified_at                     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP 
                );
                """

    def _calculate_etl_window(self,
                              prev_max_date_query: str,
                              max_days_to_load: int,
                              increment_sdt: bool,
                              load_type: str,
                              forced_sdt: Optional[str]) -> tuple[datetime.datetime, datetime.datetime]:
        """
        This method determines the start date-time (SDT / start_load_date) and end date-time (EDT / end_load_date)
        based on the previous successful run, load mode (Full or Incremental), and runtime configuration.

        Args:
            prev_max_date_query:
                SQL query used to retrieve the maximum processed data timestamp
                from the most recent successful ETL run.
            max_days_to_load:
                Maximum number of days to include in the load window.
            increment_sdt (bool):
                If True, increments the start date by one day in Incremental mode
                (commonly used for daily-partitioned datasets).
            load_type (str):
                Load mode indicator:
                - 'F' for Full load
                - 'I' for Incremental load
            forced_sdt (str):
                Optional forced start date (YYYY-MM-DD). When provided, this value
                overrides the previous maximum date in Full load mode.

        Returns:
            tuple[datetime.datetime, datetime.datetime]:
                A tuple containing:
                - sdt: Calculated start date-time (timezone-aware, UTC)
                - edt: Calculated end date-time (timezone-aware, UTC)

        Raises:
            ValueError:
                If Full mode is selected and neither a forced start date nor a previous maximum date is available.
        """

        # 1. Get the latest successfully loaded data max date
        result_data_max_date = self.pg_connector.run_query(query=prev_max_date_query, commit=False, get_result=True)
        lg.info(f"Result data max date: {result_data_max_date}")

        # 2. Check if the query actually returned a date
        if not result_data_max_date.empty:
            # Assign it to a variable, .iat[0, 0] retrieves the first row, first column
            value = result_data_max_date.iat[0, 0]
            lg.info(f"The _calculate_etl_window value: {value}")
            if value is not None:
                self.prev_max_date = value
                lg.info(f"The _calculate_etl_window prev_max_date: {self.prev_max_date}")
            else:
                self.prev_max_date = None
                lg.info(f"The _calculate_etl_window prev_max_date: {self.prev_max_date}")
        else:
            self.prev_max_date = None
            lg.info(f"The _calculate_etl_window prev_max_date: {self.prev_max_date}")

        # 3. Generate a current reference timestamp
        now_utc = datetime.datetime.now(timezone.utc)
        lg.info(f"The NOW UTC: {now_utc}")

        lg.info(f"Running in {load_type} mode.")
        # 4. Determine sdt (start date time) for F and I modes
        if load_type == 'F':
            # if we pass a date in .bat file (=forced_sdt)
            if forced_sdt:
                forced_sdt = datetime.datetime.strptime(forced_sdt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                lg.info(f"Forced sdt (F mode): {forced_sdt}")

                self.sdt = forced_sdt  # this should become the new start date
                lg.info(f"The sdt (F mode): {self.sdt}")
            elif self.prev_max_date:   # if not, start from the previous_max_date
                self.sdt = self.prev_max_date
                lg.info(f"The sdt (F mode): {self.sdt}")
            else:
                # elif self.prev_max_date is None:
                raise ValueError("F mode requires either a forced_sdt or a previous max date.")

            # 5. Determine edt (F mode loads exactly X days)
            self.edt = self.sdt + timedelta(days=max_days_to_load)
            lg.info(f"The edt (F mode): {self.edt}")

        # 6. Incremental load: Start from where we left off last time.
        elif load_type == 'I' and self.prev_max_date:
            # If increment_sdt is True, we add 1 day (standard if data is daily-partitioned, e.g. for ftp projects).
            self.sdt = self.prev_max_date + timedelta(days=1) if increment_sdt else self.prev_max_date
            lg.info(f"The sdt (I mode): {self.sdt}")

            # 7. Determine EDT (incremental cannot load future data)
            max_window_end = self.sdt + timedelta(days=max_days_to_load)
            lg.info(f"The _calculate_etl_window max_window_end: {max_window_end}")

            # 8. EDT becomes the minimum of now_utc and max_window_end
            self.edt = min(now_utc, max_window_end)
            lg.info(f"The edt (I mode): {self.edt}")

        lg.info(f"The final _calculate_etl_window self.sdt and self.edt values: {self.sdt} {self.edt}")
        return self.sdt, self.edt

    def insert_audit_etl_runs_record(
            self,
            load_type: str,
            sources: List[str],
            target_database: str,
            target_table: str,
            prev_max_date_query: str,
            script_version: str,
            forced_sdt: Optional[str],
            max_days_to_load: int = 365,
            increment_sdt: bool = False
    ) -> None:
        """
        Inserts a new ETL run record into the audit.etl_runs table.

        This method initializes the audit schema and table if they do not exist, calculates the ETL load window
        (SDT and EDT), inserts a new audit record for the current ETL execution, and stores the generated run
        identifier.

        Args:
            load_type:
                Load mode indicator:
                - 'F' for Full load
                - 'I' for Incremental load
            sources:
                List of sources involved in the ETL run.
            target_database:
                Name of the target database receiving the loaded data.
            target_table:
                Name of the target table receiving the loaded data.
            prev_max_date_query:
                SQL query used to retrieve the maximum processed data timestamp
                from the most recent successful ETL run.
            script_version:
                Version identifier of the executing ETL script.
            forced_sdt:
                Optional forced start date (YYYY-MM-DD). Used primarily in Full
                load mode to override the previous maximum data timestamp.
            max_days_to_load:
                Maximum number of days to include in the load window.
                Defaults to 365.
            increment_sdt:
                If True, increments the start date by one day in Incremental
                load mode (commonly used for daily-partitioned datasets).
                Defaults to False.

        Raises:
            ValueError:
                Propagated from `_calculate_etl_window` if load window calculation fails due to invalid configuration.
        """

        # 1. If running for a first time, create the schema and the table
        self.pg_connector.create_schema(schema='audit')

        lg.info(f"Running the create_audit_etl_runs_table query: {EtlAuditManager.create_audit_etl_runs_table()}")
        self.pg_connector.run_query(query=EtlAuditManager.create_audit_etl_runs_table())

        # 2. Process some of the variables
        sources_string = ", ".join(sources)

        # 3. Use the internal function `_calculate_etl_window` to calculate sdt and edt
        self.sdt, self.edt = self._calculate_etl_window(prev_max_date_query=prev_max_date_query,
                                                        max_days_to_load=max_days_to_load,
                                                        increment_sdt=increment_sdt,
                                                        load_type=load_type,
                                                        forced_sdt=forced_sdt)

        # 4. Insert query
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
                '{load_type}', 
                '{sources_string}', 
                '{target_database}', 
                '{target_table}', 
                '{self.sdt.strftime('%Y-%m-%d %H:%M:%S.%f%z')}', 
                '{self.edt.strftime('%Y-%m-%d %H:%M:%S.%f%z')}', 
                CURRENT_TIMESTAMP, 
                '{os.environ.get('MACHINE_SCRIPT_RUNNER_ENV')}', 
                'In Progress', 
                '{script_version}'
            )
            RETURNING etl_runs_key;
        """

        # 5. Run the insert query
        lg.info(f"Running the INSERT INTO query: {insert_query}")

        # 6. The method .iat[0, 0] retrieves the first row, first column
        self.etl_runs_key = self.pg_connector.run_query(query=insert_query, commit=True, get_result=True).iat[0, 0]
        lg.info(f"Etl_runs_key: {self.etl_runs_key}")

    def update_etl_runs_table_record(self, status: str) -> None:
        """
        1. Fetch the number of records in the df.
        2. Get the data min/max dates.
        3. Update audit.etl_runs record.
        """

        # 1. Fetch the number of records in the df
        self.num_of_records = self.swc.num_of_records
        lg.info(f"Final count pulled from script worker: {self.num_of_records}")

        # 2. Fetch data_min_date and data_max_date from Script Worker if they exist (len(df) > 0)
        # Otherwise, set sdt to data_min_date and data_max_date
        if self.swc.data_min_date:
            self.data_min_date = self.swc.data_min_date
        lg.info(f"Data min date pulled from script worker: {self.data_min_date}")

        if self.swc.data_max_date:
            self.data_max_date = self.swc.data_max_date
        lg.info(f"Data min date pulled from script worker: {self.data_max_date}")

        # 3. Format dates for SQL
        data_min_val = f"'{self.data_min_date.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'" if self.data_min_date is not None else "NULL"
        data_max_val = f"'{self.data_max_date.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'" if self.data_max_date is not None else "NULL"
        prev_date_val = f"'{self.prev_max_date.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'" if self.prev_max_date else "NULL"

        # 4. Update Query
        update_query = f"""
            UPDATE audit.etl_runs 
            SET  
                data_min_date = {data_min_val},
                data_max_date = {data_max_val},
                script_execution_end_time = CURRENT_TIMESTAMP,
                status = '{status}',
                num_of_records = {self.num_of_records},
                prev_max_date = {prev_date_val},
                modified_at = CURRENT_TIMESTAMP
            WHERE etl_runs_key = {self.etl_runs_key};
        """

        # 5. Run the update query
        lg.info(f"Running the UPDATE query: {update_query}")
        self.pg_connector.run_query(query=update_query, commit=True, get_result=False)