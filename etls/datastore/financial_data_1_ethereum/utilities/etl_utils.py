from typing import Any, List, Dict
import pandas as pd
import utilities.logging_manager as lg


class EtlUtils:
    """
    Comprehensive ETL utilities class.
    Contains helpers for:
    - DataFrame transformations
    - File operations
    - ETL metadata
    - Data quality checks
    - Testing helpers
    - Credential helpers
    - Database utilities
    - Orchestration helpers
    - Generic utilities
    """

    def __init__(self,
                 sfc: 'ScriptFactory') -> None:
        self.sfc = sfc
        self.version = "1.0"

    def convert_columns_to_int(self,
                               df: pd.DataFrame,
                               convert_columns_int_list: list[str]
                               ) -> pd.DataFrame:
        """
        Convert all columns in the given list to nullable Int64.

        Rules:
        - Raises an error on non-integer floats or invalid strings.
        - None / NaN values are preserved.
        """
        lg.info(f"Converting the columns in {convert_columns_int_list} to integers.")
        for col in convert_columns_int_list:

            # 1. Convert to numeric, raising an error for invalid strings
            numeric_series = pd.to_numeric(df[col], errors='raise')

            # 2. Ensure all non-NA values are whole numbers (e.g., 1.0 is okay, 1.3 is not)
            #    - .dropna(): Ignores None/NaN values so they don't interfere with the check
            #    - % 1 == 0: Vectorized check if values are whole numbers
            #    - .all(): Ensures **every single non-NA value** passes the check
            if not ((numeric_series.dropna() % 1) == 0).all():
                raise ValueError(f"Column '{col}' contains non-integer decimals.")

            # 3. Convert to nullable Int64
            df[col] = numeric_series.astype('Int64')

        lf.info("Conversion of columns to Int64 was successful.")
        return df

    def rename_columns(self,
                       df: pd.DataFrame,
                       rename_columns_dict: Dict[str, str]
                       ) -> pd.DataFrame:

        """ Rename column names by passing a dictionary with old versions as keys and the new versions as values

         rename_columns_dict = {
                "old_version_1" : "new_version_2",
                "old_version_2" : "new_version_2"
                } """
        lg.info(f"Renaming the columns in the dictionary: {rename_columns_dict}")
        return df.rename(columns=rename_columns_dict)



    def transform_dataframe(self,
                            df: pd.DataFrame,
                            convert_columns_int_list: List[int] = None,
                            rename_columns_dict: Dict[str, str] = None,

                            move_etl_runs_key_before='',
                            columns_esc_backslash=[],
                            columns_replace_backslash_list=[],
                            replace_backslash_with='',
                            columns_strip_list=[],
                            columns_replace_newline=[],
                            replace_newline_with=''
                            ) -> pd.DataFrame:

        # Convert columns to integer
        if convert_columns_int_list:
            df = convert_columns_to_int(df=df, convert_columns_int_list=convert_columns_int_list)

        # Rename columns
        if rename_columns_dict:
            df = self.rename_columns(df=df, rename_columns_dict=rename_columns_dict)

        pass

    def process_dataframe_date_ranges(self,
                                      df: pd.DataFrame,
                                      date_columns: List[str]
                                      ) -> None:
        # list of timestamps
        date_min_list = [pd.to_datetime(df[column]).min() for column in date_columns]
        date_max_list = [pd.to_dateime(df[column]).max() for column in date_columns]

        # timestamp
        data_min_date = min(date_min_list)
        data_max_date = max(data_max_list)

        # set data_min/max_date to audit manager
        self.sfp.etl_audit_manager.data_min_date = self.data_min_date
        self.sfp.etl_audit_manager.data_max_date = self.data_max_date
        pass

    # ===========================================================
    # 1. DataFrame column utilities
    # ===========================================================

    # 1.1 Column type conversions
    def convert_column_types(self):
        """Convert DataFrame columns to specific data types (int, float, str, etc.)."""
        pass

    # 1.2 String cleaning
    def clean_string_columns(self):
        """Trim whitespace, normalize casing, remove unwanted characters in string columns."""
        pass

    # 1.3 Date parsing
    def parse_date_columns(self):
        """Convert string date columns into proper datetime objects."""
        pass

    # 1.4 Null handling
    def handle_null_values(self):
        """Fill, drop, or otherwise handle null values in the DataFrame."""
        pass

    # 1.5 Deduplication
    def remove_duplicates(self):
        """Remove duplicate rows based on key columns or full row comparison."""
        pass

    # 1.6 Column renaming
    def rename_columns(self):
        """Rename DataFrame columns to standardized names."""
        pass

    # 1.7 Schema validation
    def validate_schema(self):
        """Ensure the DataFrame matches the expected schema (columns, types, order)."""
        pass

    # 1.8 Column ordering
    def reorder_columns(self):
        """Reorder DataFrame columns into a predefined or logical sequence."""
        pass

    # 1.9 Flattening nested structures
    def flatten_nested_columns(self):
        """Flatten nested JSON-like structures into separate DataFrame columns."""
        pass

    # 1.10 Merging small lookup tables
    def merge_lookup_tables(self):
        """Merge small lookup/reference tables into the main DataFrame."""
        pass

    # ===========================================================
    # 2. File utilities
    # ===========================================================

    # 2.1 Safe CSV/Parquet writing
    def save_dataframe_safely(self):
        """Write DataFrame to CSV/Parquet with overwrite protection and validation."""
        pass

    # 2.2 File compression
    def compress_file(self):
        """Compress a file (gzip/zip) for storage or upload."""
        pass

    # 2.3 File hashing
    def compute_file_hash(self):
        """Compute MD5/SHA hash of a file for integrity checks."""
        pass

    # 2.4 File size validation
    def validate_file_size(self):
        """Ensure file size is within expected limits."""
        pass

    # 2.5 Temporary file cleanup
    def cleanup_temp_files(self):
        """Remove temporary files created during ETL processing."""
        pass

    # ===========================================================
    # 3. ETL metadata utilities
    # ===========================================================

    # 3.1 Add ETL run ID
    def add_etl_runs_key_to_csv(self):
        """Add ETL run ID or metadata column to the output CSV/Parquet."""
        pass

    # 3.2 Add timestamps
    def add_timestamp_columns(self):
        """Add created_at / updated_at / processed_at timestamps."""
        pass

    # 3.3 Add source metadata
    def add_source_metadata(self):
        """Add metadata such as source system, batch number, or file origin."""
        pass

    # ===========================================================
    # 4. Data quality utilities
    # ===========================================================

    # 4.1 Column-level validation
    def validate_column_values(self):
        """Validate column values against expected ranges or patterns."""
        pass

    # 4.2 Row count checks
    def validate_row_count(self):
        """Ensure row count is within expected thresholds."""
        pass

    # 4.3 Schema drift detection
    def detect_schema_drift(self):
        """Detect unexpected changes in schema compared to previous runs."""
        pass

    # 4.4 Duplicate detection
    def detect_duplicates(self):
        """Identify duplicate rows without removing them."""
        pass

    # 4.5 Referential integrity checks
    def check_referential_integrity(self):
        """Validate relationships between DataFrames (foreign key checks)."""
        pass

    # 4.6 Check source date range
    def check_source_date_range(self):
        """
        Validate that the source data falls within the expected date range.
        Useful for detecting missing days, unexpected future dates,
        or gaps in incremental loads.
        """
        pass

    # 4.7 Run full data quality check
    def run_data_quality_check(self):
        """
        Execute a full suite of data quality checks, including:
        - schema validation
        - null checks
        - duplicate detection
        - referential integrity checks
        - date range validation
        - row count thresholds
        This acts as a master validator before saving or uploading data.
        """
        pass

    # ===========================================================
    # 5. Testing utilities
    # ===========================================================

    # 5.1 Sample DataFrame generators
    def generate_sample_dataframe(self):
        """Generate sample DataFrames for testing transformations."""
        pass

    # 5.2 Schema comparison helpers
    def compare_schemas(self):
        """Compare two DataFrame schemas for equality."""
        pass

    # 5.3 Mock connection builders
    def build_mock_connection(self):
        """Create a mock DB/API connection for testing."""
        pass

    # 5.4 Mock credential loaders
    def load_mock_credentials(self):
        """Load fake credentials for testing ETL logic."""
        pass

    # ===========================================================
    # 6. Credential helpers
    # ===========================================================

    # 6.1 Safe credential parsing
    def parse_credentials(self):
        """Parse credentials from config/environment variables safely."""
        pass

    # 6.2 Environment variable helpers
    def load_env_credentials(self):
        """Load credentials from environment variables."""
        pass

    # 6.3 Connection string builders
    def build_connection_string(self):
        """Build a connection string from credential components."""
        pass

    # ===========================================================
    # 7. Database utilities
    # ===========================================================

    # 7.1 Create triggers
    def create_trigger(self):
        """Create a database trigger for audit or automation purposes."""
        pass

    # 7.2 Create log/audit tables
    def create_log_table(self):
        """Create a log or audit table in the target database."""
        pass

    # 7.3 Table existence checks
    def check_table_exists(self):
        """Check if a table exists in the target database."""
        pass

    # 7.4 Safe table truncation
    def truncate_table_safely(self):
        """Safely truncate a table with validation and logging."""
        pass

    # 7.5 Index creation
    def create_index(self):
        """Create an index on a table to improve performance."""
        pass

    # ===========================================================
    # 8. Orchestration helpers
    # ===========================================================

    # 8.1 Retry wrappers
    def retry_operation(self):
        """Retry a function call with exponential backoff."""
        pass

    # 8.2 Timing decorators
    def measure_execution_time(self):
        """Measure execution time of ETL steps."""
        pass

    # 8.3 Performance logging
    def log_performance_metrics(self):
        """Log performance metrics for ETL steps."""
        pass

    # 8.4 Dependency checks
    def check_dependencies(self):
        """Check if required files, tables, or configs exist before ETL."""
        pass

    # ===========================================================
    # 9. Generic utilities
    # ===========================================================

    # 9.1 Safe JSON loading
    def load_json_safely(self):
        """Load JSON files with error handling."""
        pass

    # 9.2 Safe YAML loading
    def load_yaml_safely(self):
        """Load YAML files with error handling."""
        pass

    # 9.3 Path normalization
    def normalize_path(self):
        """Normalize file paths across OS environments."""
        pass

    # 9.4 String normalization
    def normalize_string(self):
        """Normalize strings (unicode, accents, whitespace)."""
        pass

    # 9.5 Date/time helpers
    def get_current_timestamp(self):
        """Return the current timestamp in a standardized format."""
        pass
