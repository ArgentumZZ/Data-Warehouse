from typing import Any, List, Dict
import pandas as pd
import numpy as np
import utilities.logging_manager as lg
import json, re


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

    @staticmethod
    def convert_columns_to_int(df: pd.DataFrame,
                               columns_int_list: list[str]
                               ) -> pd.DataFrame:
        """
        Convert all columns in the given list to nullable Int64. Enforce early data corruption detection.

        Rules:
        - Raises an error on non-integer floats or invalid strings.
        - None / NaN values are preserved.
        """
        lg.info(f"Converting the columns in {columns_int_list} to integers.")
        for col in columns_int_list:

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

        lg.info("Conversion of columns to Int64 was successful.")
        return df

    @staticmethod
    def convert_columns_to_float(df: pd.DataFrame,
                                 columns_numeric_list: list[str]
                                 ) -> pd.DataFrame:
        """
        Convert columns to nullable Float64.
        Whitespace/empty strings are treated as NaN.
        Actual data corruption (letters, etc.) still raises an error
        """

        if not columns_numeric_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        lg.info(f"Converting the columns in {columns_numeric_list} to nullable Float64.")
        for col in columns_numeric_list:
            if col not in df.columns:
                raise KeyError(f"{col} not found")

            # 1. Replace empty strings or whitespace-only strings with np.nan
            # The regex ^\s*$ matches:
            # ^ : Start of string
            # \s*: Zero or more whitespace characters
            # $ : End of string
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)

            # 2. Convert to numeric (raises error for 'abc', etc.)
            # 3. Cast to nullable 'Float64'
            df[col] = pd.to_numeric(df[col], errors='raise').astype('Float64')

        lg.info("Conversion of columns to Float64 was successful.")
        return df

    @staticmethod
    def rename_columns(df: pd.DataFrame,
                       rename_columns_dict: Dict[str, str]
                       ) -> pd.DataFrame:

        """ Rename column names by passing a dictionary with old versions as keys and the new versions as values

         rename_columns_dict = {
                "old_version_1" : "new_version_2",
                "old_version_2" : "new_version_2"
                } """

        lg.info(f"Renaming the columns in the dictionary: {rename_columns_dict}")
        return df.rename(columns=rename_columns_dict)

    @staticmethod
    def validate_no_nulls(df: pd.DataFrame,
                          validate_no_nulls_string: str
                          ) -> pd.DataFrame:

        # 1. Check if a list was passed
        if not validate_no_nulls_string:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check that all columns exist in df
        for col in validate_no_nulls_string:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Check for nulls in each column
        lg.info("Running null columns check.")
        for col in validate_no_nulls_string:
            if df[col].isnull().any():
                raise ValueError(f"Column '{col}' contains null values.")

        lg.info("Validating no nulls completed successfully.")
        return df

    @staticmethod
    def replace_backslash(df: pd.DataFrame,
                          columns_replace_backslash_list: List[str] = None,
                          replace_with: str = ''):

        """
        1. Replace backslashes in the specified columns.
        2. Backslashes are special characters in: JSON, CSV, SQL strings, Regex, File paths.
        """

        # 1. Check if a list was passed
        if not columns_replace_backslash_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check that the columns exists in the dataframe
        missing = [c for c in columns_replace_backslash_list if c not in df.columns]
        if missing:
            lg.error(f"Columns not found in DataFrame: {missing}")
            raise ValueError(f"Columns not found in DataFrame: {missing}")

        # 3. Replace backslashes with parameter `replace_with`
        # .astype(str) ensures the .str accessor always works
        # regex=False prevents Pandas from interpreting backslashes as regex escapes
        for col in columns_replace_backslash_list:
            df[col] = df[col].astype(str).str.replace("\\", replace_with, regex=False)

        lg.info("Backslash replacement completed successfully.")
        return df

    @staticmethod
    def escape_backslash(df: pd.DataFrame,
                         columns_escape_backslash_list: List[str] = None):

        """
        1. Escape backslashes in the specified columns.
        2. Backslashes are special characters in: JSON, CSV, SQL strings, Regex, File paths.
        3. Turn them into literal characters instead of letting them act as a control character.
        """

        # 1. Check if a list was passed
        if not columns_escape_backslash_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check that the columns exists in the dataframe
        missing = [c for c in columns_escape_backslash_list if c not in df.columns]
        if missing:
            lg.error(f"Columns not found in DataFrame: {missing}")
            raise ValueError(f"Columns not found in DataFrame: {missing}")

        # 3. Escape backslashes with \\
        # .astype(str) ensures the .str accessor always works
        # regex=False prevents Pandas from interpreting backslashes as regex escapes
        for col in columns_escape_backslash_list:
            df[col] = df[col].astype(str).str.replace("\\", "\\\\", regex=False)

        lg.info("Escaping backslash completed successfully.")
        return df

    @staticmethod
    def lowercase_column_names(df: pd.DataFrame) -> pd.DataFrame:
        lg.info("Converting all column names to lowercase.")
        df.columns = df.columns.str.lower()

        lg.info("Lowercasing column names completed successfully.")
        return df

    @staticmethod
    def manage_json_columns(df: pd.DataFrame, columns_json_list: List[str] = None) -> pd.DataFrame:
        """
        Converts Python objects (dicts/lists) into valid JSON strings.

        Why:
        1. Pandas/Python defaults to single quotes {'a': 1} which Postgres rejects.
        2. json.dumps() forces double quotes {"a": 1} which Postgres requires.
        3. The check prevents double-stringifying columns that are already strings.
        """

        if not columns_json_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        for col in columns_json_list:
            if col in df.columns:
                lg.info(f"Serializing JSON for column: {col}")
                # json.dumps ensures double quotes are used: {"key": "value"}
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    # ^ Explanation:
                    # - If x is a dict/list: Convert to "{"proper": "json"}"
                    # - If x is None/NaN: Leave as is so Postgres sees it as NULL
                    # - If x is a string: Skip to avoid "{\"nested\": \"quotes\"}"
                )

        lg.info("Managing JSON columns completed successfully.")
        return df

    @staticmethod
    def strip_column_values(df: pd.DataFrame,
                            columns_strip_list: List[str] = None) -> pd.DataFrame:


        # 1. Check if a list was passed
        if not columns_strip_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Filter the list to only include columns that actually exist AND are objects/strings
        valid_string_cols = df[columns_strip_list].select_dtypes(include=['object', 'string']).columns
        lg.info(f"Valid string columns list: {valid_string_cols}.")

        # 3. Apply transformation only to valid columns
        lg.info("Stripping whitespace from column values.")
        for col in valid_string_cols:
            # Vectorized strip and replace
            df[col] = df[col].str.strip().replace('', np.nan)

        lg.info("Stripping column values completed successfully.")
        return df

    @staticmethod
    def check_non_null_columns(df: pd.DataFrame,
                               columns_non_null_list: List[str] = None
                               ) -> pd.DataFrame:
        """Raises ValueError if any non-null columns contain NULLs."""

        # 1. Check if a list was passed
        if not columns_non_null_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if the columns exist
        for col in columns_non_null_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

            # 3. Raise an error if a NULL is found
            if df[col].isnull().any():
                raise ValueError(f"Data Quality Error: Column '{col}' contains null values.")

        lg.info("Checking non null columns completed successfully.")
        return df

    @staticmethod
    def handle_duplicates(df: pd.DataFrame,
                          columns_unique_list: List[str] = None,
                          action='raise') -> pd.DataFrame:

        """
        Action 'raise' (default): Stop the pipeline if duplicates exist.
        Action 'drop': Remove them and keep the first occurrence.

        Raises ValueError if duplicated records were found in the unique list."""

        # 1. Check if a list was passed
        if not columns_unique_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if the columns exist
        for col in columns_unique_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Raise an error if a duplicate is found
        duplicates = df.duplicated(subset=columns_unique_list)
        if duplicates.any():
            if action == 'raise':
                duplicated_counts = duplicates.sum()
                raise ValueError(f"Found {duplicated_counts} duplicate rows based on {columns_unique_list}")
            elif action == 'drop':
                df = df.drop_duplicates(subset=columns_unique_list, keep='first')
            return df

        lg.info("Handling duplicates completed successfully.")
        return df

    @staticmethod
    def format_date_columns(df: pd.DataFrame,
                            columns_date_config_dict: Dict[str, str]) -> pd.DataFrame:
        """
        1. Convert date columns to datetime pandas objects.
        2. Convert the columns to the applied date format.

        Example: column_config: {'created_at': '%Y-%m-%d %H:%M:%S',
                                 'birth_date': '%Y-%m-%d'}
        """
        # 1. Check if a list was passed
        if not columns_date_config_dict:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if the columns exist
        for col in columns_date_config_dict.keys():
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Convert date/timestamp column to datetime object and the corresponding format
        for col, date_format in columns_date_config_dict.items():
            df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')

        lg.info("Formating date columns completed successfully.")
        return df

    @staticmethod
    def sanitize_columns(df: pd.DataFrame,
                         columns_sanitize_list: List[str] = None,
                         replace_with=' '):
        """
        Cleans string columns by replacing control characters (\r\n\t) with a placeholder
        and normalizing empty or null-like strings to actual Null values.

        Args:
            df (pd.DataFrame): The DataFrame to process.
            columns_sanitize_list (list): List of column names to apply cleaning to.
            replace_with (str): The string to insert in place of newlines/tabs.
                                Defaults to a single space.

        Returns:
            pd.DataFrame: The modified DataFrame with sanitized columns.
        """

        # 1. Check if the list was passed
        if not columns_sanitize_list:
            return df

        # 2. Check if the columns exist in the data frane
        for col in columns_sanitize_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Apply the transformations
        for col in columns_sanitize_list:

            # Step 1: Force column to string type and replace \r (carriage return),
            # \n (newline), and \t (tab) with the replacement string using regex.
            df[col] = df[col].astype(str).str.replace(r'[\r\n\t]+', replace_with, regex=True)

            # Step 2: After the string conversion and replacement, cleanup the "noise".
            # - '' (Empty strings) occur if the cell was empty or only contained control chars.
            # - 'nan' occurs because .astype(str) converts actual np.nan into a string.

            # This converts both back to a proper numpy NaN object.
            df[col] = df[col].replace(['', 'nan'], np.nan)

        return df

    def set_comments(self):
        pass

    def check_source_date_range(self):
        pass

    def run_data_quality_check(self):
        pass

    def get_command_line_parameters(self):
        pass

    def set_params_based_on_command_line(self):
        pass

    def delete_target_dates(self):
        pass

    def set_reference_page(self):
        pass

    @staticmethod
    def transform_dataframe(df: pd.DataFrame,
                            validate_no_nulls_string: str = None,
                            columns_int_list: List[str] = None,
                            columns_numeric_list: List[str] = None,
                            columns_str_dict: Dict[str, str] = None,
                            columns_replace_backslash_list: List[str] = None,
                            columns_escape_backslash_list: List[str] = None,
                            columns_json_list: List[str] = None,
                            columns_strip_list: List[str] = None,
                            columns_non_null_list: List[str] = None,
                            columns_unique_list: List[str] = None,
                            columns_date_config_dict: Dict[str, str] = None,
                            columns_sanitize_list: List[str] = None,
                            columns_lowercase: bool = True
                            ) -> pd.DataFrame:

        # 1. Rename columns
        if columns_str_dict:
            df = EtlUtils.rename_columns(df=df, rename_columns_dict=columns_str_dict)

        # 2. Lowercase columns
        if columns_lowercase:
            df = EtlUtils.lowercase_column_names(df=df)

        # 3. Handle JSON Columns (Essential for Postgres)
        if columns_json_list:
            df = EtlUtils.manage_json_columns(df=df, columns_json_list=columns_json_list)

        # 4. Convert columns to integer
        if columns_int_list:
            df = EtlUtils.convert_columns_to_int(df=df, columns_int_list=columns_int_list)

        # 5. Convert columns to float
        if columns_numeric_list:
            df = EtlUtils.convert_columns_to_float(df=df, columns_numeric_list=columns_numeric_list)

        # 6. Check for null values in unique columns
        if validate_no_nulls_string:
            df = EtlUtils.validate_no_nulls(df=df, validate_no_nulls_string=validate_no_nulls_string)

        # 7. Replace backslashes
        if columns_replace_backslash_list:
            df = EtlUtils.replace_backslash(df=df, columns_replace_backslash_list=columns_replace_backslash_list)

        # 8. Escape backslashes
        if columns_escape_backslash_list:
            df = EtlUtils.escape_backslash(df=df, columns_escape_backslash_list=columns_escape_backslash_list)

        # 9. Strip whitespace
        if columns_strip_list:
            df = EtlUtils.strip_column_values(df=df, columns_strip_list=columns_strip_list)

        # 10. Sanitize_columns
        if columns_sanitize_list:
            df = EtlUtils.sanitize_columns(df=df, columns_sanitize_list=columns_sanitize_list)

        # 11. Check for NULLS
        if columns_non_null_list:
            df = EtlUtils.check_non_null_columns(df=df, columns_non_null_list=columns_non_null_list)

        # 12. Check for duplicates
        if columns_unique_list:
            df = EtlUtils.handle_duplicates(df=df, columns_unique_list=columns_unique_list)

        # 13. Format date columns
        if columns_date_config_dict:
            df = EtlUtils.format_date_columns(df=df, columns_date_config_dict=columns_date_config_dict)

        return df

    def process_dataframe_date_ranges(self,
                                      df: pd.DataFrame,
                                      date_columns: List[str]
                                      ) -> None:
        # list of timestamps
        date_min_list = [pd.to_datetime(df[column]).min() for column in date_columns]
        date_max_list = [pd.to_datetime(df[column]).max() for column in date_columns]

        # timestamp
        data_min_date = min(date_min_list)
        data_max_date = max(date_max_list)

        # set data_min/max_date to audit manager
        self.sfp.etl_audit_manager.data_min_date = data_min_date
        self.sfp.etl_audit_manager.data_max_date = data_max_date
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
