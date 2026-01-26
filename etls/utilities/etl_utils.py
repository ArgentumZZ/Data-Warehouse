# import libraries
from typing import Any, List, Dict, Tuple
import pandas as pd
import numpy as np
import json, pytz

# import custom libraries
import utilities.logging_manager as lg

class EtlUtils:
    """
    A Comprehensive ETL utilities class that contains helpers for:
        - DataFrame transformations
        - Generic ETL utilities
    """

    def __init__(self,
                 sfc: 'ScriptFactory') -> None:
        self.sfc = sfc
        self.version = "1.0"

    @staticmethod
    def rename_columns(df: pd.DataFrame,
                       rename_columns_dict: Dict[str, str]
                       ) -> pd.DataFrame:

        """
        Rename column names by passing a dictionary with old versions as keys and the new versions as values

        Args:
            df:
                Input pandas DataFrame
            rename_columns_dict:
                a dictionary with columns for renaming. For example,
                                 rename_columns_dict = {
                                        "old_version_1" : "new_version_2",
                                        "old_version_2" : "new_version_2"
                                        }
        Returns:
            A pandas DataFrame with renamed columns.
        """

        # 1. Check if a list was passed
        if not rename_columns_dict:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if a column exist in the dataframe
        for col in rename_columns_dict:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Rename the columns
        df = df.rename(columns=rename_columns_dict)
        lg.info(f"Renaming the columns {rename_columns_dict} completed successfully.")
        return df

    @staticmethod
    def lowercase_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Lowercase the column names of the input dataframe.
        """
        lg.info("Converting all column names to lowercase.")
        df.columns = df.columns.str.lower()

        lg.info("Lowercasing column names completed successfully.")
        return df

    @staticmethod
    def strip_column_values(df: pd.DataFrame,
                            columns_strip_list: List[str] = None) -> pd.DataFrame:

        """
        Strip leading and trailing whitespace from string column values.

        Args:
            df:
                Input pandas DataFrame for processing
            columns_strip_list:
                A list with column names for processing.

        Returns:
             A pandas DataFrame with column values stripped from whitespace.
        """

        # 1. Check if a list was passed
        if not columns_strip_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if the columns exist
        for col in columns_strip_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Filter the list to only include columns that actually exist AND are objects/strings
        valid_string_cols = df[columns_strip_list].select_dtypes(include=['object', 'string']).columns
        lg.info(f"Valid string columns list: {valid_string_cols}.")

        # 4. Apply transformation only to valid columns
        lg.info("Stripping whitespace from column values.")
        for col in valid_string_cols:
            # Vectorized strip and replace
            df[col] = df[col].str.strip().replace('', np.nan)

        lg.info(f"Stripping column values in {columns_strip_list} completed successfully.")
        return df

    @staticmethod
    def replace_backslash(df: pd.DataFrame,
                          columns_replace_backslash_list: List[str] = None,
                          replace_with: str = '') -> pd.DataFrame:

        """
        1. Replace backslashes in the specified columns.
        2. Backslashes (\) are special characters in: JSON, CSV, SQL strings, Regex, File paths.

        Args:
            df:
                input dataframe to process
            columns_replace_backslash_list:
                A list with column names for processing.
            replace_with:
                A default value to replace backslashes with.

        Returns:
            A processed pandas DataFrame.
        """

        # 1. Check if a list was passed
        if not columns_replace_backslash_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check that the columns exists in the dataframe
        for col in columns_replace_backslash_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Replace backslashes with parameter `replace_with`
        # .astype(str) ensures the .str accessor always works
        # regex=False prevents Pandas from interpreting backslashes as regex escapes
        for col in columns_replace_backslash_list:
            df[col] = df[col].astype(str).str.replace("\\", replace_with, regex=False)

        lg.info(f"Backslash replacement in {columns_replace_backslash_list} completed successfully.")
        return df

    @staticmethod
    def escape_backslash(df: pd.DataFrame,
                         columns_escape_backslash_list: List[str] = None) -> pd.DataFrame:

        """
        1. Escape backslashes in the specified columns.
        2. Backslashes (\) are special characters in: JSON, CSV, SQL strings, Regex, File paths.
        3. Turn them into literal characters instead of letting them act as a control character.

        Args:
            df:
                input dataframe to process
            columns_escape_backslash_list:
                A list with columns for processing.

        Returns:
            A processed pandas DataFrame.
        """

        # 1. Check if a list was passed
        if not columns_escape_backslash_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check that the columns exists in the dataframe
        for col in columns_escape_backslash_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Escape backslashes with \\
        # .astype(str) ensures the .str accessor always works
        # regex=False prevents Pandas from interpreting backslashes as regex escapes
        for col in columns_escape_backslash_list:
            df[col] = df[col].astype(str).str.replace("\\", "\\\\", regex=False)

        lg.info(f"Escaping backslash in {columns_escape_backslash_list} completed successfully.")
        return df

    @staticmethod
    def sanitize_columns(df: pd.DataFrame,
                         columns_sanitize_list: List[str] = None,
                         replace_with=' ') -> pd.DataFrame:
        """
        Cleans string columns by replacing control characters (\r\n\t) with a placeholder
        and normalizing empty or null-like strings to actual Null values.

        Args:
            df:
                The DataFrame to process.
            columns_sanitize_list:
                List of column names to apply cleaning to.
            replace_with:
                The string to insert in place of newlines/tabs. Defaults to a single space.

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

        lg.info(f"Sanitizing columns {columns_sanitize_list} completed successfully.")
        return df

    @staticmethod
    def format_date_columns(df: pd.DataFrame,
                            columns_date_config_dict: Dict[str, str] = None) -> pd.DataFrame:
        """
        1. Convert date columns to datetime pandas objects.
        2. Convert the columns to the applied date format.

        Example: column_config: {'created_at': '%Y-%m-%d %H:%M:%S',
                                 'birth_date': '%Y-%m-%d'}

        Args:
            df:
                Input pandas DataFrame
            columns_date_config_dict:
                A dictionary with datetime columns for processing.

        Returns:
            A processed pandas DataFrame.
        """

        # 1. Check if a list was passed
        if not columns_date_config_dict:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if the columns exist
        for col in columns_date_config_dict.keys():
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Convert date/timestamp column to datetime object,
        # the corresponding format and replace empty strings with None
        for col, date_format in columns_date_config_dict.items():
            # Convert to datetime object
            df[col] = pd.to_datetime(df[col], errors='raise')

            # Convert to the new format
            df[col] = df[col].dt.strftime(date_format)

            # 3. Replace empty strings with None, so Postgres sees NULLs instead of ""
            df[col] = df[col].replace('', None)

        lg.info(f"Formating date columns {columns_date_config_dict} completed successfully.")
        return df

    @staticmethod
    def convert_columns_to_int(df: pd.DataFrame,
                               columns_int_list: list[str] = None,
                               ) -> pd.DataFrame:
        """
        1. Convert all columns in the given list to nullable Int64. Enforce early data corruption detection.
        2. Raise an error on non-integer floats or invalid strings.
        3. None / NaN values are preserved.

        Args:
            df:
                Input pandas DataFrame
            columns_int_list:
                A list with column for processing.

        Returns:
            A processed pandas DataFrame.
        """

        # 1. Check if a list was passed
        if not columns_int_list:
            return df

        # 2. Check if a column exist in the dataframe
        for col in columns_int_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Convert the columns to integers
        lg.info(f"Converting the columns in {columns_int_list} to integers.")
        for col in columns_int_list:

            # 4. Convert to numeric, raising an error for invalid strings
            numeric_series = pd.to_numeric(df[col], errors='raise')

            # 5. Ensure all non-NA values are whole numbers (e.g., 1.0 is okay, 1.3 is not)
            #    - .dropna(): Ignores None/NaN values so they don't interfere with the check
            #    - % 1 == 0: Vectorized check if values are whole numbers
            #    - .all(): Ensures **every single non-NA value** passes the check
            if not ((numeric_series.dropna() % 1) == 0).all():
                raise ValueError(f"Column '{col}' contains non-integer decimals.")

            # 6. Convert to nullable Int64
            df[col] = numeric_series.astype('Int64')

        lg.info(f"Conversion of columns {columns_int_list} to Int64 was successful.")
        return df

    @staticmethod
    def convert_columns_to_float(df: pd.DataFrame,
                                 columns_numeric_list: List[str] = None,
                                 ) -> pd.DataFrame:
        """
        1. Convert columns to nullable Float64.
        2. Whitespace/empty strings are treated as NaN.
        3. Actual data corruption (letters, etc.) still raises an error.

        Args:
            df:
                Input pandas DataFrame
            columns_numeric_list:
                A list with columns for processing.

        Returns:
            A processed pandas DataFrame.
        """

        # 1. Check if a list was passed
        if not columns_numeric_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check if a column exist in the dataframe
        for col in columns_numeric_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Convert the columns to floats
        lg.info(f"Converting the columns in {columns_numeric_list} to nullable Float64.")
        for col in columns_numeric_list:

            # 1. Replace empty strings or whitespace-only strings with np.nan
            # The regex ^\s*$ matches:
            # ^ : Start of string
            # \s*: Zero or more whitespace characters
            # $ : End of string
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)

            # 2. Convert to numeric (raises error for 'abc', etc.)
            # 3. Cast to nullable 'Float64'
            df[col] = pd.to_numeric(df[col], errors='raise').astype('Float64')

        lg.info(f"Conversion of columns {columns_numeric_list} to Float64 was successful.")
        return df

    @staticmethod
    def serialize_json_columns(df: pd.DataFrame,
                               columns_json_list: List[str] = None
                               ) -> pd.DataFrame:
        """
        Converts Python objects (dicts/lists) into valid JSON strings.

        Why:
        1. Pandas/Python defaults to single quotes {'a': 1} which Postgres rejects.
        2. json.dumps() forces double quotes {"a": 1} which Postgres requires.
        3. The check prevents double-stringifying columns that are already strings.
        """

        # 1. Check if a list was passed
        if not columns_json_list:
            lg.info("No columns provided. Skipping transformation.")
            return df

        # 2. Check that the columns exists in the dataframe
        for col in columns_json_list:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

        # 3. Serialize JSON
        for col in columns_json_list:
            lg.info(f"Serializing JSON for column: {col}")
            # json.dumps ensures double quotes are used: {"key": "value"}
            df[col] = df[col] = [json.dumps(x) if isinstance(x, (dict, list)) else x for x in df[col]]
            # ^ Explanation:
            # - If x is a dict/list: Convert to "{"proper": "json"}"
            # - If x is None/NaN: Leave as is so Postgres sees it as NULL
            # - If x is a string: Skip to avoid "{\"nested\": \"quotes\"}"

        lg.info(f"Serializing JSON columns {columns_json_list} completed successfully.")
        return df

    @staticmethod
    def check_non_null_columns(df: pd.DataFrame,
                               columns_non_null_list: List[str] = None
                               ) -> pd.DataFrame:
        """
        1. Check the column values for NULL values.
        2. Raises ValueError if any non-null columns contain NULLs.

        Args:
            df:
                Input pandas DataFrame
            columns_non_null_list:
                A list with column for processing.

        Returns:
            A processed pandas DataFrame.
        """

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

        lg.info(f"Checking columns {columns_non_null_list} for null values completed successfully.")
        return df

    @staticmethod
    def handle_duplicates(df: pd.DataFrame,
                          columns_unique_list: List[str] = None,
                          action='raise') -> pd.DataFrame:

        """
        1. Check if there are any duplicates based on the subset input. There are two options:
        2. Action 'raise' (default): Stop the pipeline if duplicates exist.
        3. Action 'drop': Remove them and keep the first occurrence.
        4. Raises ValueError if duplicated records were found in the unique list.

        Args:
            df:
                Input pandas DataFrame
            columns_unique_list:
                A list with column for processing.
            action:
                Behavior of the function, action='raise' (default) stops the ETL pipeline if duplicates are detected.
                action='drop' removes duplicates and keeps the first occurrence.

        Returns:
            A processed pandas DataFrame.
        """

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

        lg.info(f"Check for handling duplicates in {columns_unique_list} completed successfully.")
        return df

    @staticmethod
    def transform_dataframe(df: pd.DataFrame,
                            columns_str_dict: Dict[str, str] = None,
                            columns_lowercase: bool = True,
                            columns_strip_list: List[str] = None,
                            columns_replace_backslash_list: List[str] = None,
                            columns_escape_backslash_list: List[str] = None,
                            columns_sanitize_list: List[str] = None,
                            columns_date_config_dict: Dict[str, str] = None,
                            columns_int_list: List[str] = None,
                            columns_numeric_list: List[str] = None,
                            columns_json_list: List[str] = None,
                            columns_non_null_list: List[str] = None,
                            columns_unique_list: List[str] = None
                            ) -> pd.DataFrame:

        """
        A central function that applies transformations to a dataframe.

        1. Cleansing and standardizing.
        2. Pre-processing.
        3. Data integrity.
        4. Validation.

        Args:
            df: Input pandas DataFrame for processing.
            columns_str_dict: A dictionary with columns for renaming.
            columns_lowercase: A boolean operator that controls whether columns names should be lowercased.
            columns_strip_list: A list with column names for stripping whitespace.
            columns_replace_backslash_list: A list with column names for replacing backslashes.
            columns_escape_backslash_list: A list with column names for escaping backslashes.
            columns_sanitize_list: A list with column names for replacing control characters (\r\n\t) with a placeholder.
            columns_date_config_dict: A dictionary with date/timestamp columns for convertion in another date format.
            columns_int_list: A list with columns for conversion in Int64 format.
            columns_numeric_list: A list with columns for conversion in Float54 format.
            columns_json_list: A list with column names for converting dicts/lists into valid JSON strings.
            columns_non_null_list: A list with column names for detecting NULL values.
            columns_unique_list: A list with column names for detecting duplicated values.

        Returns:
            A processed pandas DataFrame.
        """

        # I. Clean and standardize.

        # 1. Rename columns
        if columns_str_dict:
            lg.info("Running transformation rename_columns.")
            df = EtlUtils.rename_columns(df=df, rename_columns_dict=columns_str_dict)

        # 2. Lowercase columns
        if columns_lowercase:
            lg.info("Running transformation lowercase_column_columns.")
            df = EtlUtils.lowercase_column_names(df=df)

        # II. Pre-Processing.

        # 3. Strip whitespace
        if columns_strip_list:
            lg.info("Running transformation strip_column_values.")
            df = EtlUtils.strip_column_values(df=df, columns_strip_list=columns_strip_list)

        # 4. Replace backslashes
        if columns_replace_backslash_list:
            lg.info("Running transformation replace_backslash.")
            df = EtlUtils.replace_backslash(df=df, columns_replace_backslash_list=columns_replace_backslash_list)

        # 5. Escape backslashes
        if columns_escape_backslash_list:
            lg.info("Running transformation escape_backslash.")
            df = EtlUtils.escape_backslash(df=df, columns_escape_backslash_list=columns_escape_backslash_list)

        # 6. Sanitize_columns
        if columns_sanitize_list:
            lg.info("Running transformation sanitize_columns.")
            df = EtlUtils.sanitize_columns(df=df, columns_sanitize_list=columns_sanitize_list)

        # III. Data Integrity.

        # 7. Format date columns
        if columns_date_config_dict:
            lg.info("Running transformation format_date_columns.")
            df = EtlUtils.format_date_columns(df=df, columns_date_config_dict=columns_date_config_dict)

        # 8. Convert columns to integer
        if columns_int_list:
            lg.info("Running transformation convert_columns_to_int.")
            df = EtlUtils.convert_columns_to_int(df=df, columns_int_list=columns_int_list)

        # 9. Convert columns to float
        if columns_numeric_list:
            lg.info("Running transformation convert_columns_to_float.")
            df = EtlUtils.convert_columns_to_float(df=df, columns_numeric_list=columns_numeric_list)

        # 10. Handle JSON Columns (Essential for Postgres)
        if columns_json_list:
            lg.info("Running transformation serialize_json_columns.")
            df = EtlUtils.serialize_json_columns(df=df, columns_json_list=columns_json_list)

        # IV. Validation.

        # 11. Check for NULLS
        if columns_non_null_list:
            lg.info("Running transformation check_non_null_columns.")
            df = EtlUtils.check_non_null_columns(df=df, columns_non_null_list=columns_non_null_list)

        # 12. Check for duplicates
        if columns_unique_list:
            lg.info("Running transformation handle_duplicates.")
            df = EtlUtils.handle_duplicates(df=df, columns_unique_list=columns_unique_list)

        return df

    def process_dataframe_date_ranges(self,
                                      df: pd.DataFrame,
                                      date_columns: List[str] = None,
                                      ) -> Tuple[pd.Timestamp, pd.Timestamp]:

        """
        1. Process CDC (change data capture) date columns in the source (e.g.
        columns source_updated_at, source_modified_at, source_created_at) to determine min and max values
        for the ETl Audit Manager.

        2. The date columns must not have NULL values.

        Args:
            df:
                Input pandas DataFrame
            date_columns:
                CDC date columns.

        Returns:
            A tuple of clamped pandas timestamps - clamped_data_min_date and clamped_data_max_date.
        """

        # 1. Check if the columns exist
        for col in date_columns:
            if col not in df.columns:
                raise KeyError(f"Column '{col}' not found.")

            # Raise an error if a NULL is found
            if df[col].isnull().any():
                raise ValueError(f"Data Quality Error: Column '{col}' contains null values.")

        # 2. Create lists with min and max timestamp values
        # For each date/timestamp column, find the min/max and append it to the list
        date_min_list = [pd.to_datetime(df[column], utc=True).min() for column in date_columns]
        lg.info(f"The date_min_list: {date_min_list}")

        date_max_list = [pd.to_datetime(df[column], utc=True).max() for column in date_columns]
        lg.info(f"The date_max_list: {date_max_list}")

        # 3. Calculate min and max values
        calculated_min_date = min(date_min_list)
        lg.info(f"Calculated_min_date: {calculated_min_date}")

        calculated_max_date = max(date_max_list)
        lg.info(f"Calculated_max_date: {calculated_max_date}")

        # 4. Normalize ETL window timestamps to UTC-aware timestamps
        sdt_utc = pd.to_datetime(self.sfc.etl_audit_manager.sdt, utc=True)
        lg.info(f"SDT_UTC: {sdt_utc}")

        edt_utc = pd.to_datetime(self.sfc.etl_audit_manager.edt, utc=True)
        lg.info(f"SDT_UTC: {edt_utc}")

        # 5. Protect the ETL audit layer from garbage dates, out‑of‑range dates, late‑arriving data, future timestamps
        # and timezone‑shifted values.

        # Determine the minimum/maximum timestamp from the corresponding list and sdt/edt in audit manager
        clamped_data_min_date = max(calculated_min_date, sdt_utc)  # max! not min
        lg.info(f"The clamped_data_min_date: {clamped_data_min_date}")

        clamped_data_max_date = min(calculated_max_date, edt_utc)  # min! not max
        lg.info(f"The clamped_data_max_date: {clamped_data_max_date}")

        # 6. Add the dates to the etl audit manager
        self.sfc.etl_audit_manager.data_min_date = clamped_data_min_date
        self.sfc.etl_audit_manager.data_max_date = clamped_data_max_date

        return clamped_data_min_date, clamped_data_max_date

    # Other functions for future implementation
    def set_comments(self):
        pass

    def check_source_date_range(self):
        pass

    def run_data_quality_check(self):
        pass

    def delete_target_dates(self):
        pass

    def set_reference_page(self):
        pass