"""
dataframe_utils.py

Purpose:
    Provide helpers for working with pandas DataFrames, including:
        - Normalizing column names
        - Type conversions
"""

def normalize_columns(df):
    """
    Normalize DataFrame column names to lowercase snake_case.

    Args:
        df (DataFrame): Input DataFrame.

    Returns:
        DataFrame: Updated DataFrame.
    """
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df
