"""
error_utils.py

Purpose:
    Define custom exception classes for ETL-specific error handling.
"""

class ETLError(Exception):
    """Base class for ETL-related errors."""
    pass


class RetryableError(ETLError):
    """Error type indicating the operation can be retried."""
    pass
