"""
error_utils.py

Purpose:
    Define custom exception classes for ETL-specific error handling.
    These exceptions provide semantic clarity for common ETL failure modes,
    including retry logic, data validation, configuration issues, and
    external system failures.
"""


# ---------------------------------------------------------------------------
# Base classes
# ---------------------------------------------------------------------------

class ETLError(Exception):
    """Base class for ETL-related errors."""
    pass


class RetryableError(ETLError):
    """Error type indicating the operation can be retried."""
    pass


# ---------------------------------------------------------------------------
# Configuration & environment errors
# ---------------------------------------------------------------------------

class ConfigurationError(ETLError):
    """Raised when ETL configuration is missing, invalid, or inconsistent."""
    pass


class EnvironmentError(ETLError):
    """Raised when required environment variables or system resources are missing."""
    pass


# ---------------------------------------------------------------------------
# Data-related errors
# ---------------------------------------------------------------------------

class DataValidationError(ETLError):
    """Raised when input data fails validation rules."""
    pass


class SchemaMismatchError(ETLError):
    """Raised when data does not match the expected schema."""
    pass


class MissingDataError(ETLError):
    """Raised when required data is missing or incomplete."""
    pass


# ---------------------------------------------------------------------------
# File & path errors
# ---------------------------------------------------------------------------

class FileNotFoundErrorETL(ETLError):
    """Raised when an expected file is missing."""
    pass


class DirectoryNotFoundErrorETL(ETLError):
    """Raised when an expected directory is missing."""
    pass


class FilePermissionError(ETLError):
    """Raised when file or directory permissions prevent an operation."""
    pass


# ---------------------------------------------------------------------------
# External system errors
# ---------------------------------------------------------------------------

class ExternalServiceError(ETLError):
    """Raised when an external API, database, or service fails."""
    pass


class NetworkError(RetryableError):
    """Raised when a network operation fails but may succeed on retry."""
    pass


class TimeoutErrorETL(RetryableError):
    """Raised when an operation times out but may succeed on retry."""
    pass


# ---------------------------------------------------------------------------
# Execution & orchestration errors
# ---------------------------------------------------------------------------

class StepExecutionError(ETLError):
    """Raised when an ETL step fails unexpectedly."""
    pass


class DependencyError(ETLError):
    """Raised when a required dependency or upstream step is missing or failed."""
    pass


class PipelineHaltError(ETLError):
    """Raised to intentionally stop the pipeline due to a critical condition."""
    pass
