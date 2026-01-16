# ---------------------------------------------------------------------------
# Custom exception classes that may user for ETL-specific and
# data-warehouse-specific error handling
# ---------------------------------------------------------------------------


class ETLError(Exception):
    """
    Base class for ETL-related errors.

    When to raise:
        - Any ETL failure that does not fit a more specific category.
        - A generic catch-all for unexpected pipeline issues.
    """
    pass


class RetryableError(ETLError):
    """
    Error type indicating the operation can be retried.

    When to raise:
        - Transient network failures.
        - Temporary unavailability of external systems.
        - Timeouts that may succeed on retry.
    """
    pass


# ---------------------------------------------------------------------------
# Configuration & environment errors
# ---------------------------------------------------------------------------

class ConfigurationError(ETLError):
    """
    Raised when ETL configuration is missing, invalid, or inconsistent.

    When to raise:
        - Missing required config keys.
        - Invalid types or values in configuration.
        - Conflicting or mutually exclusive settings.
    """
    pass


class EnvironmentErrorETL(ETLError):
    """
    Raised when required environment variables or system resources are missing.

    When to raise:
        - Missing environment variables.
        - Missing credentials or secrets.
        - OS-level limitations preventing execution.
    """
    pass


# ---------------------------------------------------------------------------
# Data-related errors
# ---------------------------------------------------------------------------

class DataValidationError(ETLError):
    """
    Raised when input data fails validation rules.

    When to raise:
        - Invalid values, formats, or ranges.
        - Failed business rules.
        - Nulls in required fields.
    """
    pass


class SchemaMismatchError(ETLError):
    """
    Raised when data does not match the expected schema.

    When to raise:
        - Missing columns.
        - Unexpected columns.
        - Wrong data types.
        - Schema drift detected.
    """
    pass


class MissingDataError(ETLError):
    """
    Raised when required data is missing or incomplete.

    When to raise:
        - Required file/table/record not found.
        - Lookup table missing keys.
        - Foreign key references missing.
    """
    pass


# ---------------------------------------------------------------------------
# File & path errors
# ---------------------------------------------------------------------------

# Use FileNotFoundError: when an expected file is missing, input file isn't found or expected output file missing after a step.


class DirectoryNotFound(ETLError):
    """
    Raised when an expected directory is missing.

    When to raise:
        - Landing/staging/output directory missing.
        - Required folder structure not present.
    """
    pass


class FilePermissionError(ETLError):
    """
    Raised when file or directory permissions prevent an operation.

    When to raise:
        - Cannot read/write/delete due to permissions.
        - OS denies access to a path.
    """
    pass


# ---------------------------------------------------------------------------
# External system errors
# ---------------------------------------------------------------------------

class ExternalServiceError(ETLError):
    """
    Raised when an external API, database, or service fails.

    When to raise:
        - API returns an error response.
        - Database connection fails.
        - External dependency unavailable.
    """
    pass


class NetworkError(RetryableError):
    """
    Raised when a network operation fails but may succeed on retry.

    When to raise:
        - DNS failures.
        - Connection resets.
        - Temporary connectivity issues.
    """
    pass


# ---------------------------------------------------------------------------
# Execution & orchestration errors
# ---------------------------------------------------------------------------

class StepExecutionError(ETLError):
    """
    Raised when an ETL step fails unexpectedly.

    When to raise:
        - A transformation or load step crashes.
        - A subprocess returns a non-zero exit code.
    """
    pass


class DependencyError(ETLError):
    """
    Raised when a required dependency or upstream step is missing or failed.

    When to raise:
        - Upstream step did not produce expected output.
        - Required job did not run or failed.
    """
    pass


class PipelineHaltError(ETLError):
    """
    Raised to intentionally stop the pipeline due to a critical condition.

    When to raise:
        - Business rule requires halting the pipeline.
        - Safety condition triggered.
    """
    pass
