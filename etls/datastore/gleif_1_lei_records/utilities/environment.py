"""
execution_metadata.py

Purpose:
    Store metadata about the current ETL execution, such as timestamps,
    record counts, and audit information. This module provides a simple
    dictionary-like interface for tracking execution state.
"""


class ExecutionMetadata:
    """Container for ETL execution metadata."""

    def __init__(self):
        self.data = {}

    def set(self, key, value):
        """
        Store a metadata value.

        Args:
            key (str): Metadata key.
            value (any): Metadata value.
        """
        self.data[key] = value

    def get(self, key, default=None):
        """
        Retrieve a metadata value.

        Args:
            key (str): Metadata key.
            default (any): Fallback value.

        Returns:
            any: Stored value or default.
        """
        return self.data.get(key, default)
