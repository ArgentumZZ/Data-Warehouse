"""
validator.py

Purpose:
    Provide validation helpers for configuration and runtime checks.
"""

class Validator:
    """Validation utilities."""

    @staticmethod
    def require(value, name):
        """
        Ensure a configuration value is present.

        Args:
            value (any): Value to check.
            name (str): Name of the config field.

        Raises:
            RuntimeError: If value is None.
        """
        if value is None:
            raise RuntimeError(f"Missing required configuration: {name}")
