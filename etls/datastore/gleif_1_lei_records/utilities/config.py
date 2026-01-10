"""
config.py

Purpose:
    Provide a simple, centralized interface for reading environment variables.
    This module ensures consistent access to environment-driven configuration
    across the ETL framework.
"""

import os


class Config:
    """Utility class for accessing environment variables."""

    @staticmethod
    def get_env(name, default=None):
        """
        Return the value of an environment variable.

        Args:
            name (str): Name of the environment variable.
            default (any): Value to return if variable is not set.

        Returns:
            str | any: The environment variable value or the default.
        """
        return os.environ.get(name, default)
