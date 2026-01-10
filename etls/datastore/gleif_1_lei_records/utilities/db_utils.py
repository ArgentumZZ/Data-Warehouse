"""
db_utils.py

Purpose:
    Provide safe database execution helpers and wrappers for cursor operations.
"""

class DBUtils:
    """Database helper utilities."""

    @staticmethod
    def safe_execute(cursor, query, params=None):
        """
        Execute a SQL query safely.

        Args:
            cursor: Database cursor.
            query (str): SQL query.
            params (dict): Optional parameters.

        Raises:
            RuntimeError: If execution fails.
        """
        try:
            cursor.execute(query, params or {})
        except Exception as e:
            raise RuntimeError(f"Database error: {e}")
