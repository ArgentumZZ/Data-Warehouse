# ===========================================================
# Date and timestamp helpers that may be implemented
# ===========================================================
from datetime import datetime, timedelta


def today():
    """
    Return today's date.

    Returns:
        date: Current date.
    """
    return datetime.now().date()

def yesterday(self):
    """Return yesterday's date."""
    pass

def add_days(base_date, days):
    """
    Add or subtract days from a date.

    Args:
        base_date: Base date.
        days: Number of days to add (negative allowed).

    Returns:
        date: Adjusted date.
    """
    return base_date + timedelta(days=days)