"""
date_utils.py

Purpose:
    Provide helpers for working with dates and timestamps, including:
        - Getting today's date
        - Adding/subtracting days
"""

from datetime import datetime, timedelta


def today():
    """
    Return today's date.

    Returns:
        date: Current date.
    """
    return datetime.now().date()


def add_days(date, days):
    """
    Add or subtract days from a date.

    Args:
        date (date): Base date.
        days (int): Number of days to add (negative allowed).

    Returns:
        date: Adjusted date.
    """
    return date + timedelta(days=days)
