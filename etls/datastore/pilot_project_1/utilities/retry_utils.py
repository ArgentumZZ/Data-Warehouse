"""
retry_utils.py

Purpose:
    Provide retry decorators for database/API operations.
"""

import time


def retry(times=3, delay=1):
    """
    Retry a function multiple times with delay.

    Args:
        times (int): Number of attempts.
        delay (int): Delay between attempts in seconds.

    Returns:
        function: Wrapped function.
    """
    def wrapper(func):
        def inner(*args, **kwargs):
            for attempt in range(times):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    if attempt == times - 1:
                        raise
                    time.sleep(delay)
        return inner
    return wrapper
