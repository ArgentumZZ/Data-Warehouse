"""
timer.py

Purpose:
    Provide a simple execution timer for measuring task durations.
"""

import time


class Timer:
    """Simple stopwatch-style timer."""

    def __init__(self):
        self.start_time = None

    def start(self):
        """
        Start the timer.
        """
        self.start_time = time.time()

    def stop(self):
        """
        Stop the timer and return elapsed seconds.

        Returns:
            float: Elapsed time in seconds.
        """
        return time.time() - self.start_time
