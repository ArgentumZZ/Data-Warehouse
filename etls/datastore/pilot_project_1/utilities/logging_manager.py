# logging_manager.py

import logging
import sys

# -----------------------------
# 1. Create a global logger
# -----------------------------
# - Name it "etl" for consistency across ETL scripts
# - Set default logging level to INFO
logger = logging.getLogger("etl")
logger.setLevel(logging.INFO)

# -----------------------------
# 2. Add a console handler if none exists
# -----------------------------
if not logger.handlers:
    h = logging.StreamHandler()  # output to console
    # Define log format:
    # %(asctime)s -> timestamp
    # %(levelname)s -> log level (INFO, ERROR, etc.)
    # %(filename)s -> name of the Python file
    # %(lineno)d -> line number where logger was called
    # %(message)s -> actual log message
    h.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        "%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(h)

# -----------------------------
# 3. Internal helper function
# -----------------------------
def _log(func, *args, **kwargs):
    """
    Wrapper around logging functions to automatically include traceback
    if an exception is currently active.
    """
    # If exc_info not explicitly provided and an exception is active, add it
    if 'exc_info' not in kwargs and sys.exc_info()[0]:
        kwargs['exc_info'] = True
    # Call the original logging method (info, error, etc.)
    func(*args, **kwargs)

# -----------------------------
# 4. Module-level logging functions
# -----------------------------
# These functions allow you to do: lg.info("message") directly

def info(*args, **kwargs):      _log(logger.info, *args, **kwargs)
def error(*args, **kwargs):     _log(logger.error, *args, **kwargs)
def warning(*args, **kwargs):   _log(logger.warning, *args, **kwargs)
def debug(*args, **kwargs):     _log(logger.debug, *args, **kwargs)
def critical(*args, **kwargs):  _log(logger.critical, *args, **kwargs)

# exception() always prints the traceback by default
def exception(*args, **kwargs): logger.exception(*args, **kwargs)
