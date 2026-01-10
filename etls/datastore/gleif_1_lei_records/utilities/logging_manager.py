# logging_manager.py

import logging
import sys

# 1. Create a global logger
# Get (or create) the shared ETL logger
logger = logging.getLogger("etl")

# Only log INFO and above (INFO, WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)

# 2. Add a console handler if none exists
if not logger.handlers:
    # Create a handler that sends log output to the terminal (stdout/stderr)
    h = logging.StreamHandler()  # output to console

    # Define log format:
    # %(asctime)s -> timestamp
    # %(levelname)s -> log level (INFO, ERROR, etc.)
    # %(filename)s -> name of the Python file
    # %(lineno)d -> line number where logger was called
    # %(message)s -> actual log message
    h.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d]: %(message)s",
        "%Y-%m-%d %H:%M:%S"
    ))

    # Attach the configured handler to the "etl" logger
    logger.addHandler(h)


# ---------------------------------------------------------
# 3. Smart Internal Helper
# ---------------------------------------------------------
def _log(func, *args, **kwargs):
    """
    Core wrapper to handle automatic traceback detection and
    correct line-number reporting.
    """
    # Auto-detect if we are in an 'except' block; if so, include the error trace
    if 'exc_info' not in kwargs and sys.exc_info()[0]:
        kwargs['exc_info'] = True

    # stacklevel=3 tells Python to ignore this helper and the calling function
    # when looking for the filename/line number, pointing to the real source.
    kwargs.setdefault('stacklevel', 3)

    func(*args, **kwargs)


# ---------------------------------------------------------
# 4. Shorthand API
# ---------------------------------------------------------
# These allow for calling 'lg.info("msg")' instead of the long-form logger version
def info(msg, *args, **kwargs):
    _log(logger.info, msg, *args, **kwargs)

def error(msg, *args, **kwargs):
    _log(logger.error, msg, *args, **kwargs)

def warning(msg, *args, **kwargs):
    _log(logger.warning, msg, *args, **kwargs)

def debug(msg, *args, **kwargs):
    _log(logger.debug, msg, *args, **kwargs)

def critical(msg, *args, **kwargs):
    _log(logger.critical, msg, *args, **kwargs)

# Standard exception logger (always includes traceback)
def exception(msg, *args, **kwargs):
    logger.exception(msg, *args, **kwargs)