# import libraries
import logging, os, sys
from datetime import datetime

# 1. # Create (or retrieve) a single shared logger named "etl" that all modules will use
logger = logging.getLogger("etl")

# 2. Set the minimum severity level this logger will record (ignore DEBUG, keep INFO and above - WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)

# 3. Add handlers if none exist
if not logger.handlers:

    # 4. Console handler (StreamHandler)
    # Create a handler that prints log messages to the terminal
    console_handler = logging.StreamHandler()

    # 5. Formatter defining how console log lines should look (timestamp, level, file, line, message)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d]: %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # 6. Apply the formatter to the console handler
    console_handler.setFormatter(console_formatter)

    # 7. Register the console handler with the shared "etl" logger
    logger.addHandler(console_handler)

    # 8. File handler (FileHandler)

    # os.path.dirname(path) returns the folder that contains the given path.
    # If input: C:/project/utilities/logging_manager.py, then output: C:/project/utilities
    # 9. Determine the absolute path of the folder containing this file (utilities/)
    current_dir = os.path.dirname(os.path.abspath(__file__))     # .../utilities
    project_dir = os.path.dirname(current_dir)                  # .../project

    # 10. Build the path to the shared logs directory: project/metadata/logs and build a logs folder
    log_dir = os.path.join(project_dir, "metadata", "logs")  # .../metadata/logs

    print("Current working directory:", os.getcwd())
    print("Log dir:", log_dir)

    # Create directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # 11. Build timestamped filename
    log_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"{log_timestamp}_etl.log")

    #################################################################
    # Delete _etl.log files older than 7 days
    retention_days = 7
    now = datetime.now().timestamp()

    for file_name in os.listdir(log_dir):
        # file_name: 2026-01-12_18-44-30_etl.log
        # print(f"The file name: {file_name}")

        # file_path:  C:\Users\Mihail\PycharmProjects\datawarehouse\etls\datastore\gleif_1_lei_records\metadata\logs\2026-01-12_18-44-30_etl.log
        file_path = os.path.join(log_dir, file_name)
        # print(f"The file path: {file_path}")

        # Skip anything that isn't a file
        if not os.path.isfile(file_path):
            continue

        # SAFETY CHECK — only delete ETL log files
        # Expected: True for "2026-01-12_18-44-30_etl.log"
        # Expected: False for "readme.txt", "temp.tmp", "config.json"
        if not file_name.endswith("_etl.log"):
            continue

        # Get file modification time
        file_modification_time = os.path.getmtime(file_path)
        # file_modification_time: 1768237278.4399505
        # print(f"The file modification time: {file_modification_time}")

        # If file is older than retention period, delete it
        age_in_days = (now - file_modification_time) / 86400    # 86400 seconds in a day

        if age_in_days > retention_days:
            try:
                # Removes the file at the given path
                os.remove(file_path)
            except Exception as e:
                # If deletion fails, log the error but continue
                print(f"Failed to delete old log {file_path}: {e}")
    #################################################################


    # 12. Create file handler
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

    # Define log format:
    # %(asctime)s -> timestamp
    # %(levelname)s -> log level (INFO, ERROR, etc.)
    # %(filename)s -> name of the Python file
    # %(lineno)d -> line number where logger was called
    # %(message)s -> actual log message
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d]: %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # 13. Attach the file formatter to the handler
    file_handler.setFormatter(file_formatter)

    # 14. Attach the configured handler to the "etl" logger
    logger.addHandler(file_handler)


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



"""
# -----------------------------------------
# Delete old logs based on number of runs
# Keep only the most recent N log files
# Expected output: deletes oldest files if count exceeds limit
# -----------------------------------------
MAX_RUNS = 7  # or 30

# List all log files in the directory
files = [
    os.path.join(log_dir, f)
    for f in os.listdir(log_dir)
    if os.path.isfile(os.path.join(log_dir, f))
]

# Expected output example:
# ['C:/.../2026-01-10_12-00-00_etl.log', 'C:/.../2026-01-11_09-30-22_etl.log', ...]

# Sort files by modification time (oldest first)
files.sort(key=lambda f: os.path.getmtime(f))

# Expected output example:
# Oldest file first, newest file last

# If we have more than MAX_RUNS files, delete the oldest ones
while len(files) > MAX_RUNS:
    old_file = files.pop(0)
    try:
        os.remove(old_file)
        # Expected output example:
        # Deleted old log: C:/.../2026-01-10_12-00-00_etl.log
    except Exception as e:
        # Expected output example:
        # Failed to delete old log C:/...: [WinError 5] Access is denied
        print(f"Failed to delete old log {old_file}: {e}")


"""