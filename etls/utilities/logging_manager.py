# import libraries
import logging, os, sys
from datetime import datetime

# 1. # Create a single shared logger named "etl" that all modules will use
logger = logging.getLogger("etl")

# 2. Set the minimum severity level this logger will record (ignore DEBUG, keep INFO and above - WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)

# 3. Create a Console Stream Handler that prints log messages to the terminal
console_handler = logging.StreamHandler()

# 4. Formatter defines how console log lines should look (timestamp, level, file, line, message)
console_formatter = logging.Formatter(
    "[%(filename)s:%(lineno)d] - [%(asctime)s - %(levelname)s]:\n%(message)s\n",
    "%Y-%m-%d %H:%M:%S"
)

# 5. Apply the formatter to the console handler
console_handler.setFormatter(console_formatter)

# 6. Register the console handler with the shared "etl" logger
logger.addHandler(console_handler)

# 7. Create a log folder to populate with .logs files
# Get the absolute path of the script being executed (run_script.py)
# Result: .../financial_data_1_ethereum/script_runner/run_script.py
script_path = os.path.abspath(sys.argv[0])
print(f"Absolute path of run_script.py: {script_path}")

# 8. Go up ONE level to get the 'script_runner' folder
# Result: .../financial_data_1_ethereum/script_runner
script_runner_dir = os.path.dirname(script_path)
print(f"Path of script_runner folder: {script_runner_dir}")

# 9. Go up SECOND level to get the project root
# Result: .../project_name
project_root = os.path.dirname(script_runner_dir)
print(f"Path of project root: {project_root}")

# 10. Build the path to metadata/logs at the project root level
log_dir = os.path.join(project_root, "metadata", "logs")
print("Log dir:", log_dir)

# 11. Create directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Could use Path to re-do it
"""
from pathlib import Path

# Get the path of the running script, go up 2 levels (parent of parent)
# .parent = script_runner/
# .parent.parent = financial_data_1_ethereum/
project_root = Path(sys.argv[0]).resolve().parent.parent

log_dir = project_root / "metadata" / "logs"

# Ensure directory exists (converts Path object to string for logging module)
log_dir.mkdir(parents=True, exist_ok=True)
log_dir = str(log_dir)
"""

# 12. Build timestamped filename
log_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join(log_dir, f"{log_timestamp}_etl.log")

# 13. Make the path globally accessible in the module
current_log_path = log_file

# 14. Create a file handler in append mode
file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

# 15. Define log format:
# %(asctime)s -> timestamp
# %(levelname)s -> log level (INFO, ERROR, etc.)
# %(filename)s -> name of the Python file
# %(lineno)d -> line number where logger was called
# %(message)s -> actual log message

file_formatter = logging.Formatter(
    "[%(filename)s:%(lineno)d] - [%(asctime)s - %(levelname)s]:\n%(message)s\n",
    "%Y-%m-%d %H:%M:%S"
)

# 16. Attach the file formatter to the handler
file_handler.setFormatter(file_formatter)

# 17. Attach the configured handler to the "etl" logger
logger.addHandler(file_handler)


def cleanup_old_logs(log_dir: str, retention_number: int = 5, is_enabled: bool = True, mode: str = 'N') -> None:
    """
    Delete old logs based on selected mode and retention number.
    If mode = 'N', the function will delete logs after the N newest files.
    If mode = 'R', the function will delete logs based on age (older then X days).

    Args:
        log_dir: directory of the log folder
        retention_number: a number the controls how many files to delete
        is_enabled: a boolean value that determines whether the function will activate
        mode: a string value that determines what logic to use for the function

    Returns:
        None
    """

    # 1. Early exit if not enabled
    if not is_enabled:
        return

    # 2. Safety check
    if not os.path.exists(log_dir):
        print(f"Warning: Log directory {log_dir} does not exist. Skipping cleanup.")
        return

    # 3. List all log files in the directory
    # Expected output:
    # ['C:/.../2026-01-10_12-00-00_etl.log', 'C:/.../2026-01-11_09-30-22_etl.log', ...]
    files = [
        os.path.join(log_dir, f)
        for f in os.listdir(log_dir)
        if os.path.isfile(os.path.join(log_dir, f)) and f.endswith("_etl.log")
    ]

    # 4. Delete logs based on N newest files.
    if mode == 'N':
        """
        Delete logs after N newest files.
        """

        # 4.1. Sort files by modification time (oldest first, newest file last)
        files.sort(key=lambda f: os.path.getmtime(f))

        # 4.2. If we have more than retention_number files, delete the oldest ones
        while len(files) > retention_number:
            old_file = files.pop(0)
            try:
                os.remove(old_file)
            except Exception as ex:
                print(f"Failed to delete old log {old_file}: {ex}")

    # 5. Delete logs based on age
    elif mode == 'R':
        """
        Delete based on age (older than X days).
        """

        now = datetime.now().timestamp()

        for file_path in files:

            # 5.1. Calculate the file modification time (e.g., 1768237278.4399505)
            file_modification_time = os.path.getmtime(file_path)

            # 5.2. Calculate the age of the file in days (there are 86400 seconds in a day)
            age_in_days = (now - file_modification_time) / 86400

            # 5.3. If a file is older than the retention period, delete it
            if age_in_days > retention_number:
                try:
                    # 5.4. Remove the file at the given path
                    os.remove(file_path)
                except Exception as exc:
                    # 5.5. If a deletion fails, log the error
                    print(f"Failed to delete old log {file_path}: {exc}")


def get_current_log_content():
    """
    Reads the content of the current log file.
    """
    try:
        if os.path.exists(current_log_path):
            with open(current_log_path, 'r', encoding='utf-8') as f:
                return f.read()
    except Exception as ex:
        return f"Could not read log file: {ex}"
    return "Log file not found."


def get_current_log_size() -> int:
    """
    1. Returns the current size of the log file in bytes.
    2. Acts as a pointer for the start of a task.

    Returns: bytes size
    """
    try:
        # Check if current_log_path is defined and the file exists
        if 'current_log_path' in globals() and os.path.exists(current_log_path):
            return os.path.getsize(current_log_path)

    except Exception as e:
        print(f"Error getting log size: {e}")
    return 0


def get_logs_from_position(position: int) -> str:
    """
    1. Reads and returns the log content from a specific byte offset to the end.
    2. This isolates the logs for a specific task.
    """
    try:
        if 'current_log_path' in globals() and os.path.exists(current_log_path):
            with open(current_log_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Move the 'cursor' to the starting position
                f.seek(position)
                # Read everything from that point onwards
                return f.read()
    except Exception as e:
        return f"Could not retrieve task-specific logs: {e}"
    return ""


def _log(func, *args, **kwargs):
    """
    Core logging helper that cleans up logging:
        - Automatically attaches traceback information when logging from inside an 'exception' block (adds exc_info)
        - Ensures logs records point to the actual called (not this helper)
        - Passes the call to the underlying logging method (logger.info, logger.error , etc.)
    """

    # 1. Detect whether we're currently inside an exception handler (except block). If so, include the error trace
    # sys.exc_info()[0] is non‑None only when an exception is actively being handled.
    # If the caller didn't explicitly pass exc_info=True (and we are inside an exception handler),
    # then we set it automatically.

    # This allows us to write:
    #   lg.info("The task failed")
    # instead of:
    #   lg.info("The task failed", exc_info=True)
    if 'exc_info' not in kwargs and sys.exc_info()[0]:
        kwargs['exc_info'] = True


    # 2. Without a stacklevel, logs would point to logging_manager.py instead of the real source of the log.
    # We want to see run_script.py:160 instead of logging_manager.py:123

    # stacklevel=3 means:
    # level 1 → inside logging module (logger.info())
    # level 2 → inside this helper (_log())
    # level 3 → the real caller (lg.info())
    # user code
    #   → lg.info()
    #       → _log()
    #         → logger.info()
    kwargs.setdefault('stacklevel', 3)

    # 3. Call the actual logging function with the modified arguments.
    # func is one of: logger.info, logger.warning, logger.error, logger.debug, logger.critical
    func(*args, **kwargs)


# These are convenience wrappers around the real Logging.logger methods.
# Provide a clean, consistent interface (e.g., lg.info())
# Route all logging calls though _log() to handle:
#   - traceback behavior
#   - fix caller file/line attribution via stacklevel

# This allows us to write:
#    lg.info("Starting ETL run")
# instead of:
#     logger.info("Starting ETL run", stacklevel=3, exc_info=True)

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

# Special case: logger.exception already forces exc_info=True,
# so we call it directly without going through _log() to avoid:
#   - duplicated tracebacks
#   - conflicting exc_info logic
# This allows us to write:
#   try:
#       ...
#   except Exception:
#       lg.exception("Unexpected error")
def exception(msg, *args, **kwargs):
    logger.exception(msg, *args, **kwargs)