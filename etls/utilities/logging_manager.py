# import libraries
import logging, os, sys
from datetime import datetime

# 1. # Create a single shared logger named "etl" that all modules will use
logger = logging.getLogger("etl")

# 2. Set the minimum severity level this logger will record (ignore DEBUG, keep INFO and above - WARNING, ERROR, CRITICAL)
logger.setLevel(logging.INFO)

# 3. Add handlers if none exist
if not logger.handlers:

    # 4.Create a Console Stream Handler that prints log messages to the terminal
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

    # 8. Create a log folder to populate with .logs files

    # 9. Get the absolute path of the script being executed (run_script.py)
    # Result: .../financial_data_1_ethereum/script_runner/run_script.py
    script_path = os.path.abspath(sys.argv[0])
    print(f"Absolute path of run_script.py: {script_path}")

    # 10. Go up ONE level to get the 'script_runner' folder
    # Result: .../financial_data_1_ethereum/script_runner
    script_runner_dir = os.path.dirname(script_path)
    print(f"Path of script_runner folder: {script_runner_dir}")

    # 11. Go up SECOND level to get the project root
    # Result: .../project_name
    project_root = os.path.dirname(script_runner_dir)
    print(f"Path of project root: {project_root}")

    # 12. Build the path to metadata/logs at the project root level
    log_dir = os.path.join(project_root, "metadata", "logs")
    print("Log dir:", log_dir)

    # 13. Create directory if it doesn't exist
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


    # 14. Build timestamped filename
    log_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"{log_timestamp}_etl.log")

    # make the path globally accessible in the module
    current_log_path = log_file

    # 15. Delete .logs files based on number of runs (keep the N most recent).
    max_runs = 5

    # 16. List all log files in the directory
    files = [
        os.path.join(log_dir, f)
        for f in os.listdir(log_dir)
        if os.path.isfile(os.path.join(log_dir, f))
    ]

    # Expected output example:
    # ['C:/.../2026-01-10_12-00-00_etl.log', 'C:/.../2026-01-11_09-30-22_etl.log', ...]

    # 17. Sort files by modification time (oldest first, newest file last)
    files.sort(key=lambda f: os.path.getmtime(f))

    # 18. If we have more than max_runs files, delete the oldest ones
    while len(files) > max_runs:
        old_file = files.pop(0)
        try:
            os.remove(old_file)
        except Exception as e:

            print(f"Failed to delete old log {old_file}: {e}")

    # 19. Create file handler in append mode
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

    # 20. Define log format:
    # %(asctime)s -> timestamp
    # %(levelname)s -> log level (INFO, ERROR, etc.)
    # %(filename)s -> name of the Python file
    # %(lineno)d -> line number where logger was called
    # %(message)s -> actual log message

    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d]: %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    # 21. Attach the file formatter to the handler
    file_handler.setFormatter(file_formatter)

    # 22. Attach the configured handler to the "etl" logger
    logger.addHandler(file_handler)

def get_current_log_content():
    """
    Reads the content of the current log file.
    """
    try:
        if os.path.exists(current_log_path):
            with open(current_log_path, 'r', encoding='utf-8') as f:
                return f.read()
    except Exception as e:
        return f"Could not read log file: {e}"
    return "Log file not found."


def _log(func, *args, **kwargs):
    """
    # Core helper that cleans up logging:
    # - Adds exc_info automatically inside exception handlers
    # - Ensures logs point to the real caller via stacklevel
    # - Passes everything through to the underlying logging function
    """

    # Detect whether we're currently inside an exception handler (except block). If so, include the error trace
    # sys.exc_info()[0] is non‑None only when an exception
    # is actively being handled. # If the caller did NOT explicitly pass exc_info=True, we set it automatically.
    if 'exc_info' not in kwargs and sys.exc_info()[0]:
        kwargs['exc_info'] = True

    # Ensure correct file/line attribution in logs.
    # stacklevel=3 means:
    # level 1 → inside logging module
    # level 2 → inside this helper
    # level 3 → the *real* caller (the user’s code)
    # This prevents logs from pointing to this wrapper instead of the true source.
    kwargs.setdefault('stacklevel', 3)

    # Call the actual logging function (e.g., logger.info, logger.error, etc.) with the modified arguments.
    func(*args, **kwargs)


# These are convenience wrappers around the real logger methods.
# Each one forwards the message to `_log()`, which adds smart behavior:
# - automatic exc_info when inside an exception handler
# - correct stacklevel so logs point to the real caller
# This let us write: lg.info("message") instead of logger.info(...)
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
# so we call it directly without going through _log().
def exception(msg, *args, **kwargs):
    logger.exception(msg, *args, **kwargs)

##################################################################################################################
# Code for delete _etl.log files older than X days
######################################################################################################################
    '''
    retention_days = 7
    now = datetime.now().timestamp()

    for file_name in os.listdir(log_dir):
        # file_name: 2026-01-12_18-44-30_etl.log
        # print(f"The file name: {file_name}")'''

        # file_path:  C:\Users\Mihail\PycharmProjects\datawarehouse\etls\datastore\gleif_1_lei_records\metadata\logs\2026-01-12_18-44-30_etl.log
    '''file_path = os.path.join(log_dir, file_name)
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
    '''
######################################################################################################################