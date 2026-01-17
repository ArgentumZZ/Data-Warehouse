"""
file_utils.py

Purpose:
    Provide helpers for working with files and directories, including:
        - Creating nested folder structures
        - Generating random directory names
        - Normalizing paths
        - Reading/writing files safely
        - Checking file/directory existence
        - Removing files/directories
        - Listing directory contents
"""

import os
import utilities.logging_manager as lg
from datetime import datetime

# ---------------------------------------------------------------------------
# Existing functions
# ---------------------------------------------------------------------------

def build_output_file_path(table: str) -> str:
    """
    A utility function that creates an output folder to store CSV files from each project's run.

    Arg:
        table: Name of the table (the project's table)

    Returns:
        A string file path.
    """

    # 1. Find the current directory of the file (where this function is run):
    # e.g. C:\Users\Mihail\PycharmProjects\datawarehouse\etls\datastore\financial_data_1_ethereum\script_factory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    lg.info(f"Current Directory: {current_dir}")

    # 2. Parent directory (e.g. C:\Users\Mihail\PycharmProjects\datawarehouse\etls\datastore\financial_data_1_ethereum)
    parent_current_dir = os.path.dirname(current_dir)
    lg.info(f"Parent Current Directory: {parent_current_dir}")

    # 3. Build the path to the output directory: project/metadata/output and build an output folder
    output_dir = os.path.join(parent_current_dir, "metadata", "output")
    lg.info(f"Output directory: {output_dir}")

    # 4. Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # 5. Build a timestamped filename
    # e.g. project/metadata/output/ethereum_2026-01-13-18:00:00.csv
    file_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(output_dir, f"{table}_{file_timestamp}.csv")
    return file_path

# ---------------------------------------------------------------------------
# 2. File utilities
# ---------------------------------------------------------------------------

def normalize(path):
    """
    Normalize a filesystem path by expanding variables, user home,
    resolving relative components, and converting to an absolute path.

    Args:
        path (str): Input path.

    Returns:
        str: Normalized absolute path.
    """
    path = os.path.expandvars(path)
    path = os.path.expanduser(path)
    path = os.path.normpath(path)
    return os.path.abspath(path)


def join(*parts):
    """
    Join path components and normalize the result.

    Returns:
        str: Normalized absolute path.
    """
    return normalize(os.path.join(*parts))


def ensure_dir(path):
    """
    Ensure that a directory exists. Creates it if missing.

    Args:
        path (str): Directory path.

    Returns:
        str: Normalized directory path.
    """
    path = normalize(path)
    os.makedirs(path, exist_ok=True)
    return path


def file_exists(path):
    """Return True if the path points to an existing file."""
    return os.path.isfile(normalize(path))


def dir_exists(path):
    """Return True if the path points to an existing directory."""
    return os.path.isdir(normalize(path))


def remove_file(path):
    """
    Remove a file if it exists.

    Args:
        path (str): File path.
    """
    path = normalize(path)
    if os.path.isfile(path):
        os.remove(path)


def remove_dir(path):
    """
    Remove a directory if it exists and is empty.

    Args:
        path (str): Directory path.
    """
    path = normalize(path)
    if os.path.isdir(path):
        os.rmdir(path)


def list_files(path):
    """
    List files in a directory (non-recursive).

    Args:
        path (str): Directory path.

    Returns:
        list[str]: Filenames only.
    """
    path = normalize(path)
    if not os.path.isdir(path):
        return []
    return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]


def list_dirs(path):
    """
    List subdirectories in a directory (non-recursive).

    Args:
        path (str): Directory path.

    Returns:
        list[str]: Directory names only.
    """
    path = normalize(path)
    if not os.path.isdir(path):
        return []
    return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]


def read_text(path, encoding="utf-8"):
    """
    Read a text file safely.

    Args:
        path (str): File path.
        encoding (str): Text encoding.

    Returns:
        str: File contents or empty string if missing.
    """
    path = normalize(path)
    if not os.path.isfile(path):
        return ""
    with open(path, "r", encoding=encoding) as f:
        return f.read()


def write_text(path, content, encoding="utf-8"):
    """
    Write text to a file, creating parent directories if needed.

    Args:
        path (str): File path.
        content (str): Text to write.
        encoding (str): Text encoding.
    """
    path = normalize(path)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding=encoding) as f:
        f.write(content)


def append_text(path, content, encoding="utf-8"):
    """
    Append text to a file, creating parent directories if needed.

    Args:
        path (str): File path.
        content (str): Text to append.
        encoding (str): Text encoding.
    """
    path = normalize(path)
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding=encoding) as f:
        f.write(content)


def file_size(path):
    """
    Return file size in bytes, or 0 if missing.

    Args:
        path (str): File path.

    Returns:
        int: Size in bytes.
    """
    path = normalize(path)
    if os.path.isfile(path):
        return os.path.getsize(path)
    return 0

def extension(path):
    """Return the file extension (including the dot)."""
    return os.path.splitext(normalize(path))[1]


def filename(path):
    """Return the filename without directories."""
    return os.path.basename(normalize(path))


def parent(path):
    """Return the parent directory."""
    return os.path.dirname(normalize(path))


def relative_to_script(script_file, *parts):
    """
    Resolve a path relative to the script's directory.

    Args:
        script_file (str): __file__ of the calling script.

    Returns:
        str: Normalized absolute path.
    """
    base = os.path.dirname(normalize(script_file))
    return join(base, *parts)