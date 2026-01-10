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
import uuid


# ---------------------------------------------------------------------------
# Existing functions (kept exactly as provided)
# ---------------------------------------------------------------------------

def generate_random_dir(prefix="run_"):
    """
    Generate a random directory name.

    Args:
        prefix (str): Optional prefix for the directory.

    Returns:
        str: Random directory name.
    """
    return prefix + uuid.uuid4().hex[:8]


def create_folders(path_parts, isfolder=True):
    """
    Create a folder structure from a list of path components.

    Args:
        path_parts (list[str]): Components of the path.
        isfolder (bool): Whether to create the final path as a folder.

    Returns:
        tuple: (full_path, parent_path, path_parts)
    """
    full_path = os.path.join(*path_parts)
    if isfolder:
        os.makedirs(full_path, exist_ok=True)
    parent = os.path.dirname(full_path)
    return full_path, parent, path_parts


# ---------------------------------------------------------------------------
# Additional helpers (additive only)
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
