"""
path_utils.py

Purpose:
    Provide helpers for normalizing, expanding, validating, and manipulating
    filesystem paths in a predictable, cross-platform way.
"""

import os


# ---------------------------------------------------------------------------
# Core normalization
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
    path = os.path.expandvars(path)      # Expand $VAR or %VAR%
    path = os.path.expanduser(path)      # Expand ~ or ~user
    path = os.path.normpath(path)        # Collapse .., ., and redundant slashes
    return os.path.abspath(path)         # Convert to absolute path


# ---------------------------------------------------------------------------
# Joining
# ---------------------------------------------------------------------------

def join(*parts):
    """
    Join path components and normalize the result.

    Returns:
        str: Normalized absolute path.
    """
    return normalize(os.path.join(*parts))


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def exists(path):
    """Return True if the path exists."""
    return os.path.exists(normalize(path))


def is_file(path):
    """Return True if the path points to a file."""
    return os.path.isfile(normalize(path))


def is_dir(path):
    """Return True if the path points to a directory."""
    return os.path.isdir(normalize(path))


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def ensure_dir(path):
    """
    Ensure that a directory exists. Creates it if missing.

    Returns:
        str: Normalized directory path.
    """
    path = normalize(path)
    os.makedirs(path, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def extension(path):
    """Return the file extension (including the dot)."""
    return os.path.splitext(normalize(path))[1]


def filename(path):
    """Return the filename without directories."""
    return os.path.basename(normalize(path))


def parent(path):
    """Return the parent directory."""
    return os.path.dirname(normalize(path))


# ---------------------------------------------------------------------------
# Script-relative resolution
# ---------------------------------------------------------------------------

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
