"""
path_utils.py

Purpose:
    Provide helpers for normalizing and expanding filesystem paths.
"""

import os


def normalize(path):
    """
    Normalize a filesystem path.

    Args:
        path (str): Input path.

    Returns:
        str: Normalized absolute path.
    """
    return os.path.abspath(os.path.expandvars(path))
