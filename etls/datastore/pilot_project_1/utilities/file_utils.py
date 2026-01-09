"""
file_utils.py

Purpose:
    Provide helpers for working with files and directories, including:
        - Creating nested folder structures
        - Generating random directory names
        - Normalizing paths
"""

import os
import uuid


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
