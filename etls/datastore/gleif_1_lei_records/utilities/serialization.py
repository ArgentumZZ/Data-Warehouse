"""
serialization.py

Purpose:
    Provide JSON serialization/deserialization helpers.
"""

import json


def to_json(data):
    """
    Convert Python data to pretty JSON.

    Args:
        data (any): Input data.

    Returns:
        str: JSON string.
    """
    return json.dumps(data, indent=2)


def from_json(text):
    """
    Parse JSON text into Python data.

    Args:
        text (str): JSON string.

    Returns:
        any: Parsed data.
    """
    return json.loads(text)
