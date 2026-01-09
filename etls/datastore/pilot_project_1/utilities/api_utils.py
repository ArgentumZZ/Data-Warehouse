"""
api_utils.py

Purpose:
    Provide simple wrappers for HTTP API calls.
"""

import requests


def api_get(url, headers=None):
    """
    Perform a GET request and return JSON.

    Args:
        url (str): Request URL.
        headers (dict): Optional headers.

    Returns:
        dict: JSON response.

    Raises:
        HTTPError: If request fails.
    """
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
