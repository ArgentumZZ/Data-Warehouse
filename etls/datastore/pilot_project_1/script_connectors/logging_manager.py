"""
common.logging
--------------

Minimal logger with no handlers.
Safe to import anywhere.
"""

import logging

# Create a global logger object ONCE
logger = logging.getLogger("etl")
logger.setLevel(logging.INFO)

# Add a simple console handler (optional)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)

