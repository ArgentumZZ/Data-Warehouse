import logging

# Create a global logger object
logger = logging.getLogger("etl")
logger.setLevel(logging.INFO)

# Add a simple console handler if none exists
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)

# Map the methods so you can use lg.info() instead of lg.logger.info()
info = logger.info
error = logger.error
warning = logger.warning
debug = logger.debug
critical = logger.critical