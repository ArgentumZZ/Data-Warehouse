# import libraries
from typing import Dict
import os, configparser

# import custom libraries
import utilities.logging_manager as lg

def load_smtp_config() -> Dict[str, str]:
    """
    Reads the SMTP configuration files db_config.cfg

    Returns: a dictionary with credentials.
    """

    # 1. Initialize the config
    config = configparser.ConfigParser()
    config_path = os.path.join(os.environ['CONFIG_DIR'])

    # 2. Check if the path exists
    if not os.path.exists(config_path):
        lg.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Missing {config_path}")

    # 3. Read the config_path
    config.read(config_path)

    # 4. Construct the configuration dictionary
    config_dict = {
        'host'          : config.get('SMTP_SERVER', 'host'),
        'port'          : config.getint('SMTP_SERVER', 'port'),
        'username'      : config.get('SMTP_SERVER', 'username'),
        'password'      : config.get('SMTP_SERVER', 'password'),
        'from_address'  : config.get('SMTP_SERVER', 'from_address')
    }

    return config_dict

# ===========================================================
# Credential helpers that may be implemented
# ===========================================================
def load_sftp_credentials():
    """Parse credentials from configuration file and load SFTP server variables."""
    pass

# Safe credential parsing
def parse_credentials(self):
    """Parse credentials from config/environment variables safely."""
    pass

# Environment variable helpers
def load_env_credentials(self):
    """Load credentials from environment variables."""
    pass

# Connection string builders
def build_connection_string(self):
    """Build a connection string from credential components."""
    pass
