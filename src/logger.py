"""
Logging configuration for the dune-sync package.
"""

import logging
from os import getenv

# TODO maybe support custom logging config loaded from the config file
logging.basicConfig(level=getenv("LOG_LEVEL", "INFO"))

log = logging.getLogger("dune-sync")
