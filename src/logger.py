import logging
from os import getenv

# TODO maybe support custom logging config loaded from the toml config file (see #4)
logging.basicConfig(level=getenv("LOG_LEVEL", "INFO"))

log = logging.getLogger("dune-sync")
