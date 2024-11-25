"""Logging configuration for the dune-sync package."""

import logging
import sys
from os import getenv


class InfoFilter(logging.Filter):
    """Filter that only allows records at INFO or WARNING level."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Return True if we should log this record."""
        return record.levelno not in (logging.ERROR, logging.CRITICAL)


# Create handlers for stdout and stderr
stdout_handler = logging.StreamHandler(sys.stdout)
stderr_handler = logging.StreamHandler(sys.stderr)

# Add filters
stdout_handler.addFilter(InfoFilter())
stderr_handler.setLevel(logging.ERROR)

# Configure log formatter
formatter = logging.Formatter(
    fmt=(
        "%(asctime)s [%(levelname)s] %(name)s:%(module)s."
        "%(funcName)s:%(lineno)d - %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
)
stdout_handler.setFormatter(formatter)
stderr_handler.setFormatter(formatter)

# Basic config
logging.basicConfig(
    level=getenv("LOG_LEVEL", "INFO"),
    handlers=[stdout_handler, stderr_handler],
    force=True,  # Ensure we override any existing configuration
)

log = logging.getLogger("dune-sync")
