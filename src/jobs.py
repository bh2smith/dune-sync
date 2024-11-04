from dataclasses import dataclass
from enum import Enum
from typing import Any

from src.interfaces import Source, Destination
from src.logger import log


class DataSource(Enum):
    """Enum for possible data sources/destinations"""

    POSTGRES = "postgres"
    DUNE = "dune"


@dataclass
class BaseJob:
    """Base class for all jobs with common attributes"""

    source: Source[Any]
    destination: Destination[Any]

    def run(self) -> None:
        df = self.source.fetch()
        if not self.source.is_empty(df):
            self.destination.save(df)
        else:
            log.warning("No Query results found! Skipping write")
