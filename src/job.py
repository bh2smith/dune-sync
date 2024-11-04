from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from src.interfaces import Source, Destination
from src.logger import log


class Database(Enum):
    """Enum for possible data sources/destinations"""

    POSTGRES = "postgres"
    DUNE = "dune"

    @classmethod
    def from_string(cls, value: str) -> Database:
        try:
            return cls(value.lower())
        except ValueError as e:
            raise ValueError(f"Unknown source type: {value}") from e


@dataclass
class Job:
    """Base class for all jobs with common attributes"""

    source: Source[Any]
    destination: Destination[Any]

    def run(self) -> None:
        df = self.source.fetch()
        if not self.source.is_empty(df):
            self.destination.save(df)
        else:
            log.warning("No Query results found! Skipping write")
