"""Job execution logic for the dune-sync package."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from src.interfaces import Destination, Named, Source
from src.logger import log
from src.metrics import collect_metrics


class Database(Enum):
    """Enum for possible data sources/destinations.

    Values:
        POSTGRES: PostgreSQL database
        DUNE: Dune Analytics
        SQLITE: SQLite database
    """

    POSTGRES = "postgres"
    DUNE = "dune"
    SQLITE = "sqlite"

    @classmethod
    def from_string(cls, value: str) -> Database:
        """Create a Database enum from a string value.

        Args:
            value (str): String representation of the database type

        Returns:
            Database: The corresponding Database enum value

        Raises:
            ValueError: If the provided string doesn't match any known database type

        """
        try:
            return cls(value.lower())
        except ValueError as e:
            raise ValueError(f"Unknown Database type: {value}") from e


@dataclass
class Job(Named):
    """Base class for all data synchronization jobs.

    A job represents a single data transfer operation from a source
    to a destination. It handles the extraction and loading of data
    between different database systems.

    Attributes:
        source (Source[Any]): The data source to extract from
        destination (Destination[Any]): The destination to load data into

    """

    name: str
    source: Source[Any]
    destination: Destination[Any]

    @collect_metrics
    async def run(self) -> None:
        """Execute the job by fetching from the source and saving to the destination.

        The method will:
        1. Fetch data from the source
        2. Check if the result set is not empty
        3. If data exists, save it to the destination
        4. If no data exists, log a warning

        Note:
            No exception is raised for empty result sets, only a warning is logged.

        """
        log.info("Fetching data for job: %s", self.name)
        start_time = time.time()

        df = await self.source.fetch()
        log.info("Saving data for job: %s", self.name)
        if not self.source.is_empty(df):
            affected_rows = self.destination.save(df)
            elapsed_time = time.time() - start_time
            log.info(
                "Completed job: %s in %.2f seconds "
                "| affected rows: %d "
                "| records fetched: %d",
                self.name,
                elapsed_time,
                affected_rows,
                len(df),
            )
        else:
            log.warning("No query results found! Skipping write")

    def __str__(self) -> str:
        """Return a string representation of the job to use in logging."""
        return f"{self.name}"
