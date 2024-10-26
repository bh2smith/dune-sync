from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal

import tomli
from dotenv import load_dotenv

from src.logger import log


@dataclass
class Env:
    """
    A class to represent the environment configuration.

    Attributes
    ----------
    db_url : str
        The URL of the database connection.
    dune_api_key : str
        The API key used for accessing the Dune Analytics API.

    Methods
    -------
    None
    """

    db_url: str
    dune_api_key: str

    @classmethod
    def load(cls) -> Env:
        load_dotenv()
        dune_api_key = os.environ.get("DUNE_API_KEY")
        db_url = os.environ.get("DB_URL")

        if dune_api_key is None:
            raise EnvironmentError("DUNE_API_KEY environment variable must be set!")
        if db_url is None:
            raise EnvironmentError("DB_URL environment variable must be set!")

        return cls(db_url, dune_api_key)


@dataclass
class DuneToLocalJob:
    """
    A class to represent a single job configuration.

    Attributes
    ----------
    query_id : int
        The ID of the query to execute.
    table_name : str
        The name of the table where the query results will be stored.
    poll_frequency : int
        The frequency (in seconds) at which the query should be polled.
    query_engine : Literal["medium", "large"]
        The query engine to use, either "medium" or "large" (default is "medium").
    """

    query_id: int
    table_name: str
    poll_frequency: int
    query_engine: Literal["medium", "large"] = "medium"  # Default value is "medium"


@dataclass
class RuntimeConfig:
    """
    A class to represent the runtime configuration settings.

    Attributes
    ----------
    jobs : List[JobConfig]
        A list of JobConfig instances, each representing a separate job configuration.
    """

    jobs: list[DuneToLocalJob]

    @classmethod
    def load_from_toml(cls, file_path: str = "config.toml") -> RuntimeConfig:
        """
        Reads the configuration from a TOML file and returns a RuntimeConfig object.

        Parameters
        ----------
        file_path : str
            The path to the TOML configuration file.

        Returns
        -------
        RuntimeConfig
            An instance of RuntimeConfig populated with the data from the TOML file.
        """
        # Load the configuration from the TOML file
        with open(file_path, "rb") as f:
            toml_dict = tomli.load(f)

        # Parse each job configuration
        jobs = []
        for job in toml_dict.get("jobs", []):
            query_id = job["query_id"]
            table_name = job.get("table_name", f"dune_data_{query_id}")
            query_engine = job.get("query_engine", "medium")

            if query_engine not in ("medium", "large"):
                raise ValueError("query_engine must be either 'medium' or 'large'.")

            poll_frequency = job.get("poll_frequency", 1)

            jobs.append(
                DuneToLocalJob(
                    query_id=query_id,
                    table_name=table_name,
                    poll_frequency=poll_frequency,
                    query_engine=query_engine,
                )
            )
        config = cls(jobs)
        config.validate()
        return config

    def validate(self) -> None:
        if len(self.jobs) == len(set(j.query_id for j in self.jobs)):
            log.warning("Detected multiple jobs running the same query")
