"""Configuration classes & loading logic for the dune-sync package."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any

import yaml
from dotenv import load_dotenv
from dune_client.query import QueryBase

from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.interfaces import Destination, Source
from src.job import Database, Job
from src.sources.dune import DuneSource, parse_query_parameters
from src.sources.postgres import PostgresSource


@dataclass
class DbRef:
    """A class to represent a database reference configuration.

    Attributes
    ----------
    name : str
        The name of the database reference
    type : Database
        The type of database (DUNE or POSTGRES)
    key : str
        The connection key (API key or connection string)

    """

    name: str
    type: Database
    key: str

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> DbRef:
        """Create a DbRef instance from a dictionary configuration.

        Args:
            data (dict[str, str]): Dictionary containing database reference
                configuration with keys:
                - name: The name of the database reference
                - type: The database type ('dune' or 'postgres')
                - key: The connection key with optional environment variable references
                      (e.g., ${DUNE_API_KEY} or ${DB_URL})

        Returns:
            DbRef: A new database reference instance

        Raises:
            KeyError: If required fields are missing from the dictionary
            ValueError: If the database type is invalid
            KeyError: If referenced environment variables don't exist

        """
        env = Env.load()
        return cls(
            name=data["name"],
            type=Database.from_string(data["type"]),
            key=env.interpolate(data["key"]),
        )


@dataclass
class Env:
    """A class to represent the environment configuration.

    Methods
    -------
    None

    """

    @classmethod
    def load(cls) -> Env:
        """Load environment variables and create an Env instance.

        Returns:
            Env: Initialized environment configuration

        """
        load_dotenv()
        return cls()

    @staticmethod
    def interpolate(value: str) -> str:
        """Interpolate environment variables in a string value.

        Handles ${VAR} and $VAR syntax.
        Returns the original value if it's not a string.

        Args:
            value: The value to interpolate. Can be any type, but only strings
                  will be processed for environment variables.

        Returns:
            The interpolated value if it's a string, otherwise the original value.

        Raises:
            KeyError: If an environment variable referenced in the string doesn't exist.

        """
        # Handle ${VAR} syntax
        template = Template(value)
        try:
            return template.substitute(os.environ)
        except KeyError as e:
            missing_var = str(e).strip("'")
            raise KeyError(f"Environment variable '{missing_var}' not found. ") from e


@dataclass
class RuntimeConfig:
    """A class to represent the runtime configuration settings.

    This class handles loading and parsing of the YAML configuration file,
    which defines the sources, destinations, and jobs for data synchronization.

    Attributes:
        jobs (list[Job]): List of configured synchronization jobs

    """

    jobs: list[Job]

    @classmethod
    def load_from_yaml(cls, file_path: Path | str = "config.yaml") -> RuntimeConfig:
        """Load and parse a YAML configuration file.

        Args:
            file_path (Path | str): Path to the YAML configuration file

        Returns:
            RuntimeConfig: Initialized runtime configuration

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML is invalid
            ValueError: If the configuration contains invalid database types

        """
        with open(file_path, "rb") as _handle:
            data = yaml.safe_load(_handle)

        # Load data sources map
        sources = {}
        if not data.get("data_sources"):
            raise SystemExit("Fatal: No data_sources defined in config.")

        for db_ref in data["data_sources"]:
            sources[str(db_ref["name"])] = DbRef.from_dict(db_ref)

        jobs = []
        for job_config in data.get("jobs", []):
            source = cls._build_source(job_config["source"], sources)
            dest = cls._build_destination(job_config["destination"], sources)
            name = job_config["name"]
            jobs.append(Job(name, source, dest))

        return cls(jobs=jobs)

    @staticmethod
    def _build_source(
        source_config: dict[str, Any], sources: dict[str, DbRef]
    ) -> Source[Any]:
        """Build a source object from configuration.

        Args:
            source_config (dict[str, Any]): Source configuration dictionary
            sources (dict[str, DbRef]): Map of available source references

        Returns:
            Source[Any]: Configured source object (DuneSource or PostgresSource)

        Raises:
            ValueError: If source type is unsupported
            KeyError: If referenced source is not found

        """
        try:
            source = sources[source_config["ref"]]
        except KeyError as e:
            raise SystemExit(
                "Fatal: no datasource with `name` = "
                f'"{source_config["ref"]}" defined in config'
            ) from e

        match source.type:
            case Database.DUNE:
                return DuneSource(
                    api_key=source.key,
                    query=QueryBase(
                        query_id=int(source_config["query_id"]),
                        params=parse_query_parameters(
                            source_config.get("parameters", [])
                        ),
                    ),
                    poll_frequency=source_config.get("poll_frequency", 1),
                    query_engine=source_config.get("query_engine", "medium"),
                )

            case Database.POSTGRES:
                return PostgresSource(
                    db_url=source.key,
                    query_string=source_config["query_string"],
                )

        raise ValueError(f"Unsupported source_db type: {source}")

    @staticmethod
    def _build_destination(
        dest_config: dict[str, Any], destinations: dict[str, DbRef]
    ) -> Destination[Any]:
        """Build a destination object from configuration.

        Args:
            dest_config (dict[str, Any]): Destination configuration dictionary
            destinations (dict[str, DbRef]): Map of available destination references

        Returns:
            Destination[Any]: Configured destination object
                (DuneDestination or PostgresDestination)

        Raises:
            ValueError: If destination type is unsupported
            KeyError: If referenced destination is not found

        """
        try:
            dest = destinations[dest_config["ref"]]
        except KeyError as e:
            raise SystemExit(
                "Fatal: no datasource with `name` = "
                f' "{dest_config["ref"]}" defined in config'
            ) from e

        match dest.type:
            case Database.DUNE:
                return DuneDestination(
                    api_key=dest.key,
                    table_name=dest_config["table_name"],
                )

            case Database.POSTGRES:
                return PostgresDestination(
                    db_url=dest.key,
                    table_name=dest_config["table_name"],
                    if_exists=dest_config["if_exists"],
                    index_columns=dest_config.get("index_columns", []),
                )
        raise ValueError(f"Unsupported destination_db type: {dest}")
