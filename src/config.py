"""Configuration classes & loading logic for the dune-sync package."""

from __future__ import annotations

import os
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from string import Template
from typing import Any, TextIO
from urllib.parse import urlsplit

import requests
import yaml
from dotenv import load_dotenv
from dune_client.query import QueryBase

from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.interfaces import Destination, Source
from src.job import Database, Job
from src.logger import log
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

    def __post_init__(self) -> None:
        """Validate that unique job names are used."""
        job_names = [job.name for job in self.jobs]
        if len(job_names) != len(set(job_names)):
            duplicates = {name for name in job_names if job_names.count(name) > 1}
            raise ValueError(
                f"Duplicate job names found in configuration: {', '.join(duplicates)}"
            )

    @classmethod
    def _is_url(cls, path: str) -> bool:
        """Perform a basic check if given string looks like a URL.

        :param path: arbitrary string
        """
        try:
            result = urlsplit(path)
            return bool(result.scheme and result.netloc)
        except (TypeError, AttributeError):  # raised when path isn't str-like
            return False

    @classmethod
    def _load_config_file(cls, file_path: Path | str) -> Any:
        with open(file_path, encoding="utf-8") as _handle:
            return cls.read_yaml(_handle)

    @classmethod
    def _load_config_url(cls, url: str) -> Any:
        """Load configuration from a URL.

        Args:
            url: The URL to fetch the configuration from

        Returns:
            The parsed YAML configuration

        Raises:
            SystemExit: If the configuration cannot be downloaded

        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            config_data = response.text
        except requests.RequestException as e:
            log.error("Error fetching config from URL: %s", e)
            raise SystemExit("Could not download config") from e

        return cls.read_yaml(StringIO(config_data))

    @classmethod
    def read_yaml(cls, file_handle: TextIO) -> Any:
        """Load YAML from text, substituting any environment variables referenced."""
        Env.load()
        text = str(file_handle.read())
        text = Env.interpolate(text)
        return yaml.safe_load(text)

    @classmethod
    def load(cls, file_path: Path | str = "config.yaml") -> RuntimeConfig:
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
        if cls._is_url(str(file_path)):
            data = cls._load_config_url(url=str(file_path))
        else:
            data = cls._load_config_file(file_path)

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
                try:
                    request_timeout = dest_config["request_timeout"]
                    request_timeout = int(request_timeout)
                except KeyError:
                    log.debug("Dune request timeout not set: defaulting to 10")
                    request_timeout = 10
                except ValueError as e:
                    log.error(
                        "request_timeout parameter must be a number, received type %s",
                        type(request_timeout),
                    )
                    raise e
                return DuneDestination(
                    api_key=dest.key,
                    table_name=dest_config["table_name"],
                    request_timeout=request_timeout,
                    insertion_type=dest_config.get("insertion_type", "append"),
                )

            case Database.POSTGRES:
                return PostgresDestination(
                    db_url=dest.key,
                    table_name=dest_config["table_name"],
                    if_exists=dest_config.get("if_exists", "append"),
                    index_columns=dest_config.get("index_columns", []),
                )
        raise ValueError(f"Unsupported destination_db type: {dest}")
