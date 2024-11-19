"""
Configuration classes & loading logic for the dune-sync package.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from string import Template

import yaml
from dotenv import load_dotenv
from dune_client.query import QueryBase
from dune_client.types import ParameterType, QueryParameter

from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.interfaces import Destination, Source
from src.job import Job, Database
from src.sources.dune import DuneSource
from src.sources.postgres import PostgresSource


@dataclass
class DbRef:
    """
    A class to represent a database reference configuration.

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
        """
        Create a DbRef instance from a dictionary configuration.

        Args:
            data (dict[str, str]): Dictionary containing database reference configuration with keys:
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
        """
        Load environment variables and create an Env instance.

        Returns:
            Env: Initialized environment configuration

        Raises:
            RuntimeError: If required environment variables are not set
        """
        load_dotenv()
        dune_api_key = os.environ.get("DUNE_API_KEY")
        db_url = os.environ.get("DB_URL")

        if dune_api_key is None:
            raise RuntimeError("DUNE_API_KEY environment variable must be set!")
        if db_url is None:
            raise RuntimeError("DB_URL environment variable must be set!")

        return cls(db_url, dune_api_key)

    @staticmethod
    def interpolate(value: Any) -> Any:
        """
        Interpolate environment variables in a string value.
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
        if not isinstance(value, str):
            return value

        # Handle ${VAR} syntax
        template = Template(value)
        try:
            return template.substitute(os.environ)
        except KeyError as e:
            missing_var = str(e).strip("'")
            raise KeyError(f"Environment variable '{missing_var}' not found. ") from e


def parse_query_parameters(params: list[dict[str, Any]]) -> list[QueryParameter]:
    """
    Convert a list of parameter dictionaries into Dune query parameters.

    Args:
        params (list[dict[str, Any]]): List of parameter dictionaries, each containing:
            - name: Parameter name
            - type: Parameter type (TEXT, NUMBER, DATE, or ENUM)
            - value: Parameter value

    Returns:
        list[QueryParameter]: List of properly typed Dune query parameters

    Raises:
        ValueError: If an unknown parameter type is encountered
    """
    query_params = []
    for param in params:
        name = param["name"]
        param_type = ParameterType.from_string(param["type"])
        value = param["value"]

        if param_type == ParameterType.TEXT:
            query_params.append(QueryParameter.text_type(name, value))
        elif param_type == ParameterType.NUMBER:
            query_params.append(QueryParameter.number_type(name, value))
        elif param_type == ParameterType.DATE:
            query_params.append(QueryParameter.date_type(name, value))
        elif param_type == ParameterType.ENUM:
            query_params.append(QueryParameter.enum_type(name, value))
        else:
            # Can't happen.
            # this code is actually unreachable because the case it handles
            # causes an exception to be thrown earlier, in ParameterType.from_string()
            raise ValueError(
                f"Unknown parameter type: {param['type']}"
            )  # pragma: no cover

    return query_params


@dataclass
class RuntimeConfig:
    """
    A class to represent the runtime configuration settings.

    This class handles loading and parsing of the YAML configuration file,
    which defines the sources, destinations, and jobs for data synchronization.

    Attributes:
        jobs (list[Job]): List of configured synchronization jobs
    """

    jobs: list[Job]

    @classmethod
    def load_from_yaml(cls, file_path: Path | str = "config.yaml") -> RuntimeConfig:
        """
        Load and parse a YAML configuration file.

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

        # Load sources map
        sources = {}
        for source in data.get("sources", []):
            sources[str(source["name"])] = DbRef.from_dict(source)

        # Load destinations map
        destinations = {}
        for destination in data.get("destinations", []):
            destinations[str(destination["name"])] = DbRef.from_dict(destination)

        jobs = []
        for job_config in data.get("jobs", []):
            source = cls._build_source(job_config["source"], sources)
            destination = cls._build_destination(
                job_config["destination"], destinations
            )
            jobs.append(Job(source, destination))

        return cls(jobs=jobs)

    @staticmethod
    def _build_source(
        source_config: dict[str, Any], sources: dict[str, DbRef]
    ) -> Source[Any]:
        """
        Build a source object from configuration.

        Args:
            source_config (dict[str, Any]): Source configuration dictionary
            sources (dict[str, DbRef]): Map of available source references

        Returns:
            Source[Any]: Configured source object (DuneSource or PostgresSource)

        Raises:
            ValueError: If source type is unsupported
            KeyError: If referenced source is not found
        """
        source = sources[source_config["ref"]]
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
                    db_url=source.key, query_string=source_config["query_string"]
                )

        raise ValueError(f"Unsupported source_db type: {source}")

    @staticmethod
    def _build_destination(
        dest_config: dict[str, Any], destinations: dict[str, DbRef]
    ) -> Destination[Any]:
        """
        Build a destination object from configuration.

        Args:
            dest_config (dict[str, Any]): Destination configuration dictionary
            destinations (dict[str, DbRef]): Map of available destination references

        Returns:
            Destination[Any]: Configured destination object (DuneDestination or PostgresDestination)

        Raises:
            ValueError: If destination type is unsupported
            KeyError: If referenced destination is not found
        """
        dest = destinations[dest_config["ref"]]
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
                )
        raise ValueError(f"Unsupported destination_db type: {dest}")
