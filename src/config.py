from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Any, Dict

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

    env_vars: dict

    @classmethod
    def load(cls) -> Env:
        load_dotenv()
        cls.env_vars = dict(os.environ)

        return cls(env_vars=cls.env_vars)

    def interpolate(self, value: Any) -> Any:
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
            return template.substitute(self.env_vars)
        except KeyError as e:
            missing_var = str(e).strip("'")
            raise KeyError(f"Environment variable '{missing_var}' not found. ") from e


def parse_query_parameters(params: list[dict[str, Any]]) -> list[QueryParameter]:
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
            raise ValueError(f"Unknown parameter type: {param['type']}")

    return query_params


@dataclass
class RuntimeConfig:
    """A class to represent the runtime configuration settings."""

    sources: Dict[str, Source[Any]]
    destinations: Dict[str, Destination[Any]]
    jobs: list[Job]

    @classmethod
    def load_from_yaml(cls, file_path: Path | str = "config.yaml") -> RuntimeConfig:
        with open(file_path, "rb") as _handle:
            data = yaml.safe_load(_handle)

        env = Env.load()

        sources = {}
        for source_config in data.get("sources", []):
            if sources.get(source_config["name"]):
                raise ValueError(f'Duplicate source name {source_config["name"]}')
            sources[source_config["name"]] = cls._build_source(env, source_config)

        destinations = {}
        for dest_config in data.get("destinations", []):
            if destinations.get(dest_config["name"]):
                raise ValueError(f'Duplicate source name {dest_config["name"]}')
            destinations[dest_config["name"]] = cls._build_destination(env, dest_config)

        jobs = []
        for job_config in data.get("jobs", []):
            source_ref = job_config["source"]["ref"]
            dest_ref = job_config["destination"]["ref"]

            if source_ref not in sources:
                raise ValueError(
                    f"Source '{source_ref}' not found in sources configuration"
                )
            if dest_ref not in destinations:
                raise ValueError(
                    f"Destination '{dest_ref}' not found in destinations configuration"
                )

            source = cls._update_source_config(sources[source_ref], job_config)
            destination = cls._update_destination_config(
                destinations[dest_ref], job_config
            )

            jobs.append(Job(source, destination))

        return cls(sources=sources, destinations=destinations, jobs=jobs)

    @staticmethod
    def _build_source(env: Env, source_config: dict[str, Any]) -> Source[Any]:
        source_type = source_config["type"].lower()
        match source_type:
            case Database.DUNE.value:
                return DuneSource(
                    api_key=env.interpolate(source_config["api_key"]),
                )
            case Database.POSTGRES.value:
                return PostgresSource(
                    db_url=env.interpolate(source_config["db_url"]),
                )
        raise ValueError(f"Unsupported source type: {source_type}")

    @staticmethod
    def _build_destination(env: Env, dest_config: dict[str, Any]) -> Destination[Any]:
        dest_type = dest_config["type"].lower()
        match dest_type:
            case Database.DUNE.value:
                return DuneDestination(
                    api_key=env.interpolate(dest_config["key"]),
                )
            case Database.POSTGRES.value:
                return PostgresDestination(
                    db_url=env.interpolate(dest_config["db_url"]),
                )
        raise ValueError(f"Unsupported destination type: {dest_type}")

    @staticmethod
    def _update_source_config(
        source: Source[Any], job_config: dict[str, Any]
    ) -> Source[Any]:
        """Update source with job-specific configuration."""
        match source:
            case DuneSource():
                source.query = QueryBase(
                    query_id=int(job_config["source"]["query_id"]),
                    params=parse_query_parameters(job_config.get("parameters", [])),
                )
                source.poll_frequency = job_config.get("poll_frequency", 1)
                source.query_engine = job_config.get("query_engine", "medium")

            case Database.POSTGRES.value:
                # TODO update
                pass
            case _:
                raise ValueError(f"Unsupported source type: {type(source)}")

        return source

    @staticmethod
    def _update_destination_config(
        dest: Destination[Any], job_config: dict[str, Any]
    ) -> Destination[Any]:
        """Update destination with job-specific configuration."""
        match dest:
            case DuneDestination():
                # TODO update
                pass
            case PostgresDestination():
                dest.table_name = job_config["destination"]["table_name"]
            case _:
                raise ValueError(f"Unsupported destination type: {type(dest)}")

        return dest
