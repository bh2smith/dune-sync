from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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

    db_url: str
    dune_api_key: str

    @classmethod
    def load(cls) -> Env:
        load_dotenv()
        dune_api_key = os.environ.get("DUNE_API_KEY")
        db_url = os.environ.get("DB_URL")

        if dune_api_key is None:
            raise RuntimeError("DUNE_API_KEY environment variable must be set!")
        if db_url is None:
            raise RuntimeError("DB_URL environment variable must be set!")

        return cls(db_url, dune_api_key)


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

    jobs: list[Job]

    @classmethod
    def load_from_yaml(cls, file_path: Path | str = "config.yaml") -> RuntimeConfig:
        with open(file_path, "rb") as _handle:
            data = yaml.safe_load(_handle)

        env = Env.load()
        jobs = []

        for job_config in data.get("jobs", []):
            source = cls._build_source(env, job_config["source"])
            destination = cls._build_destination(env, job_config["destination"])
            jobs.append(Job(source, destination))

        return cls(jobs=jobs)

    @staticmethod
    def _build_source(env: Env, source_config: dict[str, Any]) -> Source[Any]:
        source_db = Database.from_string(source_config["ref"])
        match source_db:
            case Database.DUNE:
                return DuneSource(
                    api_key=env.dune_api_key,
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
                    db_url=env.db_url, query_string=source_config["query_string"]
                )

        raise ValueError(f"Unsupported source_db type: {source_db}")

    @staticmethod
    def _build_destination(env: Env, dest_config: dict[str, Any]) -> Destination[Any]:
        destination_db = Database.from_string(dest_config["ref"])
        match destination_db:
            case Database.DUNE:
                return DuneDestination(
                    api_key=env.dune_api_key,
                    table_name=dest_config["table_name"],
                )

            case Database.POSTGRES:
                return PostgresDestination(
                    db_url=env.db_url,
                    table_name=dest_config["table_name"],
                    if_exists=dest_config["if_exists"],
                )
        raise ValueError(f"Unsupported destination_db type: {destination_db}")
