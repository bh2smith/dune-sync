from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal, Any, Union

import tomli
import yaml
from dotenv import load_dotenv
from dune_client.query import QueryBase
from dune_client.types import ParameterType, QueryParameter

from src.logger import log

TableExistsPolicy = Literal["append", "replace"]


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


class DataSource(Enum):
    """Enum for possible data sources/destinations"""

    POSTGRES = "postgres"
    DUNE = "dune"


def parse_query_parameters(
    params: Union[list[YamlConf], tuple[YamlConf]]
) -> list[QueryParameter]:
    query_params = []
    for param in params:
        name = param.name
        param_type = ParameterType.from_string(param.type)
        value = param.value

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
            raise ValueError(f"Unknown parameter type: {param.type}")

    return query_params


@dataclass
class BaseJob:
    """Base class for all jobs with common attributes"""

    table_name: str
    source: DataSource
    destination: DataSource

    def validate_source_destination(self) -> None:
        if self.source == self.destination:
            raise ValueError("Source and destination cannot be the same")

    def __post_init__(self) -> None:
        self.validate_source_destination()


@dataclass
class DuneToLocalJob(BaseJob):
    """
    A class to represent a single job configuration.

    Attributes
    ----------
    query : QueryBase
        The, possibly parameterized, query to execute.
    table_name : str
        The name of the table where the query results will be stored.
    poll_frequency : int
        The frequency (in seconds) at which the query should be polled.
    query_engine : Literal["medium", "large"]
        The query engine to use, either "medium" or "large" (default is "medium").
    """

    query: QueryBase
    poll_frequency: int
    query_engine: Literal["medium", "large"] = "medium"  # Default value is "medium"
    if_exists: TableExistsPolicy = "append"


@dataclass
class LocalToDuneJob(BaseJob):
    """
    A class to represent a single job configuration.
    """

    query_string: str

    def __str__(self) -> str:
        return f"LocalToDuneJob(table_name={self.table_name}, query_string={self.query_string})"


class YamlConf:  # pylint: disable=too-few-public-methods
    """
    A helper to create a nested object out of nested YAML configuration
    """

    # TODO these fields are bogus, just to satisfy mypy, should be cleaned up
    jobs: list[Any] = []
    name: str = ""
    type: str = ""
    value: Any = ""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = YamlConf(**value)
            elif isinstance(value, (list, tuple)):
                self.__dict__[key] = [YamlConf(**item) for item in value]
            else:
                self.__dict__[key] = value


@dataclass
class RuntimeConfig:
    """
    A class to represent the runtime configuration settings.

    Attributes
    ----------
    dune_to_local_jobs : List[JobConfig]
        A list of JobConfig instances, each representing a separate job configuration.

    local_to_dune_jobs : List[JobConfig]
        A list of JobConfig instances, each representing a separate job configuration.
    """

    dune_to_local_jobs: list[DuneToLocalJob]
    local_to_dune_jobs: list[LocalToDuneJob]

    @classmethod
    def load_from_yaml(
        cls, file_path: Union[Path, str] = "config.yaml"
    ) -> RuntimeConfig:
        with open(file_path, "rb") as _handle:
            data = yaml.safe_load(_handle)
        conf = YamlConf(**data)

        dune_to_local_jobs = [
            DuneToLocalJob(
                query=QueryBase(
                    query_id=int(job.source.query_id),
                    params=parse_query_parameters(
                        getattr(job.source, "parameters", [])
                    ),
                ),
                poll_frequency=job.source.poll_frequency,
                query_engine=job.source.query_engine,
                if_exists=job.destination.if_exists,
                table_name=job.destination.table_name,
                source=job.source.ref,
                destination=job.destination.ref,
            )
            for job in conf.jobs
            if job.source.ref == DataSource.DUNE.value
            and job.destination.ref == DataSource.POSTGRES.value
        ]

        local_to_dune_jobs = [
            LocalToDuneJob(
                query_string=job.source.query_string,
                table_name=job.destination.table_name,
                source=job.source.ref,
                destination=job.destination.ref,
            )
            for job in conf.jobs
            if job.source.ref == DataSource.POSTGRES.value
            and job.destination.ref == DataSource.DUNE.value
        ]
        return cls(dune_to_local_jobs, local_to_dune_jobs)

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
        dune_to_local_jobs, local_to_dune_jobs = [], []
        for job in toml_dict.get("jobs", []):
            # Parse source and destination from config
            source = DataSource(job["source"].lower())
            destination = DataSource(job["destination"].lower())

            # Common job parameters
            table_name = job["table_name"]

            if source == DataSource.DUNE and destination == DataSource.POSTGRES:
                if "query_engine" in job and job["query_engine"] not in [
                    "medium",
                    "large",
                ]:
                    raise ValueError("query_engine must be either 'medium' or 'large'.")
                dune_to_local_jobs.append(
                    DuneToLocalJob(
                        source=source,
                        destination=destination,
                        table_name=table_name,
                        query=QueryBase(
                            query_id=job["query_id"],
                            params=parse_query_parameters(
                                job.get("query_parameters", [])
                            ),
                        ),
                        poll_frequency=job.get("poll_frequency", 1),
                        query_engine=job.get("query_engine", "medium"),
                        if_exists=job.get("if_exists", "append"),
                    )
                )
            elif source == DataSource.POSTGRES and destination == DataSource.DUNE:
                local_to_dune_jobs.append(
                    LocalToDuneJob(
                        source=source,
                        destination=destination,
                        table_name=table_name,
                        query_string=job["query_string"],
                    )
                )
            else:
                raise ValueError(
                    f"Invalid source/destination combination: {source} -> {destination}"
                )

        config = cls(dune_to_local_jobs, local_to_dune_jobs)
        config.validate()
        return config

    def validate(self) -> None:
        num_jobs = len(self.dune_to_local_jobs)
        if num_jobs != len(set(j.query.query_id for j in self.dune_to_local_jobs)):
            log.warning("Detected multiple jobs running the same query")
        if num_jobs != len(set(j.table_name for j in self.dune_to_local_jobs)):
            log.warning("Detected duplicate table names in job list")
