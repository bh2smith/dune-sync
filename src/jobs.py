from dataclasses import dataclass
from enum import Enum
from typing import Any

from dune_client.query import QueryBase

from src.config import parse_query_parameters, Env
from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.interfaces import Source, Destination
from src.logger import log
from src.sources.dune import DuneSource
from src.sources.postgres import PostgresSource
from src.sync_types import TableExistsPolicy, DuneQueryEngineType


class DataSource(Enum):
    """Enum for possible data sources/destinations"""

    POSTGRES = "postgres"
    DUNE = "dune"


@dataclass
class BaseJob:
    """Base class for all jobs with common attributes"""

    table_name: str
    source: Source
    destination: Destination

    def validate_source_destination(self) -> None:
        if self.source == self.destination:
            raise ValueError("Source and destination cannot be the same")

    def __post_init__(self) -> None:
        self.validate_source_destination()

    def run(self) -> None:
        df = self.source.fetch()
        if not self.source.is_empty(df):
            self.destination.save(df)
        else:
            log.warning("No Query results found! Skipping write")


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
    query_engine: DuneQueryEngineType = "medium"  # Default value is "medium"
    if_exists: TableExistsPolicy = "append"


@dataclass
class LocalToDuneJob(BaseJob):
    """
    A class to represent a single job configuration.
    """

    query_string: str

    def __str__(self) -> str:
        return f"LocalToDuneJob(table_name={self.table_name}, query_string={self.query_string})"


class JobResolver:  # pylint: disable=too-few-public-methods
    """
    Given a job from the config yaml file, return a runnable object that represents the Job
    """

    job_config: dict[Any, Any]
    job: BaseJob

    def __init__(self, env: Env, job: dict[Any, Any]) -> None:
        self.job_config = job
        try:
            self._build_job_config(env)
        except (KeyError, ValueError, TypeError):
            log.error("Error parsing job config: %s; check config file.", job)
            raise

    def _build_job_config(self, env: Env) -> None:
        if (
            self.job_config["source"]["ref"] == DataSource.POSTGRES.value
            and self.job_config["destination"]["ref"] == DataSource.DUNE.value
        ):
            self.job = self._build_local_to_dune_job(env)
        elif (
            self.job_config["source"]["ref"] == DataSource.DUNE.value
            and self.job_config["destination"]["ref"] == DataSource.POSTGRES.value
        ):
            self.job = self._build_dune_to_local_job(env)
        else:
            # In the future we might try to guess based on job parameters,
            # but for now this is not implemented
            raise NotImplementedError(
                f"Unprocessable job configuration: {self.job_config}"
            )

    def _build_dune_to_local_job(self, env: Env) -> BaseJob:
        self.job = DuneToLocalJob(
            query=QueryBase(
                query_id=int(self.job_config["source"]["query_id"]),
                params=parse_query_parameters(
                    self.job_config["source"].get("parameters", [])
                ),
            ),
            poll_frequency=self.job_config["source"]["poll_frequency"],
            query_engine=self.job_config["source"]["query_engine"],
            if_exists=self.job_config["destination"]["if_exists"],
            table_name=self.job_config["destination"]["table_name"],
            source=self.job_config["source"]["ref"],
            destination=self.job_config["destination"]["ref"],
        )
        self.job.source = DuneSource(env.dune_api_key, self.job)
        self.job.destination = PostgresDestination(
            db_url=env.db_url,
            table_name=self.job_config["destination"]["table_name"],
            if_exists=self.job_config["destination"]["if_exists"],
        )
        return self.job

    def _build_local_to_dune_job(self, env: Env) -> BaseJob:
        self.job = LocalToDuneJob(
            query_string=self.job_config["source"]["query_string"],
            table_name=self.job_config["destination"]["table_name"],
            source=self.job_config["source"]["ref"],
            destination=self.job_config["destination"]["ref"],
        )
        self.job.source = PostgresSource(
            db_url=env.db_url,
            job=self.job,
        )
        self.job.destination = DuneDestination(
            api_key=env.dune_api_key,
            table_name=self.job_config["destination"]["table_name"],
        )
        return self.job

    def get_job(self) -> BaseJob:
        return self.job
