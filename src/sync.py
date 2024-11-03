# pragma: no cover
from typing import Any
from src.config import DuneToLocalJob, Env, LocalToDuneJob
from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.interfaces import Destination, Source
from src.logger import log
from src.sources.dune import DuneSource
from src.sources.postgres import PostgresSource


def dune_to_postgres(env: Env, job: DuneToLocalJob) -> None:

    sync(
        source=DuneSource(env.dune_api_key, job),
        destination=PostgresDestination(env.db_url, job.table_name, job.if_exists),
    )


def postgres_to_dune(env: Env, job: LocalToDuneJob) -> None:
    """
    Sync data from Postgres to Dune
    """
    log.info("Syncing data from Postgres to Dune for job: %s", job)

    sync(
        source=PostgresSource(env.db_url, job),
        destination=DuneDestination(env.dune_api_key, job.table_name),
    )

# TODO: This is a bit of a hack to get around the fact that the Source and Destination
# interfaces are generic but the implementations are not.
# Soon we will introduce a Job class that will have this logic.
def sync(source: Source[Any], destination: Destination[Any]) -> None:
    if not source.validate():
        raise ValueError(f"Invalid {source.__class__.__name__} configuration")
    if not destination.validate():
        raise ValueError(f"Invalid {destination.__class__.__name__} configuration")

    df = source.fetch()
    if not source.is_empty(df):
        destination.save(df)
    else:
        log.warning("No Query results found! Skipping write")
