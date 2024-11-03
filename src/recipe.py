# pragma: no cover
from src.config import DuneToLocalJob, Env, LocalToDuneJob
from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.logger import log
from src.sources.dune import DuneSource
from src.sources.postgres import PostgresSource


def dune_to_postgres(env: Env, job: DuneToLocalJob) -> None:
    source = DuneSource(env.dune_api_key, job)
    destination = PostgresDestination(env.db_url, job.table_name, job.if_exists)

    if not source.validate():
        raise ValueError("Invalid Dune configuration")
    if not destination.validate():
        raise ValueError("Invalid Postgres configuration")

    data = source.fetch()
    if not data[0].empty:
        destination.save(data)
    else:
        log.warning("No Query results found! Skipping write")


def postgres_to_dune(env: Env, job: LocalToDuneJob) -> None:
    """
    Sync data from Postgres to Dune
    """
    log.info("Syncing data from Postgres to Dune for job: %s", job)

    source = PostgresSource(env.db_url, job)
    destination = DuneDestination(env.dune_api_key, job.table_name)

    if not source.validate():
        raise ValueError("Invalid Postgres configuration")
    if not destination.validate():
        raise ValueError("Invalid Dune configuration")

    df = source.fetch()
    destination.save(df)
