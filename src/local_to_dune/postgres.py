from src.config import Env, LocalToDuneJob
from src.logger import log


def postgres_to_dune(_env: Env, job: LocalToDuneJob) -> None:
    """
    Sync data from Postgres to Dune
    """
    log.info(f"Syncing data from Postgres to Dune for job: {job}")
    raise NotImplementedError("Not implemented")
