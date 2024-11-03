import pandas as pd
from dune_client.client import DuneClient
from dune_client.models import DuneError
from sqlalchemy import create_engine

from src.config import Env, LocalToDuneJob
from src.logger import log


def extract_data_from_postgres(env: Env, job: LocalToDuneJob) -> pd.DataFrame:
    log.debug("Reading data from Postgres...")
    engine = create_engine(env.db_url)
    log.debug("Successfully read data into pandas DF")
    return pd.read_sql_query(job.query_string, con=engine)


def load_data_into_dune(df: pd.DataFrame, env: Env, job: LocalToDuneJob) -> bool:
    dune = DuneClient(env.dune_api_key)
    try:
        log.debug("Uploading DF to Dune...")
        result = dune.upload_csv(job.table_name, df.to_csv())
        log.debug("Uploaded to Dune: %s", result)
        return True
    except DuneError as dune_e:
        log.error("Dune did not accept our upload: %s", dune_e)
        return False
    except Exception as e:
        log.error("Unexpected error: %s", e)
        return False


def postgres_to_dune(env: Env, job: LocalToDuneJob) -> bool:
    """
    Sync data from Postgres to Dune
    """
    log.info("Syncing data from Postgres to Dune for job: %s", job)

    df = extract_data_from_postgres(env, job)
    return load_data_into_dune(df, env, job)
