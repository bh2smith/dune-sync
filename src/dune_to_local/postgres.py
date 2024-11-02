from typing import Any

import pandas as pd
import sqlalchemy
from dune_client.client import DuneClient
from dune_client.models import ExecutionResult
from dune_client.query import QueryBase
from pandas import DataFrame
from sqlalchemy import create_engine

from src.config import DuneToLocalJob, Env, TableExistsPolicy
from src.dune_to_local.mappings import DUNE_TO_PG
from src.logger import log

DataTypes = dict[str, Any]


def reformat_varbinary_columns(
    df: DataFrame, varbinary_columns: list[str]
) -> DataFrame:
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def dune_result_to_df(result: ExecutionResult) -> tuple[DataFrame, dict[str, type]]:
    metadata = result.metadata
    dtypes, varbinary_columns = {}, []
    for name, d_type in zip(metadata.column_names, metadata.column_types):
        dtypes[name] = DUNE_TO_PG[d_type]
        if d_type == "varbinary":
            varbinary_columns.append(name)

    df = pd.DataFrame(result.rows)
    # escape bytes
    df = reformat_varbinary_columns(df, varbinary_columns)
    return df, dtypes


def fetch_dune_data(dune_key: str, job: DuneToLocalJob) -> tuple[DataFrame, DataTypes]:
    dune = DuneClient(dune_key, performance=job.query_engine)
    response = dune.run_query(
        query=QueryBase(
            query_id=job.query_id,
            params=[
                # TODO: https://github.com/bh2smith/dune-sync/issues/30
            ],
        ),
        ping_frequency=job.poll_frequency,
    )
    if response.result is None:
        raise ValueError("Query execution failed!")
    return dune_result_to_df(response.result)


def save_to_postgres(
    engine: sqlalchemy.engine.Engine,
    table_name: str,
    df: DataFrame,
    dtypes: DataTypes,
    if_exists: TableExistsPolicy = "append",
) -> None:
    if df.empty:
        log.warning("DataFrame is empty. Skipping save to PostgreSQL.")
        return
    with engine.connect() as connection:
        df.to_sql(table_name, connection, if_exists=if_exists, index=False, dtype=dtypes)
    log.info("Data saved to %s successfully!", table_name)


def dune_to_postgres(env: Env, job: DuneToLocalJob) -> None:
    df, types = fetch_dune_data(env.dune_api_key, job)
    if not df.empty:
        # Skip engine creation if unnecessary.
        engine = create_engine(env.db_url)
        save_to_postgres(engine, job.table_name, df, types)
    else:
        log.warning("No Query results found! Skipping write")
