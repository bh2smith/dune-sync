from typing import Any

import pandas as pd
import sqlalchemy
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from pandas import DataFrame
from sqlalchemy import create_engine

from src.config import Env, RuntimeConfig
from src.mappings import DUNE_TO_PG
from src.logger import log

DataTypes = dict[str, Any]


def reformat_varbinary_columns(
    df: DataFrame, varbinary_columns: list[str]
) -> DataFrame:
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def fetch_dune_data(
    dune: DuneClient, query: QueryBase, ping_frequency: int
) -> tuple[DataFrame, DataTypes]:
    result = dune.run_query(query, ping_frequency).result
    if result is None:
        raise ValueError("Query execution failed!")

    metadata = result.metadata
    dtypes, varbinary_columns = {}, []
    for name, d_type in zip(metadata.column_names, metadata.column_types):
        dtypes[name] = DUNE_TO_PG[d_type]
        if d_type == "varbinary":
            varbinary_columns.append(name)
        if d_type == "array(varbinary)":
            print(result.rows)
    df = pd.DataFrame(result.rows)
    # escape bytes
    df = reformat_varbinary_columns(df, varbinary_columns)
    return df, dtypes


def save_to_postgres(
    engine: sqlalchemy.engine.Engine, table_name: str, df: DataFrame, dtypes: DataTypes
) -> None:
    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtypes)
    log.info("Data saved to %s successfully!", table_name)


def main() -> None:
    env = Env.load()
    config = RuntimeConfig.load_from_toml("config.toml")
    # TODO: Async job execution https://github.com/bh2smith/dune-sync/issues/20
    for job in config.jobs:
        df, types = fetch_dune_data(
            dune=DuneClient(env.dune_api_key, performance=job.query_engine),
            query=QueryBase(job.query_id),
            ping_frequency=job.poll_frequency,
        )
        if df is not None:
            engine = create_engine(env.db_url)
            save_to_postgres(engine, job.table_name, df, types)


if __name__ == "__main__":
    main()
