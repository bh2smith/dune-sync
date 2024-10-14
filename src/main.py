import os
from typing import Any

import pandas as pd
import sqlalchemy
from pandas import DataFrame
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from sqlalchemy import create_engine
from dotenv import load_dotenv

from src.mappings import DUNE_TO_PG

DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
DB_URL = os.environ.get("DB_URL")

# TODO(bh2smith): parse config file for most of the following stuff
QUERY_ID = os.environ.get("QUERY_ID")
POLL_FREQUENCY = int(os.environ.get("POLL_FREQUENCY", 1))
QUERY_ENGINE = os.environ.get("QUERY_ENGINE", "medium")
TABLE_NAME = f"dune_data_{QUERY_ID}"

DataTypes = dict[str, Any]


def reformat_varbinary_columns(
    df: DataFrame, varbinary_columns: list[str]
) -> DataFrame:
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def fetch_dune_data(dune: DuneClient, query: QueryBase) -> tuple[DataFrame, DataTypes]:
    result = dune.run_query(query, ping_frequency=POLL_FREQUENCY).result
    if result is None:
        raise ValueError("Query execution failed!")

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


def save_to_postgres(
    engine: sqlalchemy.engine.Engine, df: DataFrame, dtypes: DataTypes
) -> None:
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, dtype=dtypes)
    print("Data saved to PostgreSQL successfully!")


def main() -> None:
    load_dotenv()
    if DUNE_API_KEY is None:
        raise EnvironmentError("DUNE_API_KEY environment variable must be set!")
    if QUERY_ID is None:
        raise EnvironmentError("QUERY_ID must be set")

    df, types = fetch_dune_data(
        dune=DuneClient(DUNE_API_KEY, performance=QUERY_ENGINE),
        query=QueryBase(int(QUERY_ID)),
    )
    if df is not None:
        engine = create_engine(DB_URL)
        save_to_postgres(engine, df, types)


if __name__ == "__main__":
    main()
