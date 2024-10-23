from typing import Any

import pandas as pd
import sqlalchemy
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from pandas import DataFrame
from sqlalchemy import create_engine

from src.config import Env, RuntimeConfig
from src.etl import DataBag, Pipeline
from src.etl import Task
from src.mappings import DUNE_TO_PG

DataTypes = dict[str, Any]

env = Env.load()
bag = DataBag()


def reformat_varbinary_columns() -> DataFrame:
    """
    Escapes byte-columns returned by the API
    :return: pandas.DataFrame
    """
    df = bag.df
    for col in bag.varbin_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)

    bag.df = df
    return df


def save_to_postgres(
    engine: sqlalchemy.engine.Engine, table_name: str, df: DataFrame, dtypes: DataTypes
) -> None:
    df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=dtypes)
    print("Data saved to PostgreSQL successfully!")


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
    df = pd.DataFrame(result.rows)

    bag.varbin_columns = varbinary_columns

    return df, dtypes


def extract_dune_data(config):
    df, types = fetch_dune_data(
        dune=DuneClient(env.dune_api_key, performance=config.query_engine),
        query=QueryBase(config.query_id),
        ping_frequency=config.poll_frequency,
    )
    bag.df = df
    bag.api_types = types


def save_data(config):
    if bag.df is None:
        return

    engine = create_engine(env.db_url)
    save_to_postgres(engine, config.table_name, bag.df, bag.types)


def create_pipeline(config: RuntimeConfig):
    extract_data_task = Task(extract_dune_data, task_args=[config])

    transform_data_task = Task(reformat_varbinary_columns)

    save_data_task = Task(save_data, task_args=[config])

    pipeline = Pipeline()
    pipeline.add(save_data_task, extract_data_task, transform_data_task)
    pipeline.add(transform_data_task, extract_data_task)

    return pipeline
