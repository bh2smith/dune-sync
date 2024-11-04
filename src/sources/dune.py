from abc import ABC
from typing import Type, Any

import pandas as pd
from dune_client.client import DuneClient
from dune_client.models import ExecutionResult
from pandas import DataFrame
from sqlalchemy import BIGINT, BOOLEAN, VARCHAR, DATE, TIMESTAMP
from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION

from src.config import DuneToLocalJob
from src.interfaces import Source
from src.types import TypedDataFrame

DUNE_TO_PG: dict[str, Type[Any]] = {
    "bigint": BIGINT,
    "varbinary": BYTEA,
    "date": DATE,
    "boolean": BOOLEAN,
    "varchar": VARCHAR,
    "double": DOUBLE_PRECISION,
    "timestamp with time zone": TIMESTAMP,
    # TODO: parse these innards more dynamically.
    # "decimal(38, 0)": NUMERIC(38, 0),
    # "array(varbinary)": ARRAY(BYTEA),
}


def _reformat_varbinary_columns(
    df: DataFrame, varbinary_columns: list[str]
) -> DataFrame:
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def dune_result_to_df(result: ExecutionResult) -> TypedDataFrame:
    metadata = result.metadata
    dtypes, varbinary_columns = {}, []
    for name, d_type in zip(metadata.column_names, metadata.column_types):
        dtypes[name] = DUNE_TO_PG[d_type]
        if d_type == "varbinary":
            varbinary_columns.append(name)

    df = pd.DataFrame(result.rows)
    # escape bytes
    df = _reformat_varbinary_columns(df, varbinary_columns)
    return df, dtypes


class DuneSource(Source[TypedDataFrame], ABC):
    def __init__(self, api_key: str, job: DuneToLocalJob):
        self.job = job
        self.client = DuneClient(api_key, performance=job.query_engine)

    def validate(self) -> bool:
        # Nothing I can think of to validate here...
        return True

    def fetch(self) -> TypedDataFrame:
        response = self.client.run_query(
            query=self.job.query,
            ping_frequency=self.job.poll_frequency,
        )
        if response.result is None:
            raise ValueError("Query execution failed!")
        return dune_result_to_df(response.result)

    def is_empty(self, data: TypedDataFrame) -> bool:
        return data[0].empty
