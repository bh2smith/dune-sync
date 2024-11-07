from abc import ABC
from typing import Type, Any, Literal

import pandas as pd
from dune_client.client import DuneClient
from dune_client.models import ExecutionResult
from dune_client.query import QueryBase
from pandas import DataFrame
from sqlalchemy import BIGINT, BOOLEAN, VARCHAR, DATE, TIMESTAMP
from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, INTEGER

from src.interfaces import Source, TypedDataFrame

DUNE_TO_PG: dict[str, Type[Any]] = {
    "bigint": BIGINT,
    "integer": INTEGER,
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
    """
    A class representing Dune as a data source.

    Attributes
    ----------
    api_key : str
        The API key used for accessing the Dune Analytics API.
    query : QueryBase
        The query to execute.
    """

    def __init__(
        self,
        api_key: str,
        query: QueryBase,
        poll_frequency: int = 1,
        query_engine: Literal["medium", "large"] = "medium",
    ) -> None:
        self.query = query
        self.poll_frequency = poll_frequency
        self.client = DuneClient(api_key, performance=query_engine)

    def fetch(self) -> TypedDataFrame:
        response = self.client.run_query(
            query=self.query,
            ping_frequency=self.poll_frequency,
        )
        if response.result is None:
            raise ValueError("Query execution failed!")
        return dune_result_to_df(response.result)

    def is_empty(self, data: TypedDataFrame) -> bool:
        return data[0].empty
