"""
Source logic for Dune Analytics.
"""

import json
import re
from abc import ABC
from typing import Type, Any, Literal, List, Tuple

import pandas as pd
from dune_client.client import DuneClient
from dune_client.models import ExecutionResult
from dune_client.query import QueryBase
from pandas import DataFrame
from sqlalchemy import BIGINT, BOOLEAN, VARCHAR, DATE, TIMESTAMP
from sqlalchemy.dialects.postgresql import (
    BYTEA,
    DOUBLE_PRECISION,
    INTEGER,
    NUMERIC,
    JSONB,
)

from src.interfaces import Source, TypedDataFrame
from src.logger import log

DUNE_TO_PG: dict[str, Type[Any] | NUMERIC] = {
    "bigint": BIGINT,
    "integer": INTEGER,
    "varbinary": BYTEA,
    "date": DATE,
    "boolean": BOOLEAN,
    "varchar": VARCHAR,
    "double": DOUBLE_PRECISION,
    "real": DOUBLE_PRECISION,
    "timestamp with time zone": TIMESTAMP,
    "uint256": NUMERIC,
}


def _parse_decimal_type(type_str: str) -> tuple[int, int] | tuple[None, None]:
    """
    Extract precision and scale from Dune's decimal type string like 'decimal(38, 0)'

    Parameters
    ----------
    type_str : str
        The Dune type string returned from the API, like `decimal(38, 0)`

    Returns
    -------
    tuple[int, int]
        Precision and scale as integers, or two Nones if parsing failed
    """
    match = re.match(r"decimal\((\d+),\s*(\d+)\)", type_str)
    if not match:
        return None, None

    precision, scale = match.groups()
    return int(precision), int(scale)


def _reformat_varbinary_columns(
    df: DataFrame, varbinary_columns: list[str]
) -> DataFrame:
    """
    Reformats specified columns in a DataFrame from hexadecimal strings to bytes.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing columns to be reformatted.
    varbinary_columns : list[str]
        A list of column names in the DataFrame that should be converted from
        hexadecimal strings to bytes.

    Returns
    -------
    DataFrame
        The modified DataFrame with specified columns converted to bytes.
    """
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def _reformat_unknown_columns(df: DataFrame, unknown_columns: list[str]) -> DataFrame:
    for col in unknown_columns:
        df[col] = df[col].apply(json.dumps)
    return df


def _handle_column_types(
    name: str,
    d_type: str,
) -> Tuple[Any, List[str], List[str]]:
    """
    Process a single column type and handle special cases.

    Parameters
    ----------
    name : str
        Column name
    d_type : str
        Dune data type

    Returns
    -------
    Tuple[Type[Any], List[str], List[str]]
        Returns a tuple containing:
        - The PostgreSQL type for this column
        - Lists of column names requiring special treatment (varbinary and unknown)
    """
    varbinary_cols = []
    unknown_cols = []

    # Handle decimal types
    if re.match(r"decimal\((\d+),\s*(\d+)\)", d_type):
        precision, scale = _parse_decimal_type(d_type)
        if precision and scale:
            DUNE_TO_PG[d_type] = NUMERIC(precision, scale)
        else:
            log.error("Failed to parse precision and scale from Dune result: %s", name)

    # Get the PostgreSQL type
    pg_type = DUNE_TO_PG.get(d_type)

    # Handle unknown types
    if pg_type is None:
        log.warning("Unknown column: %s - treating as JSONB", d_type)
        unknown_cols.append(name)
        pg_type = JSONB

    # Track varbinary columns
    if d_type == "varbinary":
        varbinary_cols.append(name)

    return pg_type, varbinary_cols, unknown_cols


def dune_result_to_df(result: ExecutionResult) -> TypedDataFrame:
    """
    Converts a Dune query result into a DataFrame with PostgreSQL-compatible data types.

    This function maps Dune's data types to PostgreSQL-compatible types and
    reformats columns of type `varbinary` to bytes for database compatibility.

    Parameters
    ----------
    result : ExecutionResult
        The result of a Dune query, including metadata and row data.

    Returns
    -------
    TypedDataFrame
        A tuple consisting of the DataFrame with the query results and a dictionary
        mapping column names to PostgreSQL-compatible data types.
    """
    metadata = result.metadata
    dtypes = {}
    all_varbinary_cols = []
    all_unknown_cols = []

    for name, d_type in zip(metadata.column_names, metadata.column_types):
        pg_type, varbinary_cols, unknown_cols = _handle_column_types(name, d_type)
        dtypes[name] = pg_type
        all_varbinary_cols.extend(varbinary_cols)
        all_unknown_cols.extend(unknown_cols)

    df = pd.DataFrame(result.rows)
    df = _reformat_varbinary_columns(df, all_varbinary_cols)
    df = _reformat_unknown_columns(df, all_unknown_cols)

    return df, dtypes


class DuneSource(Source[TypedDataFrame], ABC):
    """
    A class representing Dune as a data source for retrieving query results.

    This class interacts with the Dune Analytics API to execute queries and fetch results
    in a DataFrame format, with appropriate data type conversions.

    Attributes
    ----------
    client : DuneClient
        An instance of DuneClient initialized with the API key for connecting to Dune Analytics.
    query : QueryBase
        The query to be executed on Dune Analytics.
    poll_frequency : int
        Frequency in seconds at which the query execution status is polled (default is 1 second).

    Methods
    -------
    validate() -> bool
        Validates the source setup (currently always returns True).
    fetch() -> TypedDataFrame
        Executes the Dune query and retrieves the result as a DataFrame with associated types.
    is_empty(data: TypedDataFrame) -> bool
        Checks if the retrieved data is empty.
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
        super().__init__()

    def validate(self) -> bool:
        # Nothing I can think of to validate here...
        return True

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
