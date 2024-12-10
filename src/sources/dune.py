"""Source logic for Dune Analytics."""

from __future__ import annotations

import json
import re
from abc import ABC
from typing import Any, Literal

import pandas as pd
from dune_client.client_async import AsyncDuneClient
from dune_client.models import ExecutionResult
from dune_client.query import QueryBase
from dune_client.types import ParameterType, QueryParameter
from pandas import DataFrame
from sqlalchemy import BIGINT, BOOLEAN, DATE, TIMESTAMP, VARCHAR
from sqlalchemy.dialects.postgresql import (
    BYTEA,
    DOUBLE_PRECISION,
    INTEGER,
    JSONB,
    NUMERIC,
)

from src.interfaces import Source, TypedDataFrame
from src.logger import log

DECIMAL_PATTERN = r"decimal\((\d+),\s*(\d+)\)"
VARCHAR_PATTERN = r"varchar\((\d+)\)"

DUNE_TO_PG: dict[str, type[Any] | NUMERIC] = {
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


def _parse_varchar_type(type_str: str) -> int | None:
    """Extract the length from Dune's varchar type string like varchar(255).

    Parameters
    ----------
    type_str : str
        The Dune type string returned from the API, like `varchar(255)`

    Returns
    -------
    int | None
        Length as an integer, or None if parsing failed.

    """
    match = re.match(VARCHAR_PATTERN, type_str)
    if not match:
        return None

    length = match.group(1)
    return int(length)


def _parse_decimal_type(type_str: str) -> tuple[int, int] | tuple[None, None]:
    """Extract precision and scale from Dune's decimal type string like decimal(38, 0).

    Parameters
    ----------
    type_str : str
        The Dune type string returned from the API, like `decimal(38, 0)`

    Returns
    -------
    tuple[int, int]
        Precision and scale as integers, or two Nones if parsing failed

    """
    match = re.match(DECIMAL_PATTERN, type_str)
    if not match:
        return None, None

    precision, scale = match.groups()
    return int(precision), int(scale)


def _reformat_varbinary_columns(
    df: DataFrame, varbinary_columns: list[str]
) -> DataFrame:
    """Reformats specified columns in a DataFrame from hexadecimal strings to bytes.

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
) -> tuple[Any, list[str], list[str]]:
    """Process a single column type and handle special cases.

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

    # Handle varchar types
    if re.match(VARCHAR_PATTERN, d_type):
        length = _parse_varchar_type(d_type)
        if length is not None:
            # TODO(bh2smith) is it worth specifying the length?
            DUNE_TO_PG[d_type] = VARCHAR
        else:
            log.error("Failed to parse precision and scale from Dune result: %s", name)

    # Handle decimal types
    if re.match(DECIMAL_PATTERN, d_type):
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
    """Convert a Dune query result into a DataFrame with Postgres-compatible data types.

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
    varbinary_cols = []
    unknown_cols = []

    for name, d_type in zip(metadata.column_names, metadata.column_types, strict=False):
        pg_type, _varbinary_cols, _unknown_cols = _handle_column_types(name, d_type)
        dtypes[name] = pg_type
        varbinary_cols.extend(_varbinary_cols)
        unknown_cols.extend(_unknown_cols)

    df = pd.DataFrame(result.rows)
    df = _reformat_varbinary_columns(df, varbinary_cols)
    df = _reformat_unknown_columns(df, unknown_cols)

    return TypedDataFrame(df, dtypes)


class DuneSource(Source[TypedDataFrame], ABC):
    """A class representing Dune as a data source for retrieving query results.

    This class interacts with the Dune Analytics API to execute queries and
    fetch results in a DataFrame format, with appropriate data type conversions.

    Attributes
    ----------
    client : DuneClient
        An instance of DuneClient initialized with the API key for connecting to
        Dune Analytics.
    query : QueryBase
        The query to be executed on Dune Analytics.
    poll_frequency : int
        Frequency in seconds at which the query execution status is polled
        (default is 1 second).

    Methods
    -------
    validate() -> bool
        Validates the source setup (currently always returns True).
    fetch() -> TypedDataFrame
        Executes the Dune query and retrieves the result as a DataFrame
        with associated types.
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
        self.client = AsyncDuneClient(api_key, performance=query_engine)
        super().__init__()

    def validate(self) -> bool:
        """Validate the configuration."""
        # Nothing I can think of to validate here...
        return True

    async def fetch(self) -> TypedDataFrame:
        """Fetch data from the source."""
        # TODO(dune-client): Update Async Dune Client with "run_query" method.
        await self.client.connect()
        response = await self.client.refresh(
            query=self.query,
            ping_frequency=self.poll_frequency,
        )
        await self.client.disconnect()
        if response.result is None:
            raise ValueError("Query execution failed!")
        return dune_result_to_df(response.result)

    def is_empty(self, data: TypedDataFrame) -> bool:
        """Check if the provided DataFrame is empty."""
        return data.is_empty()


def parse_query_parameters(params: list[dict[str, Any]]) -> list[QueryParameter]:
    """Convert a list of parameter dictionaries into Dune query parameters.

    Args:
        params (list[dict[str, Any]]): List of parameter dictionaries, each containing:
            - name: Parameter name
            - type: Parameter type (TEXT, NUMBER, DATE, or ENUM)
            - value: Parameter value

    Returns:
        list[QueryParameter]: List of properly typed Dune query parameters

    Raises:
        ValueError: If an unknown parameter type is encountered

    """
    query_params = []
    for param in params:
        name = param["name"]
        param_type = ParameterType.from_string(param["type"])
        value = param["value"]

        if param_type == ParameterType.TEXT:
            query_params.append(QueryParameter.text_type(name, value))
        elif param_type == ParameterType.NUMBER:
            query_params.append(QueryParameter.number_type(name, value))
        elif param_type == ParameterType.DATE:
            query_params.append(QueryParameter.date_type(name, value))
        elif param_type == ParameterType.ENUM:
            query_params.append(QueryParameter.enum_type(name, value))
        else:
            # Can't happen.
            # this code is actually unreachable because the case it handles
            # causes an exception to be thrown earlier, in ParameterType.from_string()
            raise ValueError(
                f"Unknown parameter type: {param['type']}"
            )  # pragma: no cover

    return query_params
