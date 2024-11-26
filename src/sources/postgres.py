"""Source logic for PostgreSQL."""

import asyncio
from pathlib import Path

import pandas as pd
import sqlalchemy
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.env import Env
from src.interfaces import Source
from src.logger import log


def _convert_bytea_to_hex(df: DataFrame) -> DataFrame:
    """Convert PostgreSQL BYTEA columns to hexadecimal string representation.

    This function iterates through the columns of a DataFrame and,
    if a column's first entry is of type `memoryview`, assumes that
    column is of type BYTEA and converts each entry to a hexadecimal
    string prefixed with '0x'.

    Parameters
    ----------
    df : DataFrame
        The DataFrame containing the data to be converted.

    Returns
    -------
    DataFrame
        The modified DataFrame with BYTEA columns converted to hexadecimal strings.

    """
    if df.empty:
        return df

    for column in df.columns:
        if isinstance(df[column].iloc[0], memoryview):
            df[column] = df[column].apply(lambda x: f"0x{x.tobytes().hex()}")
    return df


class PostgresSource(Source[DataFrame]):
    """Represent PostgreSQL as a data source for retrieving data via SQL queries.

    This class connects to a PostgreSQL database using SQLAlchemy and executes a query
    either directly from a string or by reading from a specified `.sql` file.

    Attributes
    ----------
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine instance used for connecting to the PostgreSQL database.
    query_string : str
        The SQL query to execute, which can be directly assigned or read from a file.

    Methods
    -------
    validate() -> bool
        Validates the SQL query by attempting to compile it without execution.
    fetch() -> DataFrame
        Executes the query and returns the result as a DataFrame with any BYTEA columns
        converted to hexadecimal.
    is_empty(data: DataFrame) -> bool
        Checks if the fetched data is empty.
    _set_query_string(query_string: str) -> None
        Sets the query string directly or from a file if the string ends with '.sql'.
    _set_query_string_from_file() -> None
        Reads and sets the query from a specified `.sql` file.

    """

    def __init__(self, db_url: str, query_string: str):
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)
        self.query_string = ""
        self._set_query_string(query_string)
        super().__init__()

    def validate(self) -> bool:
        """Validate the SQL query by attempting to compile it without execution.

        Returns
        -------
        bool
            True if the SQL query is valid; False otherwise.

        Raises
        ------
        SQLAlchemyError
            If the query is invalid or cannot be compiled, an error
            is logged and False is returned.

        """
        try:
            # Try to compile the query without executing it
            with self.engine.connect() as connection:
                connection.execute(text("EXPLAIN " + self.query_string))
                return True
        except SQLAlchemyError as e:
            log.error("Invalid SQL query: %s", str(e))
            return False

    async def fetch(self) -> DataFrame:
        """Execute the SQL query and retrieves the result as a DataFrame.

        Returns
        -------
        DataFrame
            A DataFrame containing the query results, with any BYTEA columns
            converted to hexadecimal format.

        """
        # Using asyncpg or similar async database driver would be better
        # This is a temporary solution using run_in_executor

        loop = asyncio.get_running_loop()
        # consider using an async database driver like asyncpg instead
        # of SQLAlchemy's synchronous interface.
        # The current solution using run_in_executor is a workaround
        # that moves the blocking operation to a thread pool.
        df = await loop.run_in_executor(
            None, lambda: pd.read_sql_query(self.query_string, con=self.engine)
        )
        return _convert_bytea_to_hex(df)

    def is_empty(self, data: DataFrame) -> bool:
        """Check if the provided DataFrame is empty.

        Parameters
        ----------
        data : DataFrame
            The DataFrame to check.

        Returns
        -------
        bool
            True if the DataFrame is empty, False otherwise.

        """
        return data.empty

    def _set_query_string(self, query_string: str) -> None:
        """Set the SQL query string directly or from a file if it ends with '.sql'.

        Parameters
        ----------
        query_string : str
            The SQL query to execute or the path to a `.sql` file containing the query.

        """
        self.query_string = Env.interpolate(query_string)

        if self.query_string.lower().endswith(".sql"):
            self._set_query_string_from_file()

    def _set_query_string_from_file(self) -> None:
        """Read the SQL query from a `.sql` file and sets it as the query string.

        Raises
        ------
        RuntimeError
            If the specified `.sql` file does not exist or is not a file,
            an error is raised.

        """
        sql_source = Path(self.query_string)
        if not sql_source.is_file() or not sql_source.exists():
            raise RuntimeError(
                "Detected directive to include an sql file, "
                f"but it doesn't exist or isn't a file: {sql_source}"
            )

        with open(sql_source, encoding="utf-8") as _handle:
            sql = _handle.read()
            self.query_string = sql
