from pathlib import Path

import pandas as pd
import sqlalchemy
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.interfaces import Source, Validate
from src.logger import log


def _convert_bytea_to_hex(df: DataFrame) -> DataFrame:
    if df.empty:
        return df

    for column in df.columns:
        if isinstance(df[column].iloc[0], memoryview):
            df[column] = df[column].apply(lambda x: f"0x{x.tobytes().hex()}")
    return df


class PostgresSource(Validate, Source[DataFrame]):
    """
    A class representing Postgres as a data source.

    Attributes
    ----------
    db_url : str
        The URL of the database connection.
    query_string : str
        The SQL query to execute.
    """

    def __init__(self, db_url: str, query_string: str):
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)
        self.query_string = ""
        self._set_query_string(query_string)
        super().__init__()

    def validate(self) -> bool:
        try:
            # Try to compile the query without executing it
            with self.engine.connect() as connection:
                connection.execute(text("EXPLAIN " + self.query_string))
                return True
        except SQLAlchemyError as e:
            log.error("Invalid SQL query: %s", str(e))
            return False

    def fetch(self) -> DataFrame:
        df = pd.read_sql_query(self.query_string, con=self.engine)
        return _convert_bytea_to_hex(df)

    def is_empty(self, data: DataFrame) -> bool:
        return data.empty

    def _set_query_string(self, query_string: str) -> None:
        self.query_string = query_string

        if self.query_string.lower().endswith(".sql"):
            self._set_query_string_from_file()

    def _set_query_string_from_file(self) -> None:
        sql_source = Path(self.query_string)
        if not sql_source.is_file() or not sql_source.exists():
            raise RuntimeError(
                "Detected directive to include an sql file, "
                f"but it doesn't exist or isn't a file: {sql_source}"
            )

        with open(sql_source, "r", encoding="utf-8") as _handle:
            sql = _handle.read()
            self.query_string = sql
