import pandas as pd
import sqlalchemy
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.interfaces import Source
from src.logger import log


def _convert_bytea_to_hex(df: DataFrame) -> DataFrame:
    if df.empty:
        return df

    for column in df.columns:
        if isinstance(df[column].iloc[0], memoryview):
            df[column] = df[column].apply(lambda x: f"0x{x.tobytes().hex()}")
    return df


class PostgresSource(Source[DataFrame]):
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
        self.query_string = query_string
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)

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
