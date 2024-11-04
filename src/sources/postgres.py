import pandas as pd
from sqlalchemy import create_engine, text
from pandas import DataFrame
import sqlalchemy
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
    def __init__(self, db_url: str, job: "LocalToDuneJob"):
        self.job = job
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)

    def validate(self) -> bool:
        try:
            # Try to compile the query without executing it
            with self.engine.connect() as connection:
                connection.execute(text("EXPLAIN " + self.job.query_string))
                return True
        except SQLAlchemyError as e:
            log.error("Invalid SQL query: %s", str(e))
            return False

    def fetch(self) -> DataFrame:
        df = pd.read_sql_query(self.job.query_string, con=self.engine)
        return _convert_bytea_to_hex(df)

    def is_empty(self, data: DataFrame) -> bool:
        return data.empty
