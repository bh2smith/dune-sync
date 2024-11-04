import sqlalchemy
from sqlalchemy import create_engine

from src.interfaces import Destination
from src.sync_types import TableExistsPolicy, TypedDataFrame
from src.logger import log


class PostgresDestination(Destination[TypedDataFrame]):
    """
    A class representing Postgres as a destination.

    Attributes
    ----------
    db_url : str
        The URL of the database connection.
    table_name : str
        The name of the table where the data will be saved.
    """

    def __init__(
        self, db_url: str, table_name: str, if_exists: TableExistsPolicy = "append"
    ):
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)
        self.table_name: str = table_name
        self.if_exists: TableExistsPolicy = if_exists

    def validate(self) -> bool:
        # Nothing I can think of to validate here...
        return True

    def save(
        self,
        data: TypedDataFrame,
    ) -> None:
        df, dtypes = data
        if df.empty:
            log.warning("DataFrame is empty. Skipping save to PostgreSQL.")
            return

        with self.engine.connect() as connection:
            df.to_sql(
                self.table_name,
                connection,
                if_exists=self.if_exists,
                index=False,
                dtype=dtypes,
            )
        log.info("Data saved to %s successfully!", self.table_name)
