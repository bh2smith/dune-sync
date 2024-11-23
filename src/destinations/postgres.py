"""Destination logic for PostgreSQL."""

from typing import Literal

import sqlalchemy
from sqlalchemy import create_engine

from src.interfaces import Destination, TypedDataFrame
from src.logger import log

TableExistsPolicy = Literal["append", "replace"]


class PostgresDestination(Destination[TypedDataFrame]):
    """A class representing PostgreSQL as a destination for data storage.

    This class uses SQLAlchemy to connect to a PostgreSQL database and save data
    to a specified table, with options to handle table existence policies.

    Attributes
    ----------
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine instance for connecting to the PostgreSQL database.
    table_name : str
        The name of the destination table in PostgreSQL where data will be saved.
    if_exists : TableExistsPolicy
        Policy for handling existing tables:
            "fail", "replace", or "append" (default is "append").

    Methods
    -------
    validate() -> bool
        Validates the destination setup (currently always returns True).
    save(data: TypedDataFrame) -> None
        Saves the provided data to the PostgreSQL table,
        creating or appending as specified.

    """

    def __init__(
        self, db_url: str, table_name: str, if_exists: TableExistsPolicy = "append"
    ):
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)
        self.table_name: str = table_name
        self.if_exists: TableExistsPolicy = if_exists
        super().__init__()

    def validate(self) -> bool:
        """Validate the destination setup.

        (currently a placeholder that returns True).
        """
        return True

    def save(
        self,
        data: TypedDataFrame,
    ) -> None:
        """Save the provided DataFrame to the PostgreSQL database table.

        Parameters
        ----------
        data : TypedDataFrame
            A tuple containing the DataFrame to save and its corresponding
            SQLAlchemy column types.

        Raises
        ------
        sqlalchemy.exc.SQLAlchemyError
            If there is an error while connecting or saving data to Postgres.
        Warning
            If the DataFrame is empty, a warning is logged, and no data is saved.

        """
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
