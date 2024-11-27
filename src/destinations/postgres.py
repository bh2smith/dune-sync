"""Destination logic for PostgreSQL."""

from typing import Literal

import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.dialects.postgresql import insert

from src.interfaces import Destination, TypedDataFrame
from src.logger import log

TableExistsPolicy = Literal["append", "replace", "upsert", "insert_ignore"]


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
            "replace", "append", "upsert", "insert_ignore" (default is "append").

    Methods
    -------
    validate() -> bool
        Validates the destination setup (currently always returns True).
    save(data: TypedDataFrame) -> None
        Saves the provided data to the PostgreSQL table,
        creating or appending as specified.

    """

    def __init__(
        self,
        db_url: str,
        table_name: str,
        if_exists: TableExistsPolicy = "append",
        index_columns: list[str] | None = None,
    ):
        if index_columns is None:
            index_columns = []
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)
        self.table_name: str = table_name
        self.if_exists: TableExistsPolicy = if_exists
        # List of column forming the ON CONFLICT condition.
        # Only relevant for "upsert" TableExistsPolicy
        self.index_columns: list[str] = index_columns

        super().__init__()

    def validate(self) -> bool:
        """Validate the destination setup."""
        if self.if_exists == "upsert" and len(self.index_columns) == 0:
            log.error("Upsert without index columns.")
            return False
        return True

    def validate_unique_constraints(self) -> None:
        """Validate table has unique or exclusion constraint for index columns."""
        inspector = inspect(self.engine)
        constraints = inspector.get_unique_constraints(self.table_name)
        index_columns_set = set(self.index_columns)

        for constraint in constraints:
            if index_columns_set == set(constraint["column_names"]):
                return  # Found a matching unique constraint!

        table, columns = self.table_name, self.index_columns
        index_columns_str = ", ".join(columns)
        constraint_name = f"{self.table_name}_{'_'.join(columns)}_unique"
        suggestion = (
            f"ALTER TABLE {table} ADD CONSTRAINT "
            f"{constraint_name} UNIQUE ({index_columns_str});"
        )
        message = (
            "The ON CONFLICT clause requires a unique or exclusion constraint "
            f"on the column(s): {index_columns_str}. "
            f"Please ensure the table '{table}' has the necessary constraint. "
            f"To fix this, you can run the following SQL command:\n{suggestion}"
        )
        # Log or print the error message
        log.error(message)
        raise ValueError("No unique or exclusion constraint found. See error logs.")

    def table_exists(self) -> bool:
        """Check if a table exists in the database.

        :return: True if the table exists, False otherwise.
        """
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        return self.table_name in tables

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
        df, _ = data
        if df.empty:
            log.warning("DataFrame is empty. Skipping save to PostgreSQL.")
            return
        match self.if_exists:
            case "upsert":
                self.insert(data, on_conflict="update")
            case "insert_ignore":
                self.insert(data, on_conflict="nothing")
            case "append":
                self.append(data)
            case "replace":
                self.replace(data)
            case _:
                raise ValueError(f"Invalid if_exists policy: {self.if_exists}")
        log.info("Data saved to %s successfully!", self.table_name)

    def replace(
        self,
        data: TypedDataFrame,
    ) -> None:
        """Replace the table with the provided data."""
        df, dtypes = data
        with self.engine.connect() as connection:
            df.to_sql(
                self.table_name,
                connection,
                if_exists="replace",
                index=False,
                dtype=dtypes,
            )

    def append(
        self,
        data: TypedDataFrame,
    ) -> None:
        """Append data to the table."""
        df, dtypes = data
        with self.engine.connect() as connection:
            df.to_sql(
                self.table_name,
                connection,
                if_exists="append",
                index=False,
                dtype=dtypes,
            )

    # def upsert(self, data: TypedDataFrame) -> None:
    #     """Upsert data from a DataFrame into a SQL table.

    #     :param data: Typed pandas DataFrame containing the data to upsert.
    #     """
    #     if not self.table_exists():
    #         # Do append.
    #         self.append(data)
    #         return

    #     self.validate_unique_constraints()
    #     df, _ = data
    #     # Get all column names from the DataFrame
    #     columns = df.columns.tolist()

    #     metadata = MetaData()
    #     table = Table(self.table_name, metadata, autoload_with=self.engine)
    #     statement = insert(table).values(**{col: df[col] for col in columns})

    #     statement = statement.on_conflict_do_update(
    #         index_elements=self.index_columns,
    #         set_={col: getattr(statement.excluded, col) for col in columns},
    #     )
    #     records = df.to_dict(orient="records")
    #     with self.engine.connect() as conn:
    #         with conn.begin():
    #             conn.execute(statement, records)

    def insert(
        self, data: TypedDataFrame, on_conflict: Literal["update", "nothing"]
    ) -> None:
        """Insert data from a DataFrame into a SQL table.

        :param data: Typed pandas DataFrame containing the data to upsert.
        """
        if not self.table_exists():
            # Do append.
            self.append(data)
            return

        self.validate_unique_constraints()
        df, _ = data
        # Get all column names from the DataFrame
        columns = df.columns.tolist()

        metadata = MetaData()
        table = Table(self.table_name, metadata, autoload_with=self.engine)
        statement = insert(table).values(**{col: df[col] for col in columns})

        if on_conflict == "update":
            statement = statement.on_conflict_do_update(
                index_elements=self.index_columns,
                set_={col: getattr(statement.excluded, col) for col in columns},
            )
        else:  # nothing
            statement = statement.on_conflict_do_nothing(
                index_elements=self.index_columns,
            )
        records = df.to_dict(orient="records")
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(statement, records)
