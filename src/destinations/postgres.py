"""Destination logic for PostgreSQL."""

from typing import Literal

import sqlalchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text

from src.interfaces import Destination, TypedDataFrame
from src.logger import log

TableExistsPolicy = Literal["append", "replace", "upsert"]


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
            "replace", "append", "upsert" (default is "append").

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
        conflict_columns: list[str] | None = None,
    ):
        if conflict_columns is None:
            conflict_columns = []
        self.engine: sqlalchemy.engine.Engine = create_engine(db_url)
        self.table_name: str = table_name
        self.if_exists: TableExistsPolicy = if_exists
        # List of column forming the ON CONFLICT condition.
        # Only relevant for "upsert" TableExistsPolicy
        self.conflict_columns: list[str] = conflict_columns

        super().__init__()

    def validate(self) -> bool:
        """Validate the destination setup."""
        if self.if_exists == "upsert" and len(self.conflict_columns) == 0:
            log.error("Upsert without conflict columns.")
            return False
        return True

    def validate_unique_constraints(self) -> None:
        """Validate table has unique or exclusion constraint for conflict columns."""
        inspector = inspect(self.engine)
        constraints = inspector.get_unique_constraints(self.table_name)
        conflict_columns_set = set(self.conflict_columns)

        for constraint in constraints:
            if conflict_columns_set == set(constraint["column_names"]):
                return  # Found a matching unique constraint!

        table, columns = self.table_name, self.conflict_columns
        conflict_columns_str = ", ".join(columns)
        constraint_name = f"{self.table_name}_{'_'.join(columns)}_unique"
        suggestion = (
            f"ALTER TABLE {table} ADD CONSTRAINT "
            f"{constraint_name} UNIQUE ({conflict_columns_str});"
        )
        message = (
            f"The ON CONFLICT clause requires a unique or exclusion constraint "
            f"on the column(s): {conflict_columns_str}. "
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
                self.upsert(data)
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

    def upsert(self, data: TypedDataFrame) -> None:
        """Upsert data from a DataFrame into a SQL table.

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

        # Generate the column list for the INSERT clause
        column_list = ", ".join(columns)

        # Generate the parameterized values for the INSERT clause
        value_placeholders = ", ".join(f":{col}" for col in columns)

        # Generate the update assignments for the ON CONFLICT clause
        update_assignments = ", ".join(
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col not in self.conflict_columns
        )

        # Define the insert statement with an ON CONFLICT clause
        insert_stmt = f"""
        INSERT INTO {self.table_name} ({column_list})
        VALUES ({value_placeholders})
        ON CONFLICT ({', '.join(self.conflict_columns)}) DO UPDATE SET
            {update_assignments};
        """

        # Convert the DataFrame to a list of dictionaries for SQLAlchemy to use
        records = df.to_dict(orient="records")

        # Debugging: Print statement for confirmation
        print("Insert Statement:\n", insert_stmt)
        print("Records:\n", records)

        # Execute the upsert
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text(insert_stmt), records)
