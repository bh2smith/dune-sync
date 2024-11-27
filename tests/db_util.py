from typing import Any

import sqlalchemy
from sqlalchemy import text


def query_pg(engine: sqlalchemy.engine.Engine, query_str: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        result = connection.execute(text(query_str))
        rows = result.fetchall()

    return [
        {
            # Convert memoryview to hex strings: could also use bytes(value)
            key: (f"0x{value.hex()}" if isinstance(value, memoryview) else value)
            for key, value in zip(result.keys(), row, strict=False)
        }
        for row in rows
    ]


def select_star(
    engine: sqlalchemy.engine.Engine, table_name: str
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()

    return [
        {
            # Convert memoryview to hex strings: could also use bytes(value)
            key: (f"0x{value.hex()}" if isinstance(value, memoryview) else value)
            for key, value in zip(result.keys(), row, strict=False)
        }
        for row in rows
    ]


def create_table(engine: sqlalchemy.engine.Engine, table_name: str) -> None:
    with engine.connect() as connection:
        # Begin a transaction explicitly for DDL
        with connection.begin():
            result = connection.execute(
                text(
                    f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL, value TEXT);"
                )
            )
            print("Table created successfully.", result)


def drop_table(engine: sqlalchemy.engine.Engine, table_name: str) -> None:
    with engine.connect() as connection:
        # Begin a transaction explicitly for DDL
        with connection.begin():
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name};"))


def raw_exec(engine: sqlalchemy.engine.Engine, query_str: str) -> None:
    with engine.connect() as connection:
        # Begin a transaction explicitly for DDL
        with connection.begin():
            connection.execute(text(query_str))
