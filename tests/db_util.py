from typing import Any
from sqlalchemy import text
import sqlalchemy


def query_pg(engine: sqlalchemy.engine.Engine, query_str: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        result = connection.execute(text(query_str))
        rows = result.fetchall()

    return [
        {
            # Convert memoryview to hex strings: could also use bytes(value)
            key: (f"0x{value.hex()}" if isinstance(value, memoryview) else value)
            for key, value in zip(result.keys(), row)
        }
        for row in rows
    ]
