from typing import Type, Any

from sqlalchemy import BIGINT, BOOLEAN, VARCHAR, DATE, TIMESTAMP
from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, NUMERIC, ARRAY

DUNE_TO_PG: dict[str, Type[Any]] = {
    "bigint": BIGINT,
    "varbinary": BYTEA,
    "date": DATE,
    "boolean": BOOLEAN,
    "varchar": VARCHAR,
    "double": DOUBLE_PRECISION,
    "timestamp with time zone": TIMESTAMP,
    # TODO: parse these innards more dynamically.
    "decimal(38, 0)": NUMERIC(38, 0),
    "array(varbinary)": ARRAY(BYTEA),
}
