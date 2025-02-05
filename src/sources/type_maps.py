"""Data Type mappings."""

from __future__ import annotations

from typing import Any

from sqlalchemy import BIGINT, BOOLEAN, DATE, TIMESTAMP, VARCHAR
from sqlalchemy.dialects.postgresql import (
    BYTEA,
    DOUBLE_PRECISION,
    INTEGER,
    NUMERIC,
)

DECIMAL_PATTERN = r"decimal\((\d+),\s*(\d+)\)"
VARCHAR_PATTERN = r"varchar\((\d+)\)"

DUNE_TO_PG: dict[str, type[Any] | NUMERIC] = {
    "bigint": BIGINT,
    "integer": INTEGER,
    "varbinary": BYTEA,
    "date": DATE,
    "boolean": BOOLEAN,
    "varchar": VARCHAR,
    "double": DOUBLE_PRECISION,
    "real": DOUBLE_PRECISION,
    "timestamp with time zone": TIMESTAMP,
    "uint256": NUMERIC,
}

# https://docs.dune.com/api-reference/tables/endpoint/create#body-schema-type
# This map is not a perfect inverse of the one above.
# 1. Notice `DOUBLE_PRECISION` has two pre-images: we chose double
# 2. timestamp with time zone not aligned with timestamp
# 3. Apparently no JSONB support here.
PG_TO_DUNE: dict[int, str] = {
    16: "boolean",
    17: "varbinary",
    20: "bigint",
    21: "bigint",  # smallint
    23: "integer",
    25: "varchar",
    # 26: "oid",
    700: "double",
    701: "double",
    1042: "varchar",
    1043: "varchar",
    1082: "timestamp",
    1114: "timestamp",  # This doesn't match with above
    1700: "uint256",
}
