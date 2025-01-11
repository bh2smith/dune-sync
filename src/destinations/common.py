"""Common structures used in multiple destination implementations."""

from typing import Literal

TableExistsPolicy = Literal["append", "replace", "upsert", "insert_ignore"]
