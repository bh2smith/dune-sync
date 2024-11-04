from typing import Any, Literal

from pandas import DataFrame

TypedDataFrame = tuple[DataFrame, dict[str, Any]]

TableExistsPolicy = Literal["append", "replace"]
DuneQueryEngine = Literal["medium", "large"]
