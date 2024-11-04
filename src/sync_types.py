from typing import Any

from pandas import DataFrame

TypedDataFrame = tuple[DataFrame, dict[str, Any]]
