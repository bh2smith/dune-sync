from typing import Any
import pandas as pd
from pandas import DataFrame
from dune_client.client import DuneClient
from dune_client.models import ExecutionResult

from src.interfaces import Source
from src.config import DuneToLocalJob
from src.dune_to_local.mappings import DUNE_TO_PG


TypedDataFrame = tuple[DataFrame, dict[str, Any]]


class DuneSource(Source[TypedDataFrame]):
    def __init__(self, api_key: str, job: DuneToLocalJob):
        self.job = job
        self.client = DuneClient(api_key, performance=job.query_engine)

    def validate(self) -> bool:
        # Nothing I can think of to validate here...
        return True

    def fetch(self) -> TypedDataFrame:
        response = self.client.run_query(
            query=self.job.query,
            ping_frequency=self.job.poll_frequency,
        )
        if response.result is None:
            raise ValueError("Query execution failed!")
        return self._process_result(response.result)

    def _process_result(self, result: ExecutionResult) -> TypedDataFrame:
        metadata = result.metadata
        dtypes, varbinary_columns = {}, []
        for name, d_type in zip(metadata.column_names, metadata.column_types):
            dtypes[name] = DUNE_TO_PG[d_type]
            if d_type == "varbinary":
                varbinary_columns.append(name)

        df = pd.DataFrame(result.rows)
        # escape bytes
        df = self._reformat_varbinary_columns(df, varbinary_columns)
        return df, dtypes

    def _reformat_varbinary_columns(
        self, df: DataFrame, varbinary_columns: list[str]
    ) -> DataFrame:
        for col in varbinary_columns:
            df[col] = df[col].apply(
                lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x
            )
        return df
