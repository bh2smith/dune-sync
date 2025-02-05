"""Destination logic for Dune Analytics."""

import io
import sys
from typing import Literal

from dune_client.client import DuneClient
from dune_client.models import DuneError, QueryFailed
from dune_client.query import QueryBase
from dune_client.types import QueryParameter
from pandas import DataFrame

from src.interfaces import Destination, TypedDataFrame
from src.logger import log

InsertionPolicy = Literal["append", "replace", "upload_csv"]


class DuneDestination(Destination[TypedDataFrame]):
    """A class representing as Dune as a destination.

    Uses the Dune API to upload CSV data to a table.

    Attributes
    ----------
    api_key : str
        The API key used for accessing the Dune Analytics API.
    table_name : str
        The name of the table where the query results will be stored.

    [optional]
    request_timeout : int
        Default: 10
        The request timeout for the dune client.

    """

    def __init__(
        self,
        api_key: str,
        table_name: str,
        request_timeout: int,
        insertion_type: InsertionPolicy = "append",
    ):
        self.client = DuneClient(api_key, request_timeout=request_timeout)
        if "." not in table_name:
            raise ValueError("Table name must be in the format namespace.table_name")

        self.table_name: str = table_name
        self.insertion_type: InsertionPolicy = insertion_type
        super().__init__()

    def validate(self) -> bool:
        """Validate the destination setup.

        (currently a placeholder that returns True).
        """
        return True

    def save(self, data: TypedDataFrame) -> int:
        """Upload a TypedDataFrame to Dune as a CSV.

        Returns size of dataframe (i.e. number of "affected" rows).

        Parameters
        ----------
        data : TypedDataFrame
            The data to be uploaded to Dune, which will be converted to CSV format.

        Raises
        ------
        RuntimeError
            If the upload to Dune fails.
        DuneError
            If there's an issue communicating with the Dune API.
        ValueError
            For any data processing issues prior to the upload.

        """
        log.debug("Uploading DF to Dune...")
        if self.insertion_type == "upload_csv":
            if not self._upload_csv(data.dataframe):
                raise RuntimeError("Dune Upload Failed")
        else:
            self._insert(data)
        log.debug("Inserted DF to Dune, %s")

        return len(data.dataframe)

    def _insert(self, data: TypedDataFrame) -> None:
        namespace, table_name = self._get_namespace_and_table_name()
        try:
            if self.insertion_type == "replace":
                log.debug("Clearing table: %s", table_name)
                clear_result = self.client.clear_data(namespace, table_name)
                log.debug("Cleared: %s", clear_result)

            log.info("Inserting to: %s", self.table_name)
            self.client.insert_table(
                namespace,
                table_name,
                data=io.BytesIO(data.dataframe.to_csv(index=False).encode()),
                content_type="text/csv",
            )
        except DuneError as err:
            if "This table was not found" in str(err):
                api_ref = "https://docs.dune.com/api-reference/tables/endpoint/create"
                log.error(
                    "Table %s doesn't exist. See %s for table creation details.",
                    self.table_name,
                    api_ref,
                )
            raise err

    def _upload_csv(self, data: DataFrame) -> bool:
        return self.client.upload_csv(
            self.table_name, data.to_csv(index=False)
        )

    def _table_exists(self) -> bool:
        try:
            results = self.client.run_query(
                QueryBase(
                    4554525,
                    params=[QueryParameter.text_type("table_name", self.table_name)],
                )
            )
            return results.result is not None
        except QueryFailed:
            return False

    def _get_namespace_and_table_name(self) -> tuple[str, str]:
        """Split the namespace, table name from the provided table name."""
        namespace, table_name = self.table_name.split(".")
        return namespace, table_name
