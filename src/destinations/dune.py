"""Destination logic for Dune Analytics."""

import io

from dune_client.client import DuneClient
from dune_client.models import DuneError

from src.interfaces import Destination, TypedDataFrame
from src.logger import log


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

    def __init__(self, api_key: str, table_name: str, request_timeout: int):
        self.client = DuneClient(api_key, request_timeout=request_timeout)
        self.table_name: str = table_name
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
        try:
            log.debug("Uploading DF to Dune...")
            namespace, table_name = self._get_namespace_and_table_name()
            # TODO check first if table exists? Or warn if it did...
            self.client.create_table(
                namespace,
                table_name,
                schema=[
                    {"name": name, "type": dtype} for name, dtype in data.types.items()
                ],
            )
            result = self.client.insert_table(
                namespace,
                table_name,
                data=io.BytesIO(data.dataframe.to_csv(index=False).encode()),
                content_type="text/csv",
            )
            if not result:
                raise RuntimeError("Dune Upload Failed")
            log.debug("Inserted DF to Dune, %s", result)
        except DuneError as dune_e:
            log.error("Dune did not accept our upload: %s", dune_e)
        except (ValueError, RuntimeError) as e:
            log.error("Data processing error: %s", e)
        return len(data)

    def _get_namespace_and_table_name(self) -> tuple[str, str]:
        """Split the namespace, table name from the provided table name."""
        if "." not in self.table_name:
            raise ValueError("Table name must be in the format namespace.table_name")
        namespace, table_name = self.table_name.split(".")
        return namespace, table_name
