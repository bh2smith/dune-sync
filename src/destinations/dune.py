"""Destination logic for Dune Analytics."""

from dune_client.client import DuneClient
from dune_client.models import DuneError

from src.interfaces import Destination, TypedDataFrame
from src.logger import log


class DuneDestination(Destination[TypedDataFrame]):
    """A class representing as Dune as a destination.

    Uses the Dune API to upload CSV data to a table.

    Attributes
    ----------
    client : DuneClient
        Dune Analytics API Interface (initialized via API_KEY)
    table_name : str
        The name of the table where the query results will be stored.

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
        """Upload a DataFrame to Dune as a CSV.

        Returns size of dataframe (i.e. number of "affected" rows).

        Parameters
        ----------
        data : DataFrame
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
            result = self.client.upload_csv(
                self.table_name, data.dataframe.to_csv(index=False)
            )
            if not result:
                raise RuntimeError("Dune Upload Failed")
        except DuneError as dune_e:
            log.error("Dune did not accept our upload: %s", dune_e)
        except (ValueError, RuntimeError) as e:
            log.error("Data processing error: %s", e)
        return len(data)
