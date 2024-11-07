from dune_client.client import DuneClient
from dune_client.models import DuneError
from pandas import DataFrame

from src.interfaces import Destination
from src.logger import log


class DuneDestination(Destination[DataFrame]):
    """
    A class representing as Dune as a destination.
    Uses the Dune API to upload CSV data to a table.

    Attributes
    ----------
    api_key : str
        The API key used for accessing the Dune Analytics API.
    table_name : str
        The name of the table where the query results will be stored.
    """

    def __init__(self, api_key: str, table_name: str):
        self.client = DuneClient(api_key)
        self.table_name: str = table_name

    def save(self, data: DataFrame) -> None:
        try:
            log.debug("Uploading DF to Dune...")
            result = self.client.upload_csv(self.table_name, data.to_csv(index=False))
            log.debug("Uploaded to Dune: %s", result)
            if not result:
                raise RuntimeError("Dune Upload Failed")
        except DuneError as dune_e:
            log.error("Dune did not accept our upload: %s", dune_e)
        except (ValueError, RuntimeError) as e:
            log.error("Data processing error: %s", e)
