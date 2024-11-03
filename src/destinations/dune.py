from pandas import DataFrame
from dune_client.client import DuneClient
from dune_client.models import DuneError

from src.interfaces import Destination
from src.logger import log


class DuneDestination(Destination[DataFrame]):
    def __init__(self, api_key: str, table_name: str):
        self.api_key = api_key
        self.client = DuneClient(api_key)
        self.table_name: str = table_name

    def validate(self) -> bool:
        # Nothing I can think of to validate here...
        return True

    def save(self, data: DataFrame) -> None:
        try:
            log.debug("Uploading DF to Dune...")
            result = self.client.upload_csv(self.table_name, data.to_csv())
            log.debug("Uploaded to Dune: %s", result)
        except DuneError as dune_e:
            log.error("Dune did not accept our upload: %s", dune_e)
        except Exception as e:
            log.error("Unexpected error: %s", e)
