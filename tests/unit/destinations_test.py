import os
import unittest
from unittest.mock import patch

import pandas as pd

from src.destinations.dune import DuneDestination


class DuneDestinationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env_patcher = patch.dict(
            os.environ,
            {
                "DUNE_API_KEY": "test_key",
                "DB_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
            },
            clear=True,
        )
        cls.env_patcher.start()

    @patch("requests.sessions.Session.post")
    @patch("pandas.core.generic.NDFrame.to_csv", name="Fake csv writer")
    def test_ensure_index_disabled_when_uploading(self, mock_to_csv, *_):
        dummy_data = [
            {"foo": "bar"},
            {"baz": "daz"},
        ]
        dummy_df = pd.DataFrame(dummy_data)
        destination = DuneDestination(
            api_key=os.getenv("DUNE_API_KEY"), table_name="foo"
        )
        destination.save(dummy_df)
        mock_to_csv.assert_called_once_with(index=False)
