import os
import unittest
from logging import ERROR, WARNING
from unittest.mock import patch

import pandas as pd
from dune_client.models import DuneError

from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination


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

    @patch("dune_client.api.table.TableAPI.upload_csv", name="Fake CSV uploader")
    def test_dune_error_handling(self, mock_dune_upload_csv):
        dest = DuneDestination(api_key="f00b4r", table_name="foo")
        df = pd.DataFrame([{"foo": "bar"}])

        dune_err = DuneError(
            data={"error": "bad stuff"},
            response_class="response",
            err=KeyError("you missed something"),
        )
        val_err = ValueError("Oops")
        runtime_err = RuntimeError("Big Oops")

        mock_dune_upload_csv.side_effect = dune_err

        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()

        # does this shit really look better just because it's < 88 characters long?
        exmsg = (
            "Dune did not accept our upload: "
            "Can't build response from {'error': 'bad stuff'}"
        )
        self.assertIn(exmsg, logs.output[0])

        mock_dune_upload_csv.reset_mock()
        mock_dune_upload_csv.side_effect = val_err

        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()
        expected_message = "Data processing error: Oops"
        self.assertIn(expected_message, logs.output[0])

        mock_dune_upload_csv.reset_mock()
        mock_dune_upload_csv.side_effect = runtime_err
        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()
        expected_message = "Data processing error: Big Oops"
        self.assertIn(expected_message, logs.output[0])

        mock_dune_upload_csv.reset_mock()

        # TIL: reset_mock() doesn't clear side effects....
        mock_dune_upload_csv.side_effect = None

        mock_dune_upload_csv.return_value = None

        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()
        self.assertIn("Dune Upload Failed", logs.output[0])


class PostgresDestinationTest(unittest.TestCase):
    def test_saving_empty_df(self):
        pg_dest = PostgresDestination(
            db_url="postgresql://postgres:postgres@localhost:5432/postgres",
            table_name="foo",
        )
        df = pd.DataFrame([])
        with self.assertLogs(level=WARNING) as logs:
            pg_dest.save((df, {"numeric": "int?"}))

        self.assertIn(
            "DataFrame is empty. Skipping save to PostgreSQL.", logs.output[0]
        )
