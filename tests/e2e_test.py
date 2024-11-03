import csv
import datetime
import os
import unittest
from os import getenv
from unittest.mock import patch

import pandas.testing
from dune_client.models import ResultsResponse
from pandas import DataFrame
from sqlalchemy import BIGINT, BOOLEAN, VARCHAR, DATE, TIMESTAMP
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import BYTEA

from src.config import Env, RuntimeConfig
from src.dune_to_local.postgres import save_to_postgres, dune_result_to_df
from src.local_to_dune.postgres import postgres_to_dune
from tests import fixtures_root
from tests.db_util import query_pg

DB_URL = getenv("DB_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

SAMPLE_DUNE_RESULTS = ResultsResponse.from_dict(
    {
        "execution_id": "01JB4JWVAFBX4ZDSW79JNGZ99X",
        "query_id": 4159712,
        "is_execution_finished": True,
        "state": "QUERY_STATE_COMPLETED",
        "submitted_at": "2024-10-26T14:15:16.048132Z",
        "expires_at": "2025-01-24T14:15:16.545402Z",
        "execution_started_at": "2024-10-26T14:15:16.400388Z",
        "execution_ended_at": "2024-10-26T14:15:16.5454Z",
        "result": {
            "rows": [
                {
                    "block_date": "2024-09-28",
                    "block_number": 20849352,
                    "block_time": "2024-09-28 13:12:11.000 UTC",
                    "hash": "0x5f0b3f5d3f15bf9943b1b6c77f69",
                    "success": True,
                    "type": "DynamicFee",
                }
            ],
            "metadata": {
                "column_names": [
                    "block_time",
                    "block_number",
                    "success",
                    "hash",
                    "type",
                    "block_date",
                ],
                "column_types": [
                    "timestamp with time zone",
                    "bigint",
                    "boolean",
                    "varbinary",
                    "varchar",
                    "date",
                ],
                "row_count": 1,
                "result_set_bytes": 97,
                "total_row_count": 1,
                "total_result_set_bytes": 97,
                "datapoint_count": 6,
                "pending_time_millis": 352,
                "execution_time_millis": 145,
            },
        },
    }
)

with open(fixtures_root / "simple_dune_upload.csv") as csv_file:
    reader = csv.reader(csv_file)
    data = [line for line in reader]

SAMPLE_CSV_FOR_PANDAS_MOCK = pandas.DataFrame.from_records(data)


class TestEndToEnd(unittest.TestCase):
    def test_dune_results_to_db(self):
        engine = create_engine(DB_URL)
        df, types = dune_result_to_df(SAMPLE_DUNE_RESULTS.result)

        expected = DataFrame(
            {
                "block_date": ["2024-09-28"],
                "block_number": [20849352],
                "block_time": ["2024-09-28 13:12:11.000 UTC"],
                "hash": [b"_\x0b?]?\x15\xbf\x99C\xb1\xb6\xc7\x7fi"],
                "success": [True],
                "type": ["DynamicFee"],
            }
        )
        self.assertIsNone(
            pandas.testing.assert_frame_equal(df, expected, check_dtype=True)
        )
        self.assertEqual(
            types,
            {
                "block_date": DATE,
                "block_number": BIGINT,
                "block_time": TIMESTAMP,
                "hash": BYTEA,
                "success": BOOLEAN,
                "type": VARCHAR,
            },
        )

        save_to_postgres(engine, "test_table", df, types, if_exists="replace")

        self.assertEqual(
            query_pg(engine, "select * from test_table"),
            [
                {
                    "block_date": datetime.date(2024, 9, 28),
                    "block_number": 20849352,
                    "block_time": datetime.datetime(2024, 9, 28, 13, 12, 11),
                    "hash": "0x5f0b3f5d3f15bf9943b1b6c77f69",
                    "success": True,
                    "type": "DynamicFee",
                }
            ],
        )

    @patch(
        "src.local_to_dune.postgres.pd.read_sql_query",
        return_value=SAMPLE_CSV_FOR_PANDAS_MOCK,
    )
    @patch(
        "src.local_to_dune.postgres.DuneClient.upload_csv",
        name="mock_upload",
        return_value=True,
    )
    @patch("src.config.load_dotenv")
    @patch.dict(os.environ, {"DUNE_API_KEY": "test_key", "DB_URL": DB_URL})
    def test_local_postgres_to_dune(self, *_):
        # set up test resources
        env = Env.load()

        conf = RuntimeConfig.load_from_toml(
            fixtures_root / "postgres_to_dune.config.toml"
        )
        job = conf.local_to_dune_jobs[0]

        # this mostly tests pandas and the unittest.mock members, but we may consider making an actual test using a mock
        # Dune service as part of the test orchestration
        res = postgres_to_dune(env, job)

        self.assertTrue(res)
