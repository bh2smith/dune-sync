import unittest
from unittest.mock import AsyncMock, patch

import pytest
from dune_client.query import QueryBase

from src.destinations.postgres import PostgresDestination
from src.job import Database, Job
from src.sources.dune import DuneSource


class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    def test_database_resolution(self):
        self.assertEqual(Database.POSTGRES, Database.from_string("postgres"))
        self.assertEqual(Database.DUNE, Database.from_string("dune"))
        self.assertEqual(Database.SQLITE, Database.from_string("sqlite"))
        with self.assertRaises(ValueError) as exc:
            Database.from_string("redis")

        self.assertEqual("Unknown Database type: redis", exc.exception.args[0])

    def test_job_name_formatting(self):
        src = DuneSource(api_key="f00b4r", query=QueryBase(query_id=1234))
        dest = PostgresDestination(
            db_url="postgresql://postgres:postgres@localhost:5432/postgres",
            table_name="some_table",
        )
        sample_job = Job(name="Move the goats to the pen", source=src, destination=dest)
        self.assertEqual("Move the goats to the pen", str(sample_job))

    @pytest.mark.asyncio
    @patch("src.metrics.push_to_gateway")
    async def test_metrics_collection(self, mock_metrics_push, *_):
        src = DuneSource(api_key="f00b4r", query=QueryBase(query_id=1234))
        dest = PostgresDestination(
            db_url="postgresql://postgres:postgres@localhost:5432/postgres",
            table_name="some_table",
        )
        src.fetch = AsyncMock()
        dest.save = AsyncMock()
        test_job = Job(name="job name", source=src, destination=dest)

        with patch("src.metrics.env", return_value=None):
            await test_job.run()
            mock_metrics_push.assert_not_called()

        with patch("src.metrics.env", return_value="http://localhost:9091"):
            await test_job.run()
            mock_metrics_push.assert_called_once()
            call_kwargs = mock_metrics_push.mock_calls[0].kwargs
            self.assertEqual("http://localhost:9091", call_kwargs["gateway"])
            self.assertEqual("dune-sync-job name", call_kwargs["job"])
