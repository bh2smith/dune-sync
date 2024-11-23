import unittest

from dune_client.query import QueryBase

from src.destinations.postgres import PostgresDestination
from src.job import Database, Job
from src.sources.dune import DuneSource


class DatabaseTests(unittest.TestCase):
    def test_database_resolution(self):
        self.assertEqual(Database.POSTGRES, Database.from_string("postgres"))
        self.assertEqual(Database.DUNE, Database.from_string("dune"))
        self.assertEqual(Database.SQLITE, Database.from_string("sqlite"))
        with self.assertRaises(ValueError) as exc:
            Database.from_string("redis")

        self.assertEqual(f"Unknown Database type: redis", exc.exception.args[0])

    def test_job_name_formatting(self):
        src = DuneSource(api_key="f00b4r", query=QueryBase(query_id=1234))
        dest = PostgresDestination(
            db_url="postgresql://postgres:postgres@localhost:5432/postgres",
            table_name="some_table",
        )
        sample_job = Job(name="Move the goats to the pen", source=src, destination=dest)
        self.assertEqual("Move the goats to the pen", str(sample_job))
