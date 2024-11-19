import unittest

from src.job import Database


class DatabaseTests(unittest.TestCase):
    def test_database_resolution(self):
        self.assertEqual(Database.POSTGRES, Database.from_string("postgres"))
        self.assertEqual(Database.DUNE, Database.from_string("dune"))
        self.assertEqual(Database.SQLITE, Database.from_string("sqlite"))
        with self.assertRaises(ValueError) as exc:
            Database.from_string("redis")

        self.assertEqual(f"Unknown Database type: redis", exc.exception.args[0])
