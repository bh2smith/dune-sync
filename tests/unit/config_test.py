import os
import unittest
from datetime import datetime
from unittest.mock import patch, mock_open

from dune_client.types import QueryParameter

from src.config import Env, RuntimeConfig, parse_query_parameters


class TestEnv(unittest.TestCase):
    @patch(
        "src.config.load_dotenv"
    )  # Mock load_dotenv to prevent loading the actual .env file
    @patch.dict(
        os.environ, {"DUNE_API_KEY": "test_key", "DB_URL": "postgres://localhost/test"}
    )
    def test_load_env_success(self, mock_load_dotenv):
        env = Env.load()
        self.assertEqual(env.dune_api_key, "test_key")
        self.assertEqual(env.db_url, "postgres://localhost/test")

    @patch(
        "src.config.load_dotenv"
    )  # Mock load_dotenv to prevent loading the actual .env file
    @patch.dict(os.environ, {}, clear=True)
    def test_load_env_missing_dune_api_key(self, mock_load_dotenv):
        with self.assertRaises(RuntimeError) as context:
            Env.load()
        self.assertEqual(
            str(context.exception), "DUNE_API_KEY environment variable must be set!"
        )

    @patch(
        "src.config.load_dotenv"
    )  # Mock load_dotenv to prevent loading the actual .env file
    @patch.dict(os.environ, {"DUNE_API_KEY": "test_key"}, clear=True)
    def test_load_env_missing_db_url(self, mock_load_dotenv):
        with self.assertRaises(RuntimeError) as context:
            Env.load()
        self.assertEqual(
            str(context.exception), "DB_URL environment variable must be set!"
        )


class TestRuntimeConfig(unittest.TestCase):
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b"""
        [[jobs]]
        source = "dune"
        destination = "postgres"
        query_id = 123
        table_name = "test_table"
        poll_frequency = 5
        query_engine = "medium"
    """,
    )
    def test_load_from_toml_success(self, mock_file):
        config = RuntimeConfig.load_from_toml("config.toml")
        self.assertEqual(len(config.dune_to_local_jobs), 1)
        job = config.dune_to_local_jobs[0]
        self.assertEqual(job.query.query_id, 123)
        self.assertEqual(job.table_name, "test_table")
        self.assertEqual(job.poll_frequency, 5)
        self.assertEqual(job.query_engine, "medium")

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b"""
        [[jobs]]
        source = "dune"
        destination = "postgres"
        query_id = 123
        table_name = "test_table"
        poll_frequency = 5
        query_engine = "invalid"
    """,
    )
    def test_load_from_toml_invalid_query_engine(self, mock_file):
        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load_from_toml("config.toml")
        self.assertEqual(
            str(context.exception), "query_engine must be either 'medium' or 'large'."
        )

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b"""
        [[jobs]]
        source = "postgres"
        destination = "postgres"
        query_id = 123
        table_name = "test_table"
        poll_frequency = 5
        query_engine = "invalid"
    """,
    )
    def test_load_from_toml_invalid_source_dest_combo(self, mock_file):
        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load_from_toml("config.toml")
        self.assertEqual(
            str(context.exception),
            "Invalid source/destination combination: DataSource.POSTGRES -> DataSource.POSTGRES",
        )

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b"""
        [[jobs]]
        source = "dune"
        destination = "postgres"
        table_name = "test_table"
        query_id = 123
    """,
    )
    def test_load_from_toml_missing_values(self, mock_file):
        config = RuntimeConfig.load_from_toml("config.toml")
        self.assertEqual(len(config.dune_to_local_jobs), 1)
        job = config.dune_to_local_jobs[0]
        self.assertEqual(job.query.query_id, 123)
        self.assertEqual(job.table_name, "test_table")  # Default table name
        self.assertEqual(job.poll_frequency, 1)  # Default poll frequency
        self.assertEqual(job.query_engine, "medium")  # Default query engine

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data=b"""

        [[jobs]]
        source = "postgres"
        destination = "dune"
        table_name = "test_table"
        query_string = "SELECT * FROM test_table"
    """,
    )
    def test_load_from_toml_for_local_to_dune(self, mock_file):
        config = RuntimeConfig.load_from_toml("config.toml")
        self.assertEqual(len(config.dune_to_local_jobs), 0)
        self.assertEqual(len(config.local_to_dune_jobs), 1)
        # job = config.local_to_dune_jobs[0]


class TestParseQueryParameters(unittest.TestCase):

    def test_parse_query_parameters(self):
        params = [
            {"name": "param_text", "type": "TEXT", "value": "sample text"},
            {"name": "param_number", "type": "NUMBER", "value": 42},
            {"name": "param_date", "type": "DATE", "value": "2024-09-01 00:00:00"},
            {"name": "param_enum", "type": "ENUM", "value": "option1"}
        ]

        query_params = parse_query_parameters(params)

        # Assert the number of parsed parameters
        self.assertEqual(len(query_params), 4)

        # Check each parameter type and value
        self.assertEqual(query_params[0], QueryParameter.text_type("param_text", "sample text"))
        self.assertEqual(query_params[1], QueryParameter.number_type("param_number", 42))
        self.assertEqual(
            query_params[2],
            QueryParameter.date_type("param_date", datetime.strptime("2024-09-01 00:00:00", "%Y-%m-%d %H:%M:%S"))
        )
        self.assertEqual(query_params[3], QueryParameter.enum_type("param_enum", "option1"))

    def test_unknown_parameter_type(self):
        params = [{"name": "param_unknown", "type": "UNKNOWN", "value": "some value"}]

        # Expect a ValueError for unknown parameter type
        with self.assertRaises(ValueError) as context:
            parse_query_parameters(params)

        self.assertIn("could not parse", str(context.exception))