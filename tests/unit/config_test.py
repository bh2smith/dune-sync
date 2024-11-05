import os
import unittest
from datetime import datetime
from unittest.mock import patch

from dune_client.types import QueryParameter

from src.config import Env, RuntimeConfig, parse_query_parameters
from tests import config_root


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
    maxDiff = None

    @patch.dict(
        os.environ,
        {
            "DUNE_API_KEY": "test_key",
            "DB_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
        },
        clear=True,
    )
    def test_load_conf(self):
        config_file = config_root / "basic.yaml"
        self.maxDiff = None
        conf = RuntimeConfig.load_from_yaml(config_file.absolute())
        self.assertEqual(len(conf.jobs), 2)
        # TODO: come up with more explicit assertions.


class TestParseQueryParameters(unittest.TestCase):

    def test_parse_query_parameters(self):
        params = [
            {"name": "text", "type": "TEXT", "value": "sample text"},
            {"name": "number", "type": "NUMBER", "value": 42},
            {"name": "date", "type": "DATE", "value": "2024-09-01 00:00:00"},
            {"name": "enum", "type": "ENUM", "value": "option1"},
        ]

        self.assertEqual(
            parse_query_parameters(params),
            [
                QueryParameter.text_type("text", "sample text"),
                QueryParameter.number_type("number", 42),
                QueryParameter.date_type(
                    "date",
                    datetime.strptime("2024-09-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                ),
                QueryParameter.enum_type("enum", "option1"),
            ],
        )

    def test_unknown_parameter_type(self):
        params = [{"name": "param_unknown", "type": "UNKNOWN", "value": "some value"}]

        # Expect a ValueError for unknown parameter type
        with self.assertRaises(ValueError) as context:
            parse_query_parameters(params)

        self.assertIn("could not parse", str(context.exception))
