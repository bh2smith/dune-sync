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
    @patch.dict(os.environ, {"API_KEY": "F00B4R", "MYVAR": "42"}, clear=True)
    def test_env_interpolate(self, mock_load_dotenv):
        self.assertEqual("42", Env.interpolate("$MYVAR"))
        self.assertIs(str, type(Env.interpolate("$MYVAR")))

        self.assertEqual("F00B4R", Env.interpolate("$API_KEY"))
        self.assertEqual("F00B4R", Env.interpolate("${API_KEY}"))

        with self.assertRaises(KeyError) as exc:
            Env.interpolate("$MISSINGVAR")

        self.assertEqual(
            "Environment variable 'MISSINGVAR' not found. ", exc.exception.args[0]
        )

        with self.assertRaises(KeyError) as exc:
            Env.interpolate("${OTHERMISSINGVAR}")

            self.assertEqual(
                "Environment variable 'OTHERMISSINGVAR' not found. ",
                exc.exception.args[0],
            )


class TestRuntimeConfig(unittest.TestCase):
    maxDiff = None

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

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()

    def test_load_basic_conf(self):
        config_file = config_root / "basic.yaml"
        conf = RuntimeConfig.load_from_yaml(config_file.absolute())
        self.assertEqual(2, len(conf.jobs))
        # TODO: come up with more explicit assertions.

    def test_load_unsupported_conf(self):
        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load_from_yaml(config_root / "unsupported_source.yaml")
        self.assertIn("Unsupported source_db type", str(context.exception))

        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load_from_yaml(config_root / "unsupported_dest.yaml")
        self.assertIn("Unsupported destination_db type", str(context.exception))

    def test_load_buggy_conf(self):
        with self.assertRaises(KeyError) as context:
            RuntimeConfig.load_from_yaml(config_root / "buggy.yaml")
        self.assertIn("'table_name'", str(context.exception))

        with self.assertRaises(SystemExit):
            RuntimeConfig.load_from_yaml(config_root / "unknown_src.yaml")

        with self.assertRaises(SystemExit):
            RuntimeConfig.load_from_yaml(config_root / "unknown_dest.yaml")

        with self.assertRaises(SystemExit):
            RuntimeConfig.load_from_yaml(config_root / "no_data_sources.yaml")


class TestParseQueryParameters(unittest.TestCase):

    def test_parse_query_parameters(self):
        params = [
            {"name": "text", "type": "TEXT", "value": "sample text"},
            {"name": "number", "type": "NUMBER", "value": 42},
            {"name": "date", "type": "DATE", "value": "2024-09-01 00:00:00"},
            {"name": "enum", "type": "ENUM", "value": "option1"},
        ]

        self.assertEqual(
            [
                QueryParameter.text_type("text", "sample text"),
                QueryParameter.number_type("number", 42),
                QueryParameter.date_type(
                    "date",
                    datetime.strptime("2024-09-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
                ),
                QueryParameter.enum_type("enum", "option1"),
            ],
            parse_query_parameters(params),
        )

    def test_unknown_parameter_type(self):
        params = [{"name": "param_unknown", "type": "UNKNOWN", "value": "some value"}]

        # Expect a ValueError for unknown parameter type
        with self.assertRaises(ValueError) as context:
            parse_query_parameters(params)

        self.assertIn("could not parse", str(context.exception))
