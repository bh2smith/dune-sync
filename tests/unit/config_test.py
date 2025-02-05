import os
import unittest
from datetime import datetime
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
import requests
from dune_client.types import QueryParameter

from src.config import Env, RuntimeConfig
from src.sources.dune import parse_query_parameters
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


class TestRuntimeConfig(unittest.IsolatedAsyncioTestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.env_patcher = patch.dict(
            os.environ,
            {
                "DUNE_API_KEY": "test_key",
                "DB_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
                "Query_ID": "123321",
                "POLL_FREQUENCY_DUNE_PG": "192",
                "BLOCKCHAIN_NAME": "moosis",
                "WHAT_IF_EXISTS": "replace",
                "table_name": "my.pg_table",
            },
            clear=True,
        )
        cls.env_patcher.start()

    @classmethod
    def tearDownClass(cls):
        cls.env_patcher.stop()

    def test_is_url(self):
        # Valid URLs
        assert RuntimeConfig._is_url("https://example.com") is True
        assert RuntimeConfig._is_url("http://localhost:8080") is True
        assert RuntimeConfig._is_url("ftp://files.example.com") is True
        assert RuntimeConfig._is_url("https://api.github.com/path?query=123") is True
        assert RuntimeConfig._is_url("sftp://user:pass@server.com:22") is True

        # Invalid URLs
        assert RuntimeConfig._is_url("not-a-url") is False
        assert RuntimeConfig._is_url("") is False
        assert RuntimeConfig._is_url("file.txt") is False
        assert RuntimeConfig._is_url("/path/to/file") is False
        assert RuntimeConfig._is_url("C:\\Windows\\Path") is False
        assert RuntimeConfig._is_url("://missing-scheme.com") is False
        assert RuntimeConfig._is_url("http://") is False  # Missing netloc

        # Edge cases
        assert RuntimeConfig._is_url(None) is False  # type: ignore
        assert RuntimeConfig._is_url("http:/example.com") is False  # Missing slash
        assert RuntimeConfig._is_url("https:example.com") is False  # Missing slashes

        # Cases that actually trigger exceptions
        assert RuntimeConfig._is_url([1, 2, 3]) is False  # TypeError: list is not str
        assert RuntimeConfig._is_url(123) is False  # TypeError: int is not str

    def test_load_basic_conf(self):
        config_file = config_root / "basic.yaml"
        conf = RuntimeConfig.load(config_file.absolute())
        self.assertEqual(2, len(conf.jobs))
        dune_to_pg_job = conf.jobs[0]
        pg_to_dune_job = conf.jobs[1]
        self.assertEqual("test_key", dune_to_pg_job.source.client.token)
        self.assertEqual(
            "postgresql://postgres:***@localhost:5432/postgres",
            str(pg_to_dune_job.source.engine.url),
        )

    def test_load_templated_conf(self):
        config_file = config_root / "basic_with_env_placeholders.yaml"
        conf = RuntimeConfig.load(config_file.absolute())
        self.assertEqual(2, len(conf.jobs))
        dune_to_pg_job = conf.jobs[0]
        pg_to_dune_job = conf.jobs[1]
        self.assertEqual(int("123321"), dune_to_pg_job.source.query.query_id)
        self.assertEqual(int("192"), dune_to_pg_job.source.poll_frequency)
        self.assertEqual("moosis", dune_to_pg_job.source.query.params[0].value)
        self.assertEqual("replace", dune_to_pg_job.destination.if_exists)
        self.assertEqual("my.pg_table", pg_to_dune_job.destination.table_name)

        config_file = config_root / "basic_with_env_missing_vars.yaml"
        with self.assertRaises(KeyError):
            RuntimeConfig.load(config_file.absolute())

    def test_load_invalid_names(self):
        config_file = config_root / "invalid_names.yaml"
        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load(config_file.absolute())
        self.assertIn(
            "Duplicate job names found in configuration: jobName",
            str(context.exception),
        )

    def test_load_unsupported_conf(self):
        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load(config_root / "unsupported_source.yaml")
        self.assertIn("Unsupported source_db type", str(context.exception))

        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load(config_root / "unsupported_dest.yaml")
        self.assertIn("Unsupported destination_db type", str(context.exception))

    def test_load_buggy_conf(self):
        with self.assertRaises(KeyError) as context:
            RuntimeConfig.load(config_root / "buggy.yaml")
        self.assertIn("'table_name'", str(context.exception))

        with self.assertRaises(SystemExit):
            RuntimeConfig.load(config_root / "unknown_src.yaml")

        with self.assertRaises(SystemExit):
            RuntimeConfig.load(config_root / "unknown_dest.yaml")

        with self.assertRaises(SystemExit):
            RuntimeConfig.load(config_root / "no_data_sources.yaml")

        with self.assertRaises(ValueError):
            RuntimeConfig.load(config_root / "invalid_request_timeout.yaml")

    def test_load_config_url(self):
        # Mock response for successful case
        mock_yaml_content = """
        data_sources:
        - name: test
            type: dune
            key: test_key
        jobs:
        - name: job1
            source:
            ref: test
        """

        # Test successful download
        with (
            patch("requests.get") as mock_get,
            patch("src.config.RuntimeConfig.read_yaml") as mock_read_yaml,
        ):
            # Setup mock response
            mock_response = MagicMock()
            mock_response.text = mock_yaml_content
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response

            mock_read_yaml.return_value = {"test": "data"}

            result = RuntimeConfig._load_config_url("https://example.com/config.yaml")

            # Verify the URL was called with timeout
            mock_get.assert_called_once_with(
                "https://example.com/config.yaml", timeout=10
            )
            # Verify read_yaml was called with StringIO containing our mock content
            mock_read_yaml.assert_called_once()
            assert isinstance(mock_read_yaml.call_args[0][0], StringIO)
            assert result == {"test": "data"}

        # Test HTTP error
        with patch("requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = requests.HTTPError(
                "404 Not Found"
            )
            mock_get.return_value = mock_response

            with pytest.raises(SystemExit, match="Could not download config"):
                RuntimeConfig._load_config_url("https://example.com/config.yaml")

        # Test network error
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.RequestException("Network error")

            with pytest.raises(SystemExit, match="Could not download config"):
                RuntimeConfig._load_config_url("https://example.com/config.yaml")


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
