import os
import unittest
from datetime import datetime
from unittest import skip
from unittest.mock import patch

from dune_client.types import QueryParameter, ParameterType

from src.config import Env, RuntimeConfig, parse_query_parameters
from src.jobs import DataSource, JobResolver
from tests import fixtures_root


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

    @skip("this would now be testing the yaml lib")
    def test_load_conf(self):
        config_file = fixtures_root / "basic.config.yaml"
        self.maxDiff = None
        conf = RuntimeConfig.load_from_yaml(config_file.absolute())
        dune_to_local_job_from_conf = conf.dune_to_local_jobs[0]
        self.assertEqual(dune_to_local_job_from_conf.source, DataSource.DUNE.value)
        self.assertEqual(
            dune_to_local_job_from_conf.destination, DataSource.POSTGRES.value
        )
        self.assertEqual(dune_to_local_job_from_conf.if_exists, "replace")
        self.assertEqual(dune_to_local_job_from_conf.poll_frequency, 5)
        self.assertEqual(dune_to_local_job_from_conf.query_engine, "medium")
        self.assertEqual(
            dune_to_local_job_from_conf.table_name, "parameterized_results_4238114"
        )
        self.assertEqual(
            dune_to_local_job_from_conf.query.params[0],
            QueryParameter(
                name="blockchain",
                parameter_type=ParameterType.ENUM,
                value="gnosis",
            ),
        )
        self.assertEqual(
            dune_to_local_job_from_conf.query.params[1],
            QueryParameter(
                name="block_time",
                parameter_type=ParameterType.DATE,
                value=datetime(2024, 9, 1, 0, 0),
            ),
        )
        self.assertEqual(
            dune_to_local_job_from_conf.query.params[2],
            QueryParameter(
                name="result_limit",
                parameter_type=ParameterType.NUMBER,
                value="10",
            ),
        )


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


class TestJobResolver(unittest.TestCase):

    @patch("src.config.load_dotenv")
    @patch.dict(os.environ, {"DUNE_API_KEY": "irrelevant", "DB_URL": "irrelevant"})
    def test_unknown_job_type(self, mock_env):
        conf_file = fixtures_root / "unknown.config.yaml"  # dune to sqlite? not yet
        conf = RuntimeConfig.load_from_yaml(conf_file)

        with self.assertRaises(NotImplementedError):
            JobResolver(mock_env, conf["jobs"][0]).get_job()

    @patch("src.config.load_dotenv")
    @patch.dict(os.environ, {"DUNE_API_KEY": "irrelevant", "DB_URL": "irrelevant"})
    def test_missing_job_parameter(self, mock_env):
        conf_file = fixtures_root / "buggy.config.yaml"  # dune to sqlite? not yet
        conf = RuntimeConfig.load_from_yaml(conf_file)

        with self.assertRaises(KeyError), self.assertLogs(level="ERROR"):
            JobResolver(mock_env, conf["jobs"][0]).get_job()
