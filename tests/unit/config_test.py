import os
import unittest
from datetime import datetime
from unittest import skip
from unittest.mock import patch, mock_open

from dune_client.types import QueryParameter, ParameterType

from src.config import Env, RuntimeConfig, parse_query_parameters, DataSource
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

    @skip("migrating to yaml")
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

    @skip("migrating to yaml")
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

    @skip("migrating to yaml")
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
    @skip("migrating to yaml")
    def test_load_from_toml_invalid_source_dest_combo(self, mock_file):
        with self.assertRaises(ValueError) as context:
            RuntimeConfig.load_from_toml("config.toml")
        self.assertEqual(
            str(context.exception),
            "Invalid source/destination combination: DataSource.POSTGRES -> DataSource.POSTGRES",
        )

    @skip("migrating to yaml")
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

    @skip("migrating to yaml")
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
