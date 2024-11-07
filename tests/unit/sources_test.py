import os
import unittest
from unittest.mock import patch

import pandas as pd
from dune_client.models import ExecutionResult, ResultMetadata
from sqlalchemy import BIGINT
from sqlalchemy.dialects.postgresql import BYTEA

from src.config import RuntimeConfig
from src.sources.dune import _reformat_varbinary_columns, dune_result_to_df
from src.sources.postgres import _convert_bytea_to_hex
from tests import fixtures_root, config_root


class TestSourceUtils(unittest.TestCase):
    def test_reformat_varbinary_columns(self):
        # Test data
        data = {"hex_col": ["0x1234", "0xabcd", None], "normal_col": [1, 2, 3]}
        df = pd.DataFrame(data)

        # Test function
        result = _reformat_varbinary_columns(df, ["hex_col"])

        # Assertions
        expected_bytes = [bytes.fromhex("1234"), bytes.fromhex("abcd"), None]
        assert result["hex_col"].tolist() == expected_bytes
        assert result["normal_col"].tolist() == [1, 2, 3]

    def test_dune_result_to_df(self):
        # Mock ExecutionResult
        metadata = ResultMetadata.from_dict(
            {
                "column_names": ["id", "bytes_data"],
                "column_types": ["bigint", "varbinary"],
                "row_count": 1,
                "result_set_bytes": 97,
                "total_row_count": 1,
                "total_result_set_bytes": 97,
                "datapoint_count": 6,
                "pending_time_millis": 352,
                "execution_time_millis": 145,
            }
        )

        rows = [{"id": 1, "bytes_data": "0x1234"}, {"id": 2, "bytes_data": "0xabcd"}]

        result = ExecutionResult(rows=rows, metadata=metadata)

        df, dtypes = dune_result_to_df(result)

        assert dtypes == {"id": BIGINT, "bytes_data": BYTEA}

        assert df["id"].tolist() == [1, 2]
        assert df["bytes_data"].tolist() == [
            bytes.fromhex("1234"),
            bytes.fromhex("abcd"),
        ]

    def test_convert_bytea_to_hex(self):
        data = {
            "hex_col": [memoryview(b"\x12\x34"), memoryview(b"\xab\xcd")],
            "normal_col": [1, 2],
        }
        df = pd.DataFrame(data)
        result = _convert_bytea_to_hex(df)
        assert result["hex_col"].tolist() == ["0x1234", "0xabcd"]
        assert result["normal_col"].tolist() == [1, 2]


class TestPostgresSource(unittest.TestCase):

    @patch.dict(
        os.environ,
        {
            "DUNE_API_KEY": "test_key",
            "DB_URL": "postgresql://postgres:postgres@localhost:5432/postgres",
        },
        clear=True,
    )
    def test_load_sql_file(self):
        os.chdir(fixtures_root)

        RuntimeConfig.load_from_yaml(config_root / "sql_file.yaml")

        # ensure the missing file really is missing
        missing_file = fixtures_root / "missing-file.sql"
        missing_file.unlink(missing_ok=True)
        with self.assertRaises(RuntimeError):
            RuntimeConfig.load_from_yaml(config_root / "invalid_sql_file.yaml")
