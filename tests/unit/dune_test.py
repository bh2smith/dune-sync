import json
import unittest
from logging import ERROR
from unittest.mock import patch

from pandas import DataFrame
from sqlalchemy import BIGINT, INTEGER
from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, JSONB

from src.sources.dune import (
    _handle_column_types,
    _parse_decimal_type,
    _reformat_unknown_columns,
)


class DuneSourceTest(unittest.TestCase):
    def test_parse_decimal_type(self):
        valid_decimals = [
            ["decimal(1,0)", (1, 0)],
            ["decimal(2, 10)", (2, 10)],
            ["decimal(3, 10)", (3, 10)],
        ]
        invalid_decimals = [
            "float",
            "real",
            "decimal(",
            "decimal()",
            "decimal(2)",
        ]

        for valid in valid_decimals:
            with self.subTest(msg=valid):
                dune_result, expected_result = valid[0], valid[1]
                self.assertEqual(expected_result, _parse_decimal_type(dune_result))

        for invalid in invalid_decimals:
            with self.subTest(msg=invalid):
                self.assertEqual((None, None), _parse_decimal_type(invalid))

    def test__handle_column_types(self):
        self.assertEqual(
            (DOUBLE_PRECISION, [], []), _handle_column_types("real_col", "real")
        )
        # decimal(x,y) is handled in a separate test
        self.assertEqual((BIGINT, [], []), _handle_column_types("bigint_col", "bigint"))
        self.assertEqual((INTEGER, [], []), _handle_column_types("int_col", "integer"))
        self.assertEqual(
            (BYTEA, ["byte_col"], []),
            _handle_column_types("byte_col", "varbinary"),
        )
        self.assertEqual(
            (JSONB, [], ["arr_col"]),
            _handle_column_types("arr_col", "unknown_type"),
        )
        with (
            patch("src.sources.dune._parse_decimal_type") as _mock_decimal_type,
            self.assertLogs(level=ERROR) as logs,
        ):
            _mock_decimal_type.return_value = [None, None]
            self.assertEqual(
                (JSONB, [], ["dec_col"]),
                _handle_column_types("dec_col", "decimal(12, 2222)"),
            )
        self.assertIn(
            "Failed to parse precision and scale from Dune result: dec_col",
            logs.output[0],
        )

    def test__reformat_unknown_columns(self):
        df = DataFrame([[{"key": "value"}, 1]], columns=["A", "B"])
        unknown_columns = ["A"]

        result_df = _reformat_unknown_columns(df, unknown_columns)

        self.assertFalse(result_df.empty)
        self.assertEqual(result_df["A"].iloc[0], json.dumps({"key": "value"}))
