import unittest

from src.sources.dune import _parse_decimal_type


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
                self.assertNotEqual(
                    tuple([None, None]), _parse_decimal_type(dune_result)
                )
                self.assertEqual(expected_result, _parse_decimal_type(dune_result))

        for invalid in invalid_decimals:
            with self.subTest(msg=invalid):
                self.assertEqual(tuple([None, None]), _parse_decimal_type(invalid))
