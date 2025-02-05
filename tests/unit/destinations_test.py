import os
import unittest
from logging import DEBUG, ERROR, WARNING
from unittest.mock import Mock, patch

import pandas as pd
import sqlalchemy
from dune_client.models import DuneError, QueryFailed

from src.destinations.dune import DuneDestination
from src.destinations.postgres import PostgresDestination
from src.interfaces import TypedDataFrame
from tests.db_util import create_table, drop_table, raw_exec, select_star


class DuneDestinationTest(unittest.TestCase):
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

    def test_init_validation(self):
        with self.assertRaises(ValueError) as ctx:
            DuneDestination(
                api_key="anything",
                table_name="INVALID_TABLE_NAME",
                request_timeout=10,
                insertion_type="replace",
            )
        self.assertIn(
            "Table name must be in the format namespace.table_name",
            ctx.exception.args[0],
        )

    def test_table_exists(self):
        mock_client = Mock()

        dest = DuneDestination(
            api_key="anything",
            table_name="table.name",
            request_timeout=10,
            insertion_type="append",
        )
        dest.client = mock_client
        mock_result = Mock()
        mock_result.result = "non-empty-result"
        mock_client.run_query.return_value = mock_result
        self.assertEqual(True, dest._table_exists())

        mock_client.run_query.side_effect = QueryFailed("Table not found")
        self.assertEqual(False, dest._table_exists())

    @patch("dune_client.api.table.TableAPI.insert_table", name="Fake Table Inserter")
    @patch("dune_client.api.table.TableAPI.clear_data", name="Fake Clearer")
    def test_ensure_index_disabled_when_uploading(
        self, mock_clear, mock_insert_table, *_
    ):
        mock_clear.return_value = {
            "message": "Table my_user.interest_rates successfully cleared"
        }
        mock_insert_table.return_value = {"rows_written": 9000, "bytes_written": 90}

        dummy_df = TypedDataFrame(
            dataframe=pd.DataFrame(
                [
                    {"foo": "bar", "baz": "one"},
                    {"foo": "two", "baz": "two"},
                ]
            ),
            types={"foo": "varchar", "baz": "varchar"},
        )
        destination = DuneDestination(
            api_key=os.getenv("DUNE_API_KEY"),
            table_name="foo.bar",
            request_timeout=10,
            insertion_type="replace",
        )
        destination._table_exists = Mock(return_value=True)

        with self.assertLogs(level=DEBUG) as logs:
            destination.save(dummy_df)

        self.assertIn("Uploading DF to Dune", logs.output[0])
        self.assertIn("Inserted DF to Dune,", logs.output[-1])

    @patch("pandas.core.generic.NDFrame.to_csv", name="Fake csv writer")
    def test_duneclient_sets_timeout(self, mock_to_csv, *_):
        for timeout in [1, 10, 100, 1000, 1500]:
            destination = DuneDestination(
                api_key=os.getenv("DUNE_API_KEY"),
                table_name="foo.bar",
                request_timeout=timeout,
            )
            assert destination.client.request_timeout == timeout

    @patch("dune_client.api.table.TableAPI.clear_data", name="Fake Data Clearer")
    @patch("dune_client.api.table.TableAPI.upload_csv", name="Fake CSV Uploader")
    @patch("dune_client.api.table.TableAPI.insert_table", name="Fake Table Inserter")
    def test_dune_error_handling(self, mock_insert_table, mock_csv, mock_clear, *_):
        dest = DuneDestination(
            api_key="f00b4r",
            table_name="foo.bar",
            request_timeout=10,
            insertion_type="replace",
        )
        dest._table_exists = Mock(return_value=False)

        df = TypedDataFrame(pd.DataFrame([{"foo": "bar"}]), {})

        mock_insert_table.return_value = {"rows_written": 9000, "bytes_written": 90}
        dune_not_exist_error = DuneError(
            data={"error": "This table was not found"},
            response_class="response",
            err=KeyError("you missed something"),
        )
        dune_other_error = DuneError(
            data={"error": "Bad Request"},
            response_class="response",
            err=KeyError("you missed something"),
        )
        val_err = ValueError("Oops")
        runtime_err = RuntimeError("Big Oops")

        mock_clear.side_effect = dune_not_exist_error

        with self.assertRaises(DuneError) as err:
            dest.save(df)

        self.assertEqual(err.exception, dune_not_exist_error)

        mock_clear.assert_called_once()


        mock_clear.reset_mock()
        mock_clear.side_effect = dune_other_error

        with self.assertRaises(DuneError) as err:
            dest.save(df)

        mock_clear.assert_called_once()

        self.assertEqual(err.exception, dune_other_error)

        mock_clear.reset_mock()
        mock_clear.side_effect = val_err

        with self.assertRaises(ValueError) as err:
            dest.save(df)

        mock_clear.assert_called_once()
        self.assertEqual(err.exception, val_err)

        mock_clear.reset_mock()
        mock_clear.side_effect = runtime_err
        with self.assertLogs(level=ERROR) as logs:
            dest.save(df)


        # Upload CSV:
        dest.insertion_type = "upload_csv"

        mock_csv.return_value = False

        # mock_clear.assert_called_once()
        # expected_message = "Data processing error: Big Oops"
        # self.assertIn(expected_message, logs.output[0])
        #
        # # Reset all mocks to ensure clean state
        # mock_clear.reset_mock()
        # mock_insert_table.reset_mock()
        #
        # # TIL: reset_mock() doesn't clear side effects....
        # mock_clear.side_effect = None
        # mock_clear.return_value = None
        #
        # # Set return values explicitly
        # mock_clear.return_value = None
        #
        with self.assertLogs(level=ERROR) as logs:
            dest.save(df)

        mock_csv.assert_called_once()
        self.assertIn("Dune Upload Failed", logs.output[0])


class PostgresDestinationTest(unittest.TestCase):
    def setUp(self):
        self.db_url = "postgresql://postgres:postgres@localhost:5432/postgres"

    def test_saving_empty_df(self):
        pg_dest = PostgresDestination(
            db_url=self.db_url,
            table_name="foo",
        )
        df = pd.DataFrame([])
        with self.assertLogs(level=WARNING) as logs:
            pg_dest.save(TypedDataFrame(df, {"numeric": "int?"}))

        self.assertIn(
            "DataFrame is empty. Skipping save to PostgreSQL.", logs.output[0]
        )

    def test_failed_validation(self):
        # No index columns
        with (
            self.assertRaises(ValueError) as ctx,
            self.assertLogs(level="ERROR") as logs,
        ):
            PostgresDestination(
                db_url=self.db_url,
                table_name="foo",
                if_exists="upsert",
                index_columns=[],
            )

        self.assertIn(
            "Config for PostgresDestination is invalid", ctx.exception.args[0]
        )
        self.assertIn("Upsert without index columns.", logs.output[0])

    def test_table_exists(self):
        table_name = "test_table_exists"
        pg_dest = PostgresDestination(
            db_url=self.db_url,
            table_name=table_name,
        )
        drop_table(pg_dest.engine, table_name)
        self.assertEqual(False, pg_dest.table_exists())

        create_table(pg_dest.engine, table_name)
        # Now table should exist
        self.assertEqual(True, pg_dest.table_exists())

        # Cleanup
        drop_table(pg_dest.engine, table_name)

    def test_validate_unique_constraints(self):
        table_name = "test_validate_unique_constraints"
        pg_dest = PostgresDestination(
            db_url=self.db_url,
            table_name=table_name,
            if_exists="upsert",
            index_columns=["id"],
        )
        drop_table(pg_dest.engine, table_name)
        # No such table.
        with self.assertRaises(sqlalchemy.exc.NoSuchTableError) as _:
            pg_dest.validate_unique_constraints()

        create_table(pg_dest.engine, table_name)
        with (
            self.assertRaises(ValueError) as ctx,
            self.assertLogs(level="ERROR") as logs,
        ):
            pg_dest.validate_unique_constraints()

        self.assertIn(
            "No unique or exclusion constraint found. See error logs.",
            ctx.exception.args[0],
        )
        self.assertIn(f"ALTER TABLE {table_name} ADD CONSTRAINT", logs.output[0])

        # Add constraint
        raw_exec(
            pg_dest.engine,
            query_str=f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT {table_name}_id_unique
        UNIQUE (id);
        """,
        )
        self.assertEqual(None, pg_dest.validate_unique_constraints())

        # Multi Column Constraint
        pg_dest.index_columns = ["id", "value"]

        # raises without constraint.
        with self.assertRaises(ValueError) as _:
            pg_dest.validate_unique_constraints()

        # Add constraint
        raw_exec(
            pg_dest.engine,
            query_str=f"""
        ALTER TABLE {table_name}
        ADD CONSTRAINT id_value_unique
        UNIQUE (id, value);
        """,
        )
        # Passes!
        self.assertEqual(None, pg_dest.validate_unique_constraints())

        # Clean up
        drop_table(pg_dest.engine, table_name)

    def test_upsert(self):
        table_name = "test_upsert"
        pg_dest = PostgresDestination(
            db_url=self.db_url,
            table_name=table_name,
            if_exists="upsert",
            index_columns=["id"],
        )
        df1 = pd.DataFrame({"id": [1], "value": ["alice"]})
        df2 = pd.DataFrame({"id": [2], "value": ["bob"]})

        drop_table(pg_dest.engine, table_name)
        # This upsert would create table (since it doesn't exist yet)
        pg_dest.insert(TypedDataFrame(df1, {}), on_conflict="update")
        self.assertEqual(
            [{"id": 1, "value": "alice"}],
            select_star(pg_dest.engine, table_name),
        )
        # Add id constraint:
        raw_exec(
            pg_dest.engine,
            query_str=f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {table_name}_id_unique
                UNIQUE (id);
                """,
        )
        # This would insert with no conflict or update.
        pg_dest.insert(TypedDataFrame(df2, {}), on_conflict="update")
        self.assertEqual(
            [
                {"id": 1, "value": "alice"},
                {"id": 2, "value": "bob"},
            ],
            select_star(pg_dest.engine, table_name),
        )
        # overwrite some columns with max
        pg_dest.insert(
            TypedDataFrame(
                pd.DataFrame({"id": [2, 3], "value": ["max", "chuck"]}),
                {},
            ),
            on_conflict="update",
        )
        self.assertEqual(
            [
                {"id": 1, "value": "alice"},
                {"id": 2, "value": "max"},
                {"id": 3, "value": "chuck"},
            ],
            select_star(pg_dest.engine, table_name),
        )

        # Clean up
        drop_table(pg_dest.engine, table_name)

    def test_insert_ignore(self):
        table_name = "test_insert_ignore"
        pg_dest = PostgresDestination(
            db_url=self.db_url,
            table_name=table_name,
            if_exists="insert_ignore",
            index_columns=["id"],
        )
        df1 = pd.DataFrame({"id": [1], "value": ["alice"]})
        df2 = pd.DataFrame({"id": [2], "value": ["bob"]})

        drop_table(pg_dest.engine, table_name)
        # This upsert would create table (since it doesn't exist yet)
        pg_dest.insert(TypedDataFrame(df1, {}), on_conflict="nothing")
        self.assertEqual(
            [{"id": 1, "value": "alice"}],
            select_star(pg_dest.engine, table_name),
        )
        # Add id constraint:
        raw_exec(
            pg_dest.engine,
            query_str=f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {table_name}_id_unique
                UNIQUE (id);
                """,
        )
        # This would insert with no conflict or update.
        pg_dest.insert(TypedDataFrame(df2, {}), on_conflict="nothing")
        self.assertEqual(
            [{"id": 1, "value": "alice"}, {"id": 2, "value": "bob"}],
            select_star(pg_dest.engine, table_name),
        )
        # overwrite some columns with max
        pg_dest.insert(
            TypedDataFrame(
                pd.DataFrame({"id": [2, 3], "value": ["max", "chuck"]}),
                {},
            ),
            on_conflict="nothing",
        )
        self.assertEqual(
            [
                {"id": 1, "value": "alice"},
                {"id": 2, "value": "bob"},
                {"id": 3, "value": "chuck"},
            ],
            select_star(pg_dest.engine, table_name),
        )

        # Clean up
        drop_table(pg_dest.engine, table_name)

    def test_replace(self):
        table_name = "test_replace"
        pg_dest = PostgresDestination(
            db_url=self.db_url,
            table_name=table_name,
            if_exists="replace",
        )
        df1 = pd.DataFrame({"id": [1, 2], "value": ["alice", "bob"]})

        drop_table(pg_dest.engine, table_name)

        pg_dest.replace(TypedDataFrame(df1, {}))
        self.assertEqual(
            [
                {"id": 1, "value": "alice"},
                {"id": 2, "value": "bob"},
            ],
            select_star(pg_dest.engine, table_name),
        )

        df2 = pd.DataFrame({"id": [3, 4], "value": ["chuck", "dave"]})
        pg_dest.replace(TypedDataFrame(df2, {}))
        self.assertEqual(
            [
                {"id": 3, "value": "chuck"},
                {"id": 4, "value": "dave"},
            ],
            select_star(pg_dest.engine, table_name),
        )

        # Clean up
        drop_table(pg_dest.engine, table_name)
