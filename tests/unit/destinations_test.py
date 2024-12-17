import os
import unittest
from logging import ERROR, WARNING
from unittest.mock import patch

import pandas as pd
import sqlalchemy
from dune_client.models import DuneError

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

    @patch("requests.sessions.Session.post")
    @patch("pandas.core.generic.NDFrame.to_csv", name="Fake csv writer")
    def test_ensure_index_disabled_when_uploading(self, mock_to_csv, *_):
        dummy_data = [
            {"foo": "bar"},
            {"baz": "daz"},
        ]
        dummy_df = pd.DataFrame(dummy_data)
        destination = DuneDestination(
            api_key=os.getenv("DUNE_API_KEY"), table_name="foo"
        )
        destination.save(dummy_df)
        mock_to_csv.assert_called_once_with(index=False)

    @patch("pandas.core.generic.NDFrame.to_csv", name="Fake csv writer")
    def test_duneclient_sets_timeout(self, mock_to_csv, *_):
        dummy_data = [
            {"foo": "bar"},
            {"baz": "daz"},
        ]
        dummy_df = pd.DataFrame(dummy_data)
        for timeout in [1, 10, 100, 1000, 1500]:
            destination = DuneDestination(
                api_key=os.getenv("DUNE_API_KEY"),
                table_name="foo",
                request_timeout=timeout,
            )
            assert destination.client.request_timeout == timeout

    @patch("dune_client.api.table.TableAPI.upload_csv", name="Fake CSV uploader")
    def test_dune_error_handling(self, mock_dune_upload_csv):
        dest = DuneDestination(api_key="f00b4r", table_name="foo")
        df = pd.DataFrame([{"foo": "bar"}])

        dune_err = DuneError(
            data={"error": "bad stuff"},
            response_class="response",
            err=KeyError("you missed something"),
        )
        val_err = ValueError("Oops")
        runtime_err = RuntimeError("Big Oops")

        mock_dune_upload_csv.side_effect = dune_err

        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()

        # does this shit really look better just because it's < 88 characters long?
        exmsg = (
            "Dune did not accept our upload: "
            "Can't build response from {'error': 'bad stuff'}"
        )
        self.assertIn(exmsg, logs.output[0])

        mock_dune_upload_csv.reset_mock()
        mock_dune_upload_csv.side_effect = val_err

        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()
        expected_message = "Data processing error: Oops"
        self.assertIn(expected_message, logs.output[0])

        mock_dune_upload_csv.reset_mock()
        mock_dune_upload_csv.side_effect = runtime_err
        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()
        expected_message = "Data processing error: Big Oops"
        self.assertIn(expected_message, logs.output[0])

        mock_dune_upload_csv.reset_mock()

        # TIL: reset_mock() doesn't clear side effects....
        mock_dune_upload_csv.side_effect = None

        mock_dune_upload_csv.return_value = None

        with self.assertLogs(level=ERROR) as logs:
            dest.save(data=df)

        mock_dune_upload_csv.assert_called_once()
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
