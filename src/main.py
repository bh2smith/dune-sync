import os
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from sqlalchemy import create_engine
from dotenv import load_dotenv

from src.types import DUNE_TO_PG

load_dotenv()
DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
DB_URL = os.environ.get("DB_URL")

# TODO(bh2smith): parse config file for most of the following stuff
QUERY_ID = int(os.environ.get("QUERY_ID"))
POLL_FREQUENCY = int(os.environ.get("POLL_FREQUENCY"))
QUERY_ENGINE = os.environ.get("QUERY_ENGINE")
TABLE_NAME = f"dune_data_{QUERY_ID}"


def reformat_varbinary_columns(df, varbinary_columns):
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def fetch_dune_data():
    result = (
        DuneClient(DUNE_API_KEY, performance=QUERY_ENGINE)
        .run_query(query=QueryBase(QUERY_ID), ping_frequency=POLL_FREQUENCY)
        .result
    )

    metadata = result.metadata
    dtypes, varbinary_columns = {}, []
    for name, d_type in zip(metadata.column_names, metadata.column_types):
        dtypes[name] = DUNE_TO_PG[d_type]
        if d_type == "varbinary":
            varbinary_columns.append(name)
    df = pd.DataFrame(result.rows)
    # escape bytes
    df = reformat_varbinary_columns(df, varbinary_columns)
    return df, dtypes


def save_to_postgres(df, dtypes):
    db_connection = DB_URL
    engine = create_engine(db_connection)
    df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False, dtype=dtypes)
    print("Data saved to PostgreSQL successfully!")


def main():
    df, types = fetch_dune_data()
    if df is not None:
        save_to_postgres(df, types)


if __name__ == "__main__":
    main()
