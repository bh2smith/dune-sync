import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from src.types import DUNE_TO_PG

load_dotenv()
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
DB_URL = os.getenv("DB_URL")
TABLE_NAME = "dune_data"

API_URL = "https://api.dune.com/api/v1/query/4132129/results"
HEADERS = {"x-dune-api-key": DUNE_API_KEY}


def reformat_varbinary_columns(df, varbinary_columns):
    for col in varbinary_columns:
        df[col] = df[col].apply(lambda x: bytes.fromhex(x[2:]) if pd.notnull(x) else x)
    return df


def fetch_dune_data():
    response = requests.get(API_URL, headers=HEADERS)
    if response.status_code == 200:
        data = response.json()
        result = data["result"]
        metadata =  result["metadata"]
        dtypes, varbinary_columns = {}, []
        for name, d_type in zip(metadata["column_names"], metadata["column_types"]):
            dtypes[name] = DUNE_TO_PG[d_type]
            if d_type == "varbinary":
                varbinary_columns.append(name)
        df = pd.DataFrame(result["rows"])
        # escape bytes
        df = reformat_varbinary_columns(df, varbinary_columns)
        return df, dtypes
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

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
