import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Env:
    """
    A class to represent the environment configuration.

    Attributes
    ----------
    db_url : str
        The URL of the database connection.
    dune_api_key : str
        The API key used for accessing the Dune Analytics API.

    Methods
    -------
    None
    """

    db_url: str
    dune_api_key: str


def load_env() -> Env:
    dune_api_key = os.environ.get("DUNE_API_KEY")
    db_url = os.environ.get("DB_URL")

    load_dotenv()
    if dune_api_key is None:
        raise EnvironmentError("DUNE_API_KEY environment variable must be set!")
    if db_url is None:
        raise EnvironmentError("DB_URL environment variable must be set!")

    return Env(db_url, dune_api_key)
