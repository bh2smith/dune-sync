from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal, Union

import tomli
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

    @classmethod
    def load(cls) -> Env:
        load_dotenv()
        dune_api_key = os.environ.get("DUNE_API_KEY")
        db_url = os.environ.get("DB_URL")

        if dune_api_key is None:
            raise EnvironmentError("DUNE_API_KEY environment variable must be set!")
        if db_url is None:
            raise EnvironmentError("DB_URL environment variable must be set!")

        return cls(db_url, dune_api_key)


@dataclass
class RuntimeConfig:
    """
    A class to represent the runtime configuration settings.

    Attributes
    ----------
    query_id : int
        The ID of the query to execute.
    table_name : str
        The name of the table where the query results will be stored.
    query_engine : Literal["medium", "large"]
        The query engine to use, either "medium" or "large" (default is "medium").
    poll_frequency : int
        The frequency (in seconds) at which the query should be polled.
    Methods
    -------
    load_from_toml(file_path: str) -> RuntimeConfig:
        Reads the configuration from a TOML file and returns a RuntimeConfig object.
    """

    query_id: int
    table_name: str
    poll_frequency: int
    query_engine: Literal["medium", "large"] = "medium"  # Default value is "medium"

    @classmethod
    def load_from_toml(
        cls, file_path: Union[str, os.PathLike] = "config.toml"
    ) -> RuntimeConfig:
        """
        Reads the configuration from a TOML file and returns a RuntimeConfig object.

        Parameters
        ----------
        file_path : str
            The path to the TOML configuration file.

        Returns
        -------
        RuntimeConfig
            An instance of RuntimeConfig populated with the data from the TOML file.
        """
        # Load the configuration from the TOML file
        with open(file_path, "rb") as f:
            toml_dict = tomli.load(f)

        # Required
        query_id = toml_dict["query_id"]
        table_name = toml_dict.get("table_name", f"dune_data_{query_id}")

        # Ensure that QUERY_ENGINE is either 'medium' or 'large'
        query_engine = toml_dict.get("query_engine", "medium")
        if query_engine not in ("medium", "large"):
            raise ValueError("query_engine must be either 'medium' or 'large'.")
        # Create and return the RuntimeConfig object
        return cls(
            query_id,
            table_name,
            poll_frequency=toml_dict.get("poll_frequency", 1),
            query_engine=query_engine,
        )
