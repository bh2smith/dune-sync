from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Union

import yaml
from dotenv import load_dotenv
from dune_client.types import ParameterType, QueryParameter


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
            raise RuntimeError("DUNE_API_KEY environment variable must be set!")
        if db_url is None:
            raise RuntimeError("DB_URL environment variable must be set!")

        return cls(db_url, dune_api_key)


def parse_query_parameters(params: list[dict[str, Any]]) -> list[QueryParameter]:
    query_params = []
    for param in params:
        name = param["name"]
        param_type = ParameterType.from_string(param["type"])
        value = param["value"]

        if param_type == ParameterType.TEXT:
            query_params.append(QueryParameter.text_type(name, value))
        elif param_type == ParameterType.NUMBER:
            query_params.append(QueryParameter.number_type(name, value))
        elif param_type == ParameterType.DATE:
            query_params.append(QueryParameter.date_type(name, value))
        elif param_type == ParameterType.ENUM:
            query_params.append(QueryParameter.enum_type(name, value))
        else:
            # Can't happen.
            raise ValueError(f"Unknown parameter type: {param['type']}")

    return query_params


@dataclass
class RuntimeConfig:
    """
    A class to represent the runtime configuration settings.

    """

    @classmethod
    def load_from_yaml(cls, file_path: Union[Path, str] = "config.yaml") -> Any:
        with open(file_path, "rb") as _handle:
            data = yaml.safe_load(_handle)

        return data

    def validate(self) -> None:
        return
        # TODO this needs to be refactored to work with the new Job class
        # num_jobs = len(self.dune_to_local_jobs)
        # if num_jobs != len(set(j.query.query_id for j in self.dune_to_local_jobs)):
        #     log.warning("Detected multiple jobs running the same query")
        # if num_jobs != len(set(j.table_name for j in self.dune_to_local_jobs)):
        #     log.warning("Detected duplicate table names in job list")
