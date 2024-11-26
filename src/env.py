"""Module to handle environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from string import Template

from dotenv import load_dotenv


@dataclass
class Env:
    """A class to represent the environment configuration.

    Methods
    -------
    None

    """

    @classmethod
    def load(cls) -> Env:
        """Load environment variables and create an Env instance.

        Returns:
            Env: Initialized environment configuration

        """
        load_dotenv()
        return cls()

    @staticmethod
    def interpolate(value: str) -> str:
        """Interpolate environment variables in a string value.

        Handles ${VAR} and $VAR syntax.
        Returns the original value if it's not a string.

        Args:
            value: The value to interpolate. Can be any type, but only strings
                  will be processed for environment variables.

        Returns:
            The interpolated value if it's a string, otherwise the original value.

        Raises:
            KeyError: If an environment variable referenced in the string doesn't exist.

        """
        # Handle ${VAR} syntax
        template = Template(value)
        try:
            return template.substitute(os.environ)
        except KeyError as e:
            missing_var = str(e).strip("'")
            raise KeyError(f"Environment variable '{missing_var}' not found. ") from e
