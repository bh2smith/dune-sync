"""Interface definitions for the dune-sync package."""

from abc import ABC, abstractmethod
from typing import Any, Generic, Protocol, TypeVar

from pandas import DataFrame


class LenCapable(Protocol):
    """A protocol that enforces the presence of __len__."""

    def __len__(self) -> int:
        """Return the number of items present in this interface."""


# This will represent your data type (DataFrame, dict, etc.)
T = TypeVar("T", bound=LenCapable)


class TypedDataFrame:
    """A wrapper around (DataFrame, metadata) with a __len__ method."""

    def __init__(self, dataframe: DataFrame, types: dict[str, Any]):
        self.dataframe = dataframe
        self.types = types

    def __len__(self) -> int:
        """Return the number of rows in the DataFrame."""
        return len(self.dataframe)

    def is_empty(self) -> bool:
        """Return True if the DataFrame is empty."""
        return self.dataframe.empty


# TODO: maybe a good place to define schema transformations and other data manipulation?


class Named(Protocol):
    """Represents any class with name field."""

    name: str


class Validate(ABC):
    """Enforce validation on inheriting classes."""

    def __init__(self) -> None:
        if not self.validate():
            raise ValueError(
                f"Config for {self.__class__.__name__} is invalid. "
                "See ERROR log for details."
            )

    @abstractmethod
    def validate(self) -> bool:
        """Validate the configuration."""


class Source(Validate, Generic[T]):
    """Abstract base class for data sources."""

    @abstractmethod
    async def fetch(self) -> T:
        """Fetch data from the source."""

    @abstractmethod
    def is_empty(self, data: T) -> bool:
        """Return True if the fetched data is empty."""


class Destination(Validate, Generic[T]):
    """Abstract base class for data destinations."""

    @abstractmethod
    def save(self, data: T) -> int:
        """Save data to the destination, returning records processed."""
