from abc import ABC, abstractmethod
from typing import TypeVar, Generic

# This will represent your data type (DataFrame, dict, etc.)
T = TypeVar("T")


class Source(ABC, Generic[T]):
    """Abstract base class for data sources"""

    @abstractmethod
    def fetch(self) -> T:
        """Fetch data from the source"""

    @abstractmethod
    def validate(self) -> bool:
        """Validate the source configuration"""


class Destination(ABC, Generic[T]):
    """Abstract base class for data destinations"""

    @abstractmethod
    def save(self, data: T) -> None:
        """Save data to the destination"""

    @abstractmethod
    def validate(self) -> bool:
        """Validate the destination configuration"""