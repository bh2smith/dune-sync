"""
Utilities to create micro-ETL-like pipelines for easier and more maintainable sets of tasks
"""

from __future__ import annotations

# pragma pylint: disable=too-few-public-methods

import graphlib
from collections.abc import Hashable
from graphlib import TopologicalSorter
from typing import Callable, Optional, Iterable, Any, TypeVar, Protocol, Generic


# Define a Protocol for tasks that have a run method
class Runnable(Protocol):
    """Runnable protocol"""

    def run(self) -> Any: ...


# Define a type variable constrained to Runnable
T = TypeVar("T", bound=Runnable)

# T = TypeVar("T")


class DataBag:
    """
    Use this class as a singleton, and set properties on it to pass data between tasks.
    """

    _bag: dict[Any, Any] = {}
    __databag = None

    def __new__(cls, *args: Iterable[Any], **kwargs: dict[Any, Any]) -> DataBag:
        if not cls.__databag:
            cls.__databag = super(DataBag, cls).__new__(cls)

        return cls.__databag

    def __init__(self) -> None:
        if DataBag.__databag is None:
            DataBag.__databag = self

    def __setattr__(self, key: Hashable, value: Any) -> None:
        assert self.__databag is not None, "DataBag instance has not been initialized"
        self.__databag._bag[key] = value

    def __getattr__(self, item: Any) -> Any | None:
        assert self.__databag is not None, "DataBag instance has not been initialized"
        return self.__databag._bag.get(item, None)


class Pipeline(Generic[T]):
    """
    Orchestrates a set of :class: Task instances and allows you to run them as an ETL pipeline.

    """

    def __init__(self) -> None:
        self._graph: TopologicalSorter[T] = graphlib.TopologicalSorter()

    def add(self, node: T, *predecessors: T) -> None:
        self._graph.add(node, *predecessors)

    def run(self) -> None:
        self._graph.prepare()
        while self._graph.is_active():
            for task in self._graph.get_ready():
                task.run()
                self._graph.done(task)


class Task:
    """
    Wraps a function which should become part of a Pipeline.
    """

    def __init__(
        self,
        _callable: Callable[..., Any],
        name: Optional[str] = None,
        task_args: Optional[Iterable[Any]] = None,
        task_kwargs: Optional[dict[str, Any]] = None,
    ):
        self.callable = _callable
        self.task_args = task_args or []
        self.task_kwargs = task_kwargs or {}
        self.__name__ = name or f"Task {_callable.__name__}"

    def run(self) -> Any:
        try:
            return self.callable(*self.task_args, **self.task_kwargs)
        except Exception as e:
            print(f'"{self.__name__}" failed with {e}')
            raise
