"""
Utilities to create micro-ETL-like pipelines for easier and more maintainable sets of tasks
"""
from __future__ import annotations

# pragma pylint: disable=too-few-public-methods

import graphlib
from graphlib import TopologicalSorter
from typing import Callable, Optional, Iterable, Dict, Any, Hashable, Union


class DataBag:
    """
    Use this class as a singleton, and set properties on it to pass data between tasks.
    """

    _bag: Dict[Any, Any] = {}
    __databag = None

    def __new__(cls, *args: Iterable[Any], **kwargs: Dict[Any, Any]) -> DataBag:
        if not cls.__databag:
            cls.__databag = super(DataBag, cls).__new__(cls)

        return cls.__databag

    def __init__(self) -> None:
        if DataBag.__databag is None:
            DataBag.__databag = self

    def __setattr__(self, key: Hashable, value: Any) -> None:
        self.__databag._bag[key] = value

    def __getattr__(self, item: Any) -> Union[Any, None]:
        return self.__databag._bag.get(item, None)


class Pipeline:
    """
    Orchestrates a set of :class: Task instances and allows you to run them as an ETL pipeline.

    """

    def __init__(self) -> None:
        self._graph: TopologicalSorter = graphlib.TopologicalSorter()

    def add(self, node, *predecessors) -> None:
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
        _callable: Callable,
        name: Optional[str] = None,
        task_args: Optional[Iterable] = None,
        task_kwargs: Optional[dict] = None,
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
