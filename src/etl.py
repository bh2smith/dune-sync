"""
Utilities to create micro-ETL-like pipelines for easier and more maintainable sets of tasks
"""

import graphlib
from typing import Callable, Optional, Iterable


class DataBag:
    """
    Use this class as a singleton, and set properties on it to pass data between tasks.
    """

    _bag = {}
    __databag = None

    def __new__(cls, *args, **kwargs):
        if not cls.__databag:
            cls.__databag = super(DataBag, cls).__new__(cls)

        return cls.__databag

    def __init__(self):
        if DataBag.__databag is None:
            DataBag.__databag = self

    def __setattr__(self, key, value):
        self.__databag._bag[key] = value

    def __getattr__(self, item):
        return self.__databag._bag.get(item, None)


class Pipeline:
    """
    Orchestrates a set of :class: Task instances and allows you to run them as an ETL pipeline.

    """

    def __init__(self):
        self._graph = graphlib.TopologicalSorter()

    def add(self, node, *predecessors):
        self._graph.add(node, *predecessors)

    def run(self):
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
        name: str = None,
        task_args: Optional[Iterable] = None,
        task_kwargs: Optional[dict] = None,
    ):
        self.callable = _callable
        self.task_args = task_args or []
        self.task_kwargs = task_kwargs or {}
        self.__name__ = name or f"Task {_callable.__name__}"

    def run(self):
        try:
            return self.callable(*self.task_args, **self.task_kwargs)
        except Exception as e:
            print(f'"{self.__name__}" failed with {e}')
            raise
