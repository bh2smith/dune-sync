"""Main package initialization for dune-sync."""

# TODO once https://github.com/pylint-dev/pylint/issues/10000 (yes, really) is resolved,
#  delete everything marked with MARKER: pylint-bug

# MARKER: pylint-bug
import collections

# MARKER: pylint-bug end
from pathlib import Path
from typing import Any

root_path = Path(__file__).parent.resolve()

# MARKER: pylint-bug
Awaitable = collections.abc.Awaitable
Callable = collections.abc.Callable[..., Any]
Iterable = collections.abc.Iterable
Mapping = collections.abc.Mapping
# MARKER: pylint-bug end
