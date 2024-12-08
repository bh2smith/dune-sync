"""Command line argument parser for dune-sync application."""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from src import root_path


@dataclass
class Args:
    """Command line argument parser for dune-sync application."""

    config: str
    jobs: list[str] | None

    @classmethod
    def from_command_line(cls) -> Args:
        """Create Args instance from command line arguments."""
        parser = argparse.ArgumentParser(
            description="Dune Sync - Data synchronization tool"
        )
        parser.add_argument(
            "--config",
            type=str,
            default=root_path.parent / "config.yaml",
            help="Path/URL with scheme to configuration file (default: config.yaml)",
        )
        parser.add_argument(
            "--jobs",
            type=str,
            nargs="*",  # accepts zero or more arguments
            default=None,
            help="Names of specific jobs to run (default: run all jobs)",
        )
        args = parser.parse_args()
        return cls(
            config=args.config,
            jobs=args.jobs if args.jobs else None,  # Convert empty list to None
        )
