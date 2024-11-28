"""Command line argument parser for dune-sync application."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from src import root_path


@dataclass
class Args:
    """Command line argument parser for dune-sync application."""

    config: Path
    jobs: list[str] | None
    allow_alteration: bool

    @classmethod
    def from_command_line(cls) -> Args:
        """Create Args instance from command line arguments."""
        parser = argparse.ArgumentParser(
            description="Dune Sync - Data synchronization tool"
        )
        parser.add_argument(
            "--config",
            type=Path,
            default=root_path.parent / "config.yaml",
            help="Path to configuration file (default: config.yaml)",
        )
        parser.add_argument(
            "--jobs",
            type=str,
            nargs="*",  # accepts zero or more arguments
            default=None,
            help="Names of specific jobs to run (default: run all jobs)",
        )
        parser.add_argument(
            "--allow_alteration",
            type=bool,
            default=True,
            help="Allow table alteration based on failed validation (default: True)",
        )
        args = parser.parse_args()
        return cls(
            config=args.config,
            jobs=args.jobs if args.jobs else None,  # Convert empty list to None
            allow_alteration=args.allow_alteration,
        )
