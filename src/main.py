# pragma: no cover
"""Main entry point for the dune-sync application.

This module initializes the runtime configuration and executes the configured jobs
sequentially. Each job typically consists of:
1. Extracting data from a source (Dune Analytics or Postgres)
2. Loading the data into a destination (Dune Analytics or Postgres)

The configuration is loaded from a YAML file
(defaults to config.yaml in the project root).

Usage:
    python -m src.main [--config PATH]

Arguments:
    --config PATH    Optional path to configuration file (default: config.yaml)

Environment Variables:
    Required environment variables depend on the configured sources and destinations.
    Typically includes database connection strings and API keys.

"""

import argparse
import asyncio
from pathlib import Path

from src import root_path
from src.config import RuntimeConfig


async def main() -> None:
    """Load configuration and execute jobs asyncronously.

    The function:
    1. Parses command line arguments
    2. Loads the configuration from the specified config file (defaults to config.yaml)
    3. Executes each configured job
    4. Logs the completion of each job

    Raises:
        FileNotFoundError: If config file is not found
        yaml.YAMLError: If config file is invalid
        Various exceptions depending on job configuration and execution

    """
    parser = argparse.ArgumentParser(
        description="Dune Sync - Data synchronization tool"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=root_path.parent / "config.yaml",
        help="Path to configuration file (default: config.yaml)",
    )
    args = parser.parse_args()

    config = RuntimeConfig.load_from_yaml(args.config)

    tasks = [job.run() for job in config.jobs]
    for _job, completed_task in zip(
        config.jobs, asyncio.as_completed(tasks), strict=False
    ):
        await completed_task


if __name__ == "__main__":
    asyncio.run(main())
