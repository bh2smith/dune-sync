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
import asyncio

from src.args import Args
from src.config import RuntimeConfig
from src.logger import log


async def main() -> None:
    """Load configuration and execute jobs asynchronously.

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
    args = Args.from_command_line()
    config = RuntimeConfig.load_from_yaml(args.config)

    # Filter jobs if specific ones were requested
    jobs_to_run = (
        [job for job in config.jobs if job.name in args.jobs]
        if args.jobs is not None
        else config.jobs
    )

    tasks = [job.run() for job in jobs_to_run]
    for job, completed_task in zip(
        config.jobs, asyncio.as_completed(tasks), strict=False
    ):
        await completed_task
        log.info("Job completed: %s", job)


if __name__ == "__main__":
    asyncio.run(main())
