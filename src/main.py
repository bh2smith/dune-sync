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
from src.job import Job
from src.logger import log


async def main(jobs: list[Job]) -> None:
    """Asynchronously execute a list of jobs.

    Raises:
        Various exceptions depending on job configuration and execution

    """
    tasks = [job.run() for job in jobs]
    for completed_task in asyncio.as_completed(tasks):
        try:
            await completed_task
        except Exception as e:
            log.error("Error in job execution: %s", str(e))


if __name__ == "__main__":
    args = Args.from_command_line()
    config = RuntimeConfig.load(args.config)

    # Filter jobs if specific ones were requested
    jobs_to_run = (
        [job for job in config.jobs if job.name in args.jobs]
        if args.jobs is not None
        else config.jobs
    )
    asyncio.run(main(jobs_to_run))
