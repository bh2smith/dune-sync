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
import time
import uuid
from pathlib import Path
from typing import Any

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from src import root_path
from src.config import RuntimeConfig
from src.logger import log


def log_job_metrics(job_metrics: dict[str, Any]) -> None:
    """Foo."""
    registry = CollectorRegistry()

    job_success_timestamp = Gauge(
        name="job_last_success_unixtime",
        documentation="Last time a batch job successfully finished",
        registry=registry,
    )
    job_success_timestamp.set_to_current_time()

    job_duration_metric = Gauge(
        name="job_last_success_duration",
        documentation="How long did the last job take to run",
        registry=registry,
    )
    job_duration_metric.set(job_metrics["duration"])
    push_to_gateway(
        gateway="localhost:9091",
        job=f'dune-sync-{job_metrics["run_id"]}',
        registry=registry,
    )


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
    run_id = uuid.uuid4().hex

    log.info("Run ID: %s", run_id)
    tasks = [job.run() for job in config.jobs]
    for job, completed_task in zip(
        config.jobs, asyncio.as_completed(tasks), strict=False
    ):
        job_start = time.perf_counter()
        await completed_task
        job_duration = time.perf_counter() - job_start

        job_metrics = {
            "duration": job_duration,
            "name": job.name,
            "run_id": run_id,
        }
        log_job_metrics(job_metrics)
        log.info("Job completed: %s", job)


if __name__ == "__main__":
    asyncio.run(main())
