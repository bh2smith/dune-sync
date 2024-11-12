# pragma: no cover
"""
Main entry point for the dune-sync application.

This module initializes the runtime configuration and executes the configured jobs
sequentially. Each job typically consists of:
1. Extracting data from a source (Dune Analytics or Postgres)
2. Loading the data into a destination (Dune Analytics or Postgres)

The configuration is loaded from a YAML file (config.yaml) in the project root.

Usage:
    python -m src.main

Environment Variables:
    Required environment variables depend on the configured sources and destinations.
    Typically includes database connection strings and API keys.
"""
from src import root_path
from src.config import RuntimeConfig
from src.logger import log


def main() -> None:
    """
    Main function that loads configuration and executes jobs sequentially.

    The function:
    1. Loads the configuration from config.yaml
    2. Executes each configured job
    3. Logs the completion of each job

    Raises:
        FileNotFoundError: If config.yaml is not found
        yaml.YAMLError: If config.yaml is invalid
        Various exceptions depending on job configuration and execution
    """
    config = RuntimeConfig.load_from_yaml(root_path.parent / "config.yaml")
    # TODO: Async job execution https://github.com/bh2smith/dune-sync/issues/20
    for job in config.jobs:
        job.run()
        log.info("Job completed: %s", job)


if __name__ == "__main__":
    main()
