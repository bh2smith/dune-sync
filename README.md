# Dune Sync V2

A general-purpose bidirectional data synchronization tool that enables seamless data transfer between [Dune Analytics](https://dune.com) and PostgreSQL databases. This project was developed as part of the [CoW DAO Grants Program](https://forum.cow.fi/t/grant-application-dune-sync-v2/2597).

## Features

- **Dune to PostgreSQL**: Archive data from Dune queries into your local database
- **PostgreSQL to Dune**: Upload local data to Dune tables via the CSV upload endpoint
- **Configuration-based**: Simple YAML configuration for defining sources, destinations, and jobs
- **Docker-ready**: Easy deployment using pre-built container images

## Usage

### Create a configuration file

Configuration is provided in a single YAML file. Refer to the existing `config.yaml` for an overview.
The configuration file consists of three main sections:
- `data_sources`: Defines available databases
- `jobs`: Defines synchronization jobs that connect sources to destinations

The config file may contain environment variable placeholders in
[envsubst](https://www.gnu.org/software/gettext/manual/html_node/envsubst-Invocation.html)-compatible format:
- `$VAR_NAME`
- `${VAR_NAME}`
- `$varname`

**Note**: Every variable referenced this way __must__ be defined at runtime,
otherwise the program will exit with an error.

#### Data Source Definitions

Sources are defined as a list of configurations, each containing:
- `name`: String. A unique identifier for referencing this source in jobs
- `type`: String. Must be either `dune` or `postgres`
- `key`: String. Connection details, supports environment variable templating using `${VAR_NAME}` syntax such as `${DB_URL}` or `${DUNE_API_KEY}` ([see environment setup](#define-environment))

#### Job Parameters

Each job in the `jobs` list should contain:
- `name`: Optional String. A human-readable name for the job
- `source`: Definition of which source to use and how to fetch data
- `destination`: Definition of which destination to use and how to store data

##### Source Configuration

For Dune sources (`ref: Dune1`):
- `query_id`: Integer. ID of an existing Dune Query to execute
- `query_engine`: Optional String. Either `medium` or `large`. Defaults to `medium`
- `poll_frequency`: Optional Integer. Seconds between result polling. Defaults to `1`.
- `parameters`: Optional list of Dune Query parameters
    - `name`: String. Parameter name
    - `type`: String. Must be one of: `TEXT`, `NUMBER`, `DATE`, `ENUM`
    - `value`: Any. Value for the parameter

For Postgres sources (`ref: Postgres`):
- `query_string`: String. SQL query or path to .sql file (relative to `main.py` or absolute)

##### Destination Configuration

For Dune destinations (`ref: Dune`):
- `table_name`: String. Name of Dune table to update

For Postgres destinations (`ref: Postgres`):
- `table_name`: String. Name of table to insert/append into
- `if_exists`: String. One of `fail`, `replace`, `append`

### Define environment

Copy `.env.sample` to `.env` and fill out the two required variables

- `DUNE_API_KEY` - Valid API key for [Dune](https://dune.com/)
- `DB_URL` - Connection string for the source and/or destination PostgreSQL database,
  in the form `postgresql://postgres:postgres@localhost:5432/postgres`
- (Optional) `PROMETHEUS_PUSHGATEWAY_URL` - URL of a [Prometheus Pushgateway](https://github.com/prometheus/pushgateway) which receives job-related metrics.

### Mount the config and .env files into the container and run the script

You can download the image from GitHub Container Registry:

```shell
docker pull ghcr.io/bh2smith/dune-sync:latest
```

Or build it yourself:

```shell
export IMAGE_NAME=dune-sync # (or ghcr.io image above)
docker build -t ${IMAGE_NAME} .

# Base docker command (using config.yaml mounted at /app/config.yaml)
docker run --rm \
    -v "$(pwd)/config.yaml:/app/config.yaml" \
    --env-file .env \
    ${IMAGE_NAME}

# Optional additions:
# - Mount custom config file (requires --config flag)
    -v "$(pwd)/my-config.yaml:/app/my-config.yaml" \
# - Mount queries directory (if using SQL file paths)
    -v "$(pwd)/queries:/app/queries" \
    --config /app/my-config.yaml
# - Specify jobs to run (if not specified, all jobs will be run)
    --jobs job1 job2
```

Note that postgres queries can also be file paths (they would also need to be mounted into the container).

## Local Development

Fill out the empty fields in [Sample Env](.env.sample) (e.g. `DUNE_API_KEY` and `DB_URL`)

```shell
docker-compose up -d # Starts postgres container (in the background)
python -m src.main [--config config.yaml] [--jobs d2p-test-1 p2d-test]
```

### Development Commands

The project uses a Makefile to streamline development tasks. Here are the available commands:

- `make install`: Creates a virtual environment and installs all development dependencies
- `make update`: Check for new versions of dependencies and install them
- `make fmt`: Formats code using black
- `make lint`: Runs pylint for code quality checks
- `make types`: Performs static type checking with mypy
- `make check`: Runs formatting, linting, and type checking in sequence
- `make test`: Runs pytest with coverage reporting (minimum 80% coverage required)
- `make clean`: Removes Python cache files
- `make run`: Executes the main application

To get started with development:

```shell
python -m pip install poetry  # install poetry which is used to manage the project's dependencies
make install  # Set up virtual environment
python -m poetry shell  # Activate virtual environment
make check  # Verify code quality
make test   # Run tests
```
