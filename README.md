# Dune Sync V2

## Usage

### Create a configuration file

Configuration is provided in a single YAML file. Refer to the existing `config.yaml` for an overview.
The configuration file consists of three main sections:
- `sources`: Defines available data sources
- `destinations`: Defines available data destinations
- `jobs`: Defines synchronization jobs that connect sources to destinations

#### Source Definitions

Sources are defined as a list of configurations, each containing:
- `name`: String. A unique identifier for referencing this source in jobs
- `type`: String. Must be either `dune` or `postgres`
- `key`: String. Connection details, supports environment variable templating using `${VAR_NAME}` syntax such as `${DB_URL}` or `${DUNE_API_KEY}` ([see environment setup](#define-environment))

#### Destination Definitions

Destinations are defined similarly to sources:
- `name`: String. A unique identifier for referencing this destination in jobs
- `type`: String. Must be either `dune` or `postgres`
- `key`: String. Connection details, supports environment variable templating using `${VAR_NAME}` syntax

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
```

Note that postgres queries can also be file paths (they would also need to be mounted into the container).

## Local Development

Fill out the empty fields in [Sample Env](.env.sample) (e.g. `DUNE_API_KEY` and `DB_URL`)

```shell
docker-compose up -d # Starts postgres container (in the background)
python -m src.main --config config.yaml
```
