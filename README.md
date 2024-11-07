# Dune Sync V2

## Usage

### Create a configuration file

Configuration is provided in a single YAML file. Refer to the existing `config.yaml` for an overview.
In general, it should contain a top-level key called `jobs`, whose members should be YAML dictionaries describing the
job to run.

#### Job parameters

- `name`: An optional human-readable name for the job. Note that uniqueness is not enforced.
- `source`: Definition of a data store to fetch data from. Exact parameter depends on source type, see **Sources**
  further in this section.
- `destination`: Definition of a data store to store data into. Exact parameter depends on source type, see *
  *Destinations** further in this section.

#### Sources

Currently supported sources are Dune and PostgreSQL. Description of required parameters follows.

#### Dune

- `ref`: String. Must be exactly `dune`
- `query_id`: Integer. ID of an existing Dune Query to execute and fetch results from.
- `query_engine`: Optional String. If specified must be one of exactly `medium` or `large`. Defaults to `medium`.
- `poll_frequency`: Optional Integer. Determines how often, in seconds, to poll Dune for results after running the given query ID. Defaults to `1`.
- `parameters`: Optional Dune Query parameters.
    - `name`: String. Name of parameter
    - `type`: String. Type of parameter. Must be one of: "text", "number", "datetime", "enum"
    - `value`: Any. Value to send for the defined parameter

#### Postgres

- `ref`: String. Must be exactly `postgres`
- `query_string`: String. Query to run against a Postgres server in order to fetch results and submit them to a
  Destination.
  If instead of an SQL query it's a path that ends in `.sql`, read the `query_string` from the specified file instead.
  File must be relative to `main.py` or be specified with an absolute path.

#### Destinations

Currently supported destinations are Dune and PostgreSQL. Description of required parameters follows.

#### Dune

- `ref`: String. Must be exactly `dune`
- `table_name`: String. Name of Dune table to update.

#### Postgres

- `ref`: String. Must be exactly `postgres`
- `table_name`: String. Name of table in the configured Postgres server to insert or append into. Does not have to
  exist, will be created if it doesn't.
- `if_exists`: String. One of "fail", "replace", "append". "replace" truncates the table if it exists, then stores the
  results.
  "append" inserts new rows into the table if it already exists and contains data. "fail" causes the job to fail.

### Define environment

Copy `.env.sample` to `.env` and fill out the two required variables

- `DUNE_API_KEY` - Valid API key for [Dune](https://dune.com/)
- `DB_URL` - Connection string for the source and/or destination PostgreSQL database,
  in the form `postgresql://postgres:postgres@localhost:5432/postgres`

### Mount the config and .env files into the container and run the script

```shell
docker build -t dune-sync .
docker run --rm -v "$(pwd)/config.yaml:/app/config.yaml" --env-file .env dune-sync
```

## Local Development

Fill out the empty fields in [Sample Env](.env.sample) (`DUNE_API_KEY` and `DB_URL`)

```shell
docker-compose up -d
python -m src.main
```

### Docker

```shell
docker build -t dune-sync .
docker run --rm -v "$(pwd)/config.yaml:/app/config.yaml" --env-file .env dune-sync
```
