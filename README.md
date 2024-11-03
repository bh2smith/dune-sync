# Dune Sync V2




## Local Development

Fill out the empty fields in [Sample Env](.env.sample) (`DUNE_API_KEY` and `QUERY_ID`)

```shell
docker-compose up -d
python -m src.main
```

### Docker

```shell
docker build -t dune-sync .
docker run --rm -v "$(pwd)/config.toml:/app/config.toml" --env-file .env dune-sync
```
