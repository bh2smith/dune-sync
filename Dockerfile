# Base
FROM python:3.13-slim AS base

# Install system dependencies and poetry
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl bash git && \
    pip install poetry && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY src /app/src
COPY pyproject.toml poetry.lock* ./
RUN POETRY_VIRTUALENVS_CREATE=false python -m poetry install --no-dev

# Dev
FROM base AS dev
RUN POETRY_VIRTUALENVS_CREATE=false python -m poetry install

#CMD ["python", "-m", "src.main"]

# Prod
FROM python:3.13-alpine AS prod

COPY --from=base /app/src /app/src/
COPY --from=base /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages

WORKDIR /app

USER 1000
ENTRYPOINT [ "/usr/local/bin/python", "-m" , "src.main"]
