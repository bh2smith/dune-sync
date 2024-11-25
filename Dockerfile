FROM python:3.13-alpine

RUN python -m pip install poetry
RUN mkdir /app
COPY src /app/src
COPY poetry.lock /app/poetry.lock
COPY pyproject.toml /app/pyproject.toml
WORKDIR /app
RUN POETRY_VIRTUALENVS_CREATE=false python -m poetry install
USER 1000
ENTRYPOINT [ "python", "-m" , "src.main"]
