VENV = .venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip
PROJECT_ROOT = src


install:
	python -m poetry install

update:
	python -m poetry update

clean:
	rm -rf __pycache__

fmt:
	black ./

lint:
	ruff check .
	pylint src/

types:
	mypy ${PROJECT_ROOT}/ --strict

check:
	make fmt
	make lint
	make types


test-env:
	docker-compose up -d

test-cleanup:
	docker-compose down

test: test-env
	python -m pytest
	make test-cleanup

coverage: test-env
	python -m pytest --cov=src --cov-report=html --cov-fail-under=80 tests/
	make test-cleanup

run:
	python -m src.main

.PHONY: format
format:
	ruff format .
	ruff check --fix .
