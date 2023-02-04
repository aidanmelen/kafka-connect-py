NAME = kafka-connect-py
VERSION = $(shell poetry version -s)

SHELL := /bin/bash

.PHONY: help all

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

build:  ## Build docker image
	docker build . --tag $(NAME)

dev:  ## Build docker image
	docker run -it --rm --entrypoint /bin/sh $(NAME)

lint:  ## Lint python
	poetry run black --line-length 100 src tests --exclude src/kafka_connect/cli.py

test:  ## Test python
	poetry run coverage run -m unittest discover tests -v

coverage: test ## Test python
	poetry run coverage report --include "src/kafka_connect/**" -m
	poetry run coverage lcov

release: lint test  ## Push tags and trigger Github Actions release.
	git tag $(VERSION)
	git push --tags

clean: ## Remove Python cache files.
	@rm -rf build dist .eggs *.egg-info .venv requirements.txt
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +