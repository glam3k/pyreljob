PYTHON := .venv/bin/python
BIN    := .venv/bin

.PHONY: help install test unit-test e2e lint typecheck check

help: ## show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install: ## install the package for development
	pip install -e '.[dev]'

test: unit-test e2e ## run all tests (unit + end-to-end)

unit-test: ## run the SQLite unit/integration tests
	$(PYTHON) -m pytest -q tests/test_core.py

e2e: ## run integration tests against a real PostgreSQL (needs `docker compose up -d postgres`)
	TEST_DATABASE_URL=postgresql+psycopg://pyreljob:pyreljob@localhost:5433/pyreljob \
		$(PYTHON) -m pytest -q tests/test_postgres.py

lint: ## lint with ruff
	$(BIN)/ruff check .

typecheck: ## type-check with mypy (strict)
	$(BIN)/mypy pyreljob

check: lint typecheck test ## everything CI would run
