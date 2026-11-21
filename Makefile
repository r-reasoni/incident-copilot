# ===========================================================================
# Incident Copilot – Makefile
# Optional convenience targets; Windows PowerShell users can use the direct
# docker compose, pytest, and uvicorn commands documented in README.md.
# ===========================================================================

.DEFAULT_GOAL := help
SHELL         := /bin/bash

# ---------------------------------------------------------------------------
# Variables (override on the command line, e.g. make test PYTEST_ARGS=-x)
# ---------------------------------------------------------------------------
COMPOSE_FILE  ?= docker-compose.yml
PYTEST_ARGS   ?= -v
API_HOST      ?= 0.0.0.0
API_PORT      ?= 8000

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help:            ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
.PHONY: up
up:              ## Start PostgreSQL (and any future services) via Docker Compose
	docker compose -f $(COMPOSE_FILE) up -d --wait
	@echo "✅  PostgreSQL is up on port 5432"

.PHONY: down
down:            ## Stop and remove containers (data volume preserved)
	docker compose -f $(COMPOSE_FILE) down
	@echo "🛑  Containers stopped"

.PHONY: down-volumes
down-volumes:    ## Stop containers AND destroy the Postgres data volume
	docker compose -f $(COMPOSE_FILE) down -v
	@echo "🗑️   Containers and volumes removed"

.PHONY: logs
logs:            ## Tail container logs
	docker compose -f $(COMPOSE_FILE) logs -f

# ---------------------------------------------------------------------------
# Python environment
# ---------------------------------------------------------------------------
.PHONY: install
install:         ## Install project + dev dependencies into the active venv
	pip install -e ".[dev]"

# ---------------------------------------------------------------------------
# Database migrations (Alembic – stubs, populated in later prompts)
# ---------------------------------------------------------------------------
.PHONY: migrate
migrate:         ## Run Alembic migrations (upgrade head)
	alembic upgrade head

.PHONY: migration
migration:       ## Generate a new Alembic migration (MSG="describe change")
	alembic revision --autogenerate -m "$(MSG)"

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
.PHONY: test
test:            ## Run the full pytest suite
	pytest $(PYTEST_ARGS)

.PHONY: test-cov
test-cov:        ## Run pytest with HTML coverage report
	pytest --cov=app --cov-report=html --cov-report=term-missing $(PYTEST_ARGS)
	@echo "📊 Coverage report: htmlcov/index.html"

# ---------------------------------------------------------------------------
# Linting / type-checking
# ---------------------------------------------------------------------------
.PHONY: lint
lint:            ## Run ruff linter
	ruff check .

.PHONY: format
format:          ## Auto-format with ruff
	ruff format .

.PHONY: typecheck
typecheck:       ## Run mypy
	mypy app cli

# ---------------------------------------------------------------------------
# API server
# ---------------------------------------------------------------------------
.PHONY: run-api
run-api:         ## Start the FastAPI dev server (auto-reload)
	uvicorn app.api.main:app --host $(API_HOST) --port $(API_PORT) --reload

# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------
.PHONY: dev
dev: up install  ## Bring up infra + install deps in one shot

.PHONY: clean
clean:           ## Remove Python cache files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .mypy_cache .ruff_cache .pytest_cache htmlcov .coverage
	@echo "🧹 Clean complete"
