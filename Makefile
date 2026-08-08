RUFF_VERSION := 0.15.20

.PHONY: lint format fix up down create-topics run-producer test-shared test-shared-cov test-ingestion test-ingestion-cov

check:
	uvx ruff@$(RUFF_VERSION) check .

format:
	uvx ruff@$(RUFF_VERSION) format .

fix:
	uvx ruff@$(RUFF_VERSION) check . --fix

up:
	docker compose up -d

down:
	docker compose down -v

run-producer:
	uv run python -m ingestion.cli

test-shared:
	uv run --package shared pytest packages/shared/tests/*

test-shared-cov:
	uv run --package shared pytest packages/shared/tests/ --cov=shared 

test-ingestion:
	uv run --package ingestion pytest packages/ingestion/tests/*

test-ingestion-cov:
	uv run --package ingestion pytest packages/ingestion/tests/ --cov=ingestion 

test-lakehouse:
	uv run --package lakehouse pytest packages/lakehouse/tests/*

test-lakehouse-cov:
	uv run --package lakehouse pytest packages/lakehouse/tests/* --cov=lakehouse 