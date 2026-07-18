RUFF_VERSION := 0.15.20

.PHONY: lint format fix up down create-topics run-producer

check:
	uvx ruff@$(RUFF_VERSION) check .

format:
	uvx ruff@$(RUFF_VERSION) format .

fix:
	uvx ruff@$(RUFF_VERSION) check . --fix

up:
	docker compose up -d
	sleep 5
	$(MAKE) create-topics

down:
	docker compose down -v

create-topics:
	./create-topics.sh

run-producer:
	uv run python -m ingestion.cli