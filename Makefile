RUFF_VERSION := 0.15.20

.PHONY: lint format fix

check:
	uvx ruff@$(RUFF_VERSION) check .

format:
	uvx ruff@$(RUFF_VERSION) format .

fix:
	uvx ruff@$(RUFF_VERSION) check . --fix