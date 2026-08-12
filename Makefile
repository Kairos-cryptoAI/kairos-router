UV ?= uv

.PHONY: install lint format format-check typecheck security test build run all
install:
	$(UV) sync --locked
format:
	$(UV) run --locked ruff format kairos_router tests
format-check:
	$(UV) run --locked ruff format --check kairos_router tests
lint:
	$(UV) run --locked ruff check kairos_router tests
typecheck:
	$(UV) run --locked mypy kairos_router
security:
	$(UV) run --locked bandit -q -r kairos_router -x tests
test:
	$(UV) run --locked pytest -q --tb=short
build:
	$(UV) build --no-sources
run:
	$(UV) run --locked python -m kairos_router
all: lint format-check typecheck security test build
