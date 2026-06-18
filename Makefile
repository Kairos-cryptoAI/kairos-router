.PHONY: install lint test format run
install:
	pip install -e ".[dev]"
format:
	ruff format kairos_router tests
lint:
	ruff check kairos_router tests
test:
	pytest -q
run:
	python -m kairos_router
