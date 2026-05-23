.PHONY: install test lint format docs check

UV ?= uv
UV_CACHE_DIR ?= .uv-cache
NO_MKDOCS_2_WARNING ?= true
PYTHON_TARGETS := src tests

export UV_CACHE_DIR
export NO_MKDOCS_2_WARNING

install:
	$(UV) sync --all-groups

test:
	$(UV) run pytest

lint:
	$(UV) run ruff check $(PYTHON_TARGETS)
	$(UV) run mypy $(PYTHON_TARGETS)

format:
	$(UV) run ruff format $(PYTHON_TARGETS)
	$(UV) run ruff check --fix $(PYTHON_TARGETS)

docs:
	$(UV) run --group docs mkdocs build --strict

check:
	$(UV) run ruff format --check $(PYTHON_TARGETS)
	$(UV) run ruff check $(PYTHON_TARGETS)
	$(UV) run mypy $(PYTHON_TARGETS)
	$(UV) run pytest
