# Local development setup: virtual environment, Python dependencies and pre-commit hooks.
# Run `make setup` once after cloning; rerun it after changing requirements.txt or
# .pre-commit-config.yaml. Each step only reruns when its input file has changed.

# Interpreter used to create the venv; must match .python-version.
PYTHON ?= python3.11
VENV   := .venv
BIN    := $(VENV)/bin

.PHONY: setup lint test clean

setup: $(VENV)/.requirements-installed .git/hooks/pre-commit

# 1. Create the virtual environment if it does not exist yet.
$(BIN)/python:
	$(PYTHON) -m venv $(VENV)

# 2. Install requirements.txt; the stamp file records that it was installed.
$(VENV)/.requirements-installed: requirements.txt $(BIN)/python
	$(BIN)/python -m pip install --upgrade pip
	$(BIN)/python -m pip install -r requirements.txt
	touch $@

# 3. Install the git hook and download the hook environments from .pre-commit-config.yaml.
.git/hooks/pre-commit: .pre-commit-config.yaml $(VENV)/.requirements-installed
	$(BIN)/pre-commit install
	$(BIN)/pre-commit install-hooks

lint: setup
	$(BIN)/pre-commit run --all-files

test: setup
	$(BIN)/pytest tests

clean:
	rm -rf $(VENV)
