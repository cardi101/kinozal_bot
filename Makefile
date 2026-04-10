PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
PIP := $(VENV_PYTHON) -m pip

.PHONY: install lint test check

$(VENV_PYTHON):
	$(PYTHON) -m venv $(VENV)

install: $(VENV_PYTHON)
	$(PIP) install -r requirements.txt pytest ruff

lint: $(VENV_PYTHON)
	$(VENV_PYTHON) -m ruff check .

test: $(VENV_PYTHON)
	$(VENV_PYTHON) -m pytest -q

check: lint test
