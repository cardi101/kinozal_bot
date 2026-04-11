PYTHON ?= python3
VENV ?= .venv
VENV_PYTHON := $(VENV)/bin/python
PIP := $(VENV_PYTHON) -m pip

.PHONY: install lint test check smoke

$(VENV_PYTHON):
	$(PYTHON) -m venv $(VENV)

install: $(VENV_PYTHON)
	$(PIP) install -r requirements.txt pytest ruff

lint: $(VENV_PYTHON)
	$(VENV_PYTHON) -m ruff check .

test: $(VENV_PYTHON)
	$(VENV_PYTHON) -m pytest -q

check: lint test

smoke:
	docker compose config --quiet
	docker compose up -d --build postgres redis api
	@for attempt in $$(seq 1 20); do \
		if docker compose exec -T api curl -fsS http://127.0.0.1:8000/health >/dev/null; then \
			break; \
		fi; \
		sleep 2; \
		if [ $$attempt -eq 20 ]; then \
			echo "API healthcheck did not pass in time" >&2; \
			exit 1; \
		fi; \
	done
	docker compose exec -T api curl -fsS http://127.0.0.1:8000/health
	docker compose exec -T api python smoke_bootstrap.py api
	docker compose exec -T api python smoke_bootstrap.py app
	docker compose exec -T api python smoke_repositories.py
	docker compose exec -T postgres psql -U postgres -d kinozal_news -c "select version, name from schema_migrations order by version;"
