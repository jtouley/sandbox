# comment-guard: capture -> bronze -> silver -> gold
CG := uv run cg

.PHONY: all setup login whoami capture capture-all build peek test lint clean

all: test ## Default: run the test suite

setup: ## Create the venv (Python 3.12) from uv.lock
	uv sync --locked

login: ## Store an Instagram access token (keyring, or a 0600 file)
	$(CG) login

whoami: ## Show the logged-in account and token expiry
	$(CG) whoami

capture: ## Land raw API responses for the 25 most recent posts
	$(CG) capture

capture-all: ## Land every post's comments (slow on big accounts)
	$(CG) capture --all

build: ## dbt build: bronze -> silver -> gold Parquet + tests
	$(CG) build

peek: ## Query the raw landing JSONL directly (no build needed)
	$(CG) peek

test: ## Unit + end-to-end tests against a mocked Graph API
	uv run pytest

lint: ## Ruff lint + format check
	uv run ruff check . && uv run ruff format --check .

clean: ## Remove dbt build output (never touches data/landing)
	rm -rf dbt/target dbt/logs dbt/dbt_packages
