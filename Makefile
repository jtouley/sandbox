# ---- Configuration ----
PY := uv run python
UV := uv
PC := uv run pre-commit
PKG := sandbox

# Default target
.DEFAULT_GOAL := help

# Colors (for nicer logs)
GREEN := \033[32m
NC := \033[0m

.PHONY: help all clean test init install hooks fmt lint check nb-clean day

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS(":.*?## ")}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

all: ## Run all checks and tests
	@$(MAKE) check
	@$(MAKE) test

clean: ## Clean generated files and caches
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)Cleaned cache files.$(NC)"

# ---- Environment ----
init: ## Create venv, install hooks, and verify setup
	@if [ -z "$$SANDBOX_ENV" ]; then echo "⚠️  direnv not loaded. Run: direnv allow"; exit 1; fi
	$(UV) venv
	$(UV) sync
	$(PC) install
	@echo "$(GREEN)Environment initialized.$(NC)"

install: ## Sync dependencies declared in pyproject.toml
	$(UV) sync

hooks: ## Install pre-commit hooks locally
	$(PC) install
	@echo "$(GREEN)Pre-commit hooks installed.$(NC)"

# ---- Quality ----
fmt: ## Format code (black) and fix lint (ruff)
	$(PY) -m black .
	$(PY) -m ruff check --fix .

lint: ## Lint only (ruff)
	$(PY) -m ruff check .

check: ## Run all static checks
	$(PC) run --all-files

test: ## Run unit tests (pytest)
	$(PY) -m pytest -q

# ---- Notebooks ----
nb-clean: ## Strip outputs from notebooks
	$(PC) run nbstripout --all-files

# ---- Journal ----
# Usage: make day DAY=06 TOPIC="Delta vs Parquet"
day: ## Create a new journal file from template (requires DAY=NN and optional TOPIC)
	@if [ -z "$(DAY)" ]; then echo "Usage: make day DAY=NN TOPIC='Topic'"; exit 2; fi
	@mkdir -p journal
	@topic="$${TOPIC:-Untitled}" ; \
	sed "s/{{day_number}}/$(DAY)/g;s/{{focus_area}}/$${topic}/g" journal/_template.md > journal/day$(DAY)_$$(echo "$${topic}" | tr '[:upper:]' '[:lower:]' | tr ' ' '_' | tr -cd '[:alnum:]_').md ; \
	echo "$(GREEN)Created journal/day$(DAY)_*.md$(NC)"
