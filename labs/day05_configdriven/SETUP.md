<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Setup Instructions](#setup-instructions)
  - [Install Dependencies](#install-dependencies)
  - [Verify Installation](#verify-installation)
  - [Run the Pipeline](#run-the-pipeline)
  - [Linting](#linting)
  - [Dependencies Added](#dependencies-added)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Setup Instructions

## Install Dependencies

The day05_configdriven pipeline requires additional dependencies that have been added to `pyproject.toml`:

```bash
# From project root
uv sync

# This will install:
# - structlog (structured logging)
# - pyyaml (YAML config parsing)
# - pydantic (config validation)
```

## Verify Installation

```bash
# Test imports
python -c "import structlog; import yaml; import pydantic; print('✅ All dependencies installed')"

# Run config tests
cd labs/day05_configdriven
pytest tests/test_config.py -v
```

## Run the Pipeline

```bash
cd labs/day05_configdriven

# Quick test (small scale)
python run_pipeline.py

# Fast generation (100K records)
python run_pipeline.py --source polars_synthetic --mode speed --scale 100000
```

## Linting

The code follows the project's black/ruff standards:

```bash
# From project root
make fmt    # Auto-format
make check  # Verify formatting
```

**Note:** A few linting exceptions have been added:
- `# noqa: PLR0913` for async methods with many parameters (acceptable for internal methods)
- Line breaks for long log messages (stays within 100 char limit)

## Dependencies Added

| Package | Purpose |
|---------|---------|
| `structlog` | Structured logging with context |
| `pyyaml` | YAML configuration parsing |
| `pydantic` | Type-safe config validation |

All other dependencies (polars, deltalake, prefect) were already in the project.
