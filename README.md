<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [🧪 Sandbox](#%F0%9F%A7%AA-sandbox)
  - [📚 Focus Areas](#-focus-areas)
  - [🧩 Structure](#%F0%9F%A7%A9-structure)
  - [🧠 Principles](#-principles)
  - [⚙️ Environment](#-environment)
    - [System Requirements](#system-requirements)
    - [Why This Setup?](#why-this-setup)
    - [Setup Instructions](#setup-instructions)
      - [1. **Install Prerequisites** (One-time)](#1-install-prerequisites-one-time)
      - [2. **Clone & Initialize**](#2-clone--initialize)
      - [3. **Verify Setup**](#3-verify-setup)
    - [Available Commands](#available-commands)
    - [Environment Variables](#environment-variables)
  - [📖 Journal Examples](#-journal-examples)
  - [🧪 Example Experiment](#%F0%9F%A7%AA-example-experiment)
  - [✅ Quick Start](#-quick-start)
  - [🐛 Troubleshooting](#-troubleshooting)
  - [📝 License](#-license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 🧪 Sandbox

Structured environment for **systematic learning**, **benchmarking**, and **experimentation** across modern data-engineering tools.

This repository exists to sharpen intuition around systems, performance, and design—not to build production code.
Each folder represents a level of increasing rigor: exploratory notebooks for ideation, and reproducible labs for validation.

---

## 📚 Focus Areas

| Domain | Primary Goal |
|---------|---------------|
| **Polars** | Build intuition for lazy vs. eager evaluation, memory behavior, and parallel execution. |
| **Delta / Parquet** | Study schema evolution, partitioning, and compaction strategies. |
| **Prefect** | Explore declarative orchestration, flow design, and retry semantics. |
| **Pulumi** | Strengthen infrastructure-as-code fundamentals and environment reproducibility. |
| **Python / DuckDB** | Prototype efficient local pipelines for testing ideas quickly. |

---

## 🧩 Structure

```
sandbox/
├── labs/          # Deterministic, script-based experiments (timed, benchmarked)
├── notebooks/     # Exploratory analyses and visualizations
├── journal/       # Daily reflections and structured learning notes
├── Makefile       # Command entrypoint for setup, linting, and repeatable tasks
├── pyproject.toml # uv-managed environment definition
├── .envrc         # direnv configuration (auto-loads env vars on cd)
├── .env.example   # Template for local environment variables
└── README.md
```

---

## 🧠 Principles

1. **Depth before breadth.** Understand how systems behave, not just how to use them.
2. **Reproducibility is rigor.** Every experiment should be rerunnable.
3. **Measure, then optimize.** Benchmark before you form opinions.
4. **Declarative bias.** Express *intent* before *execution*.
5. **Transparency.** Share learnings openly and raise the standard of practice.

---

## ⚙️ Environment

### System Requirements

- **macOS** (Apple Silicon optimized)
- **iTerm 2** + VS Code
- **[uv](https://docs.astral.sh/uv/)** for fast, deterministic dependency management
- **[direnv](https://direnv.net/)** for automatic environment variable loading
- **Python ≥ 3.14**

### Why This Setup?

| Tool | Purpose | Why It Matters |
|------|---------|----------------|
| **uv** | Python package management | 10-100x faster than pip/poetry; deterministic lockfiles; single binary. |
| **direnv** | Auto-load `.envrc` | No manual `source activate`; clean shell state; prevents leaking secrets. |
| **pre-commit** | Git hooks | Catch formatting/lint issues before commits; enforce consistency. |
| **Makefile** | Command centralization | Single entrypoint for common tasks; self-documenting with `make help`. |

### Setup Instructions

#### 1. **Install Prerequisites** (One-time)

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install direnv (if not already installed)
brew install direnv

# Add direnv hook to your shell config (~/.zshrc or ~/.bash_profile)
eval "$(direnv hook zsh)"  # or bash, if using bash
```

#### 2. **Clone & Initialize**

```bash
# Clone the repository
git clone https://github.com/jtouley/sandbox.git
cd sandbox

# direnv will prompt you—allow it
direnv allow

# Bootstrap the environment (creates venv, installs deps, sets up hooks)
make init
```

**What `make init` does:**
- Creates a `.venv` in the project root using `uv`
- Installs all dependencies from `pyproject.toml` (dev + runtime)
- Installs pre-commit hooks in `.git/hooks`
- Verifies the Python version and environment

#### 3. **Verify Setup**

```bash
# Check Python version
python --version  # Should be 3.14+

# List environment variables
env | grep SANDBOX  # Should show SANDBOX_ENV=local
```

### Available Commands

```bash
make help          # Show all available commands

# Environment
make init          # Bootstrap venv, deps, and hooks
make install       # Update dependencies
make hooks         # Install/reinstall pre-commit hooks

# Code quality
make fmt           # Format code (black) and fix lint (ruff)
make lint          # Lint only (no fixes)
make check         # Run all static checks (pre-commit)
make test          # Run unit tests

# Notebooks
make nb-clean      # Strip outputs from all notebooks

# Journaling
make day DAY=01 TOPIC="Polars Warmup"  # Create new journal entry
```

### Environment Variables

The `.envrc` file automatically loads environment variables when you `cd` into the project. **Do not commit `.envrc` with secrets.**

**Key Variables:**

| Variable | Purpose |
|----------|---------|
| `SANDBOX_ENV` | Marks this as a local dev environment |
| `POLARS_MAX_THREADS` | Limits parallelism to 8 cores (adjust for your machine) |
| `DATA_ROOT` | Root path for all data artifacts (`./data`) |
| `PYTHONPATH` | Includes `labs/` for direct script imports |
| `PREFECT_API_URL` | Local Prefect server endpoint |
| `PULUMI_BACKEND_URL` | Local Pulumi state backend |

**To customize for your machine:** Copy `.env.example` to `.env`, edit, and update `.envrc` to source it:

```bash
# In .envrc, add:
if [ -f .env ]; then
  dotenv .env
fi
```

---

## 📖 Journal Examples

| Day | Topic | Summary |
|-----|-------|---------|
| 01 | Systems Grounding & Polars | Benchmarked join and groupby memory profiles |
| 02 | Lazy Evaluation | Compared optimized vs. unoptimized plans |
| 03 | Delta vs Parquet | Examined schema enforcement and compaction |

---

## 🧪 Example Experiment

```python
import polars as pl

df = pl.DataFrame({
    "id": pl.arange(0, 1_000_000),
    "group": (pl.arange(0, 1_000_000) % 10).cast(pl.Utf8),
    "val": pl.int_range(0, 1_000_000)
})

(
    df.lazy()
      .group_by("group")
      .agg(pl.col("val").mean())
      .collect()
)
```

---

## ✅ Quick Start

```bash
# Clone and initialize
git clone https://github.com/jtouley/sandbox.git && cd sandbox
direnv allow && make init

# Verify environment
python --version && make check

# Create your first journal entry
make day DAY=01 TOPIC="Getting Started"

# Format and lint
make fmt && make check
```

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| `command not found: direnv` | Install: `brew install direnv` and add hook to shell config |
| `direnv: error: .envrc:1: line 1: syntax error: unexpected token 'X'` | Run `direnv allow` after fixing `.envrc` |
| `uv: command not found` | Install: `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| `make: command not found` | macOS: already installed; check `which make` |
| Python version mismatch | Run `uv venv --python 3.14` to force version |

---

## 📝 License

MIT
