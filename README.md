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

- macOS (Apple Silicon)
- iTerm 2 + VS Code
- [uv](https://docs.astral.sh/uv/) for dependency and environment management
- Python ≥ 3.12

### Setup

```bash
# Clone
git clone https://github.com/jtouley/sandbox.git
cd sandbox

# Bootstrap everything (venv, deps, hooks)
make init

# Run help for available commands
make help
```

### Available Commands

```
check                  Run all static checks
day                    Create a new journal entry
fmt                    Format code
init                   Create venv, install hooks, and verify setup
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
make fmt && make check                      # Format and lint
make day DAY=01 TOPIC="Polars Warmup"       # Create new journal entry
```