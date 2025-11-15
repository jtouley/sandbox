<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Quickstart Guide - Config-Driven Pipeline](#quickstart-guide---config-driven-pipeline)
  - [Setup (30 seconds)](#setup-30-seconds)
  - [Run Examples](#run-examples)
    - [Example 1: Default Run (Small Scale)](#example-1-default-run-small-scale)
    - [Example 2: Fast Generation (1 Million Records)](#example-2-fast-generation-1-million-records)
    - [Example 3: Process Existing day04 Data](#example-3-process-existing-day04-data)
    - [Example 4: Start Prefect Server](#example-4-start-prefect-server)
    - [Example 5: Scheduled Runs](#example-5-scheduled-runs)
  - [Configuration Tweaks](#configuration-tweaks)
    - [Edit Config for Your Needs](#edit-config-for-your-needs)
    - [Quick Environment Overrides](#quick-environment-overrides)
  - [Verify It Works](#verify-it-works)
    - [Check Output Data](#check-output-data)
    - [Inspect Delta Table](#inspect-delta-table)
  - [Common Issues](#common-issues)
    - ["Config file not found"](#config-file-not-found)
    - ["Module not found: day05_configdriven"](#module-not-found-day05_configdriven)
    - ["Missing psutil"](#missing-psutil)
  - [Next Steps](#next-steps)
  - [Quick Performance Test](#quick-performance-test)
  - [Questions?](#questions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Quickstart Guide - Config-Driven Pipeline

Get up and running in 3 minutes.

## Setup (30 seconds)

```bash
# 1. Copy environment template
cp labs/day05_configdriven/.envrc.example .envrc

# 2. Load environment
direnv allow
# Or manually: source .envrc

# 3. Verify config path
echo $PIPELINE_CONFIG
# Should show: /Users/jasontouleyrou/Projects/sandbox/labs/day05_configdriven/config/pipeline.yaml
```

## Run Examples

### Example 1: Default Run (Small Scale)
```bash
cd labs/day05_configdriven
python run_pipeline.py

# Runs:
# - Source: pubsub simulator
# - Mode: chaos (realistic delays)
# - Scale: 10,000 records
# - Strategies: bronze_append, polars_merge
# - Time: ~30 seconds
```

**Expected output:**
```
▶️  Executing pipeline...
[INFO] Generating data: source=pubsub, mode=chaos, scale=10000
[INFO] Data generated: records=11000 (includes duplicates)
[INFO] Processing strategy: bronze_append
[INFO] Processing strategy: polars_merge
[INFO] Pipeline completed!

PERFORMANCE BENCHMARKING RESULTS
Strategy             Records      Time       Throughput
bronze_append        11000        0.098s     112,245 rec/s
polars_merge         11000        0.024s     458,333 rec/s
```

### Example 2: Fast Generation (1 Million Records)
```bash
python run_pipeline.py \
  --source polars_synthetic \
  --mode speed \
  --scale 1000000

# Runs:
# - Source: Polars synthetic (no asyncio overhead)
# - Mode: speed (fast generation)
# - Scale: 1,000,000 records
# - Time: ~2 seconds total
```

**Why it's fast:**
- Polars generates 1M records in ~1 second
- No asyncio delays
- Pure DataFrame operations

### Example 3: Process Existing day04 Data
```bash
python run_pipeline.py \
  --source file \
  --path "../day04_pubsub/data/raw/pubsub_sim_*.jsonl"

# Runs:
# - Source: File ingestion
# - Reads existing JSONL from day04
# - Processes with configured strategies
```

### Example 4: Start Prefect Server
```bash
python run_pipeline.py --serve

# Starts local Prefect server
# View UI: http://127.0.0.1:4200
# Flow runs on-demand
# Press Ctrl+C to stop
```

### Example 5: Scheduled Runs
```bash
python run_pipeline.py --serve --cron "0 */6 * * *"

# Runs automatically every 6 hours
# Prefect handles scheduling
```

## Configuration Tweaks

### Edit Config for Your Needs

```bash
# Open config
vim labs/day05_configdriven/config/pipeline.yaml

# Common tweaks:
# 1. Change scale
generation:
  scale: 100000  # Change to 10, 1000, 1000000, etc.

# 2. Enable/disable strategies
strategies:
  enabled:
    - bronze_append  # Keep
    # - polars_merge  # Comment out to disable

# 3. Change chaos settings
chaos:
  mode: "random"
  dup_prob: [0.10, 0.20]  # 10-20% duplicates
```

### Quick Environment Overrides

```bash
# Change log level
export LOG_LEVEL="DEBUG"

# Change data path
export RAW_DATA_PATH="./my_custom_data/raw"

# Re-run with new settings
python run_pipeline.py
```

## Verify It Works

### Check Output Data
```bash
# List generated files
ls -lh data/raw/
# Should see: pubsub_sim_*.jsonl or synthetic_*.jsonl

# Count records
wc -l data/raw/*.jsonl

# View Delta tables
ls -lh data/delta/
# Should see: pubsub_bronze_append/, pubsub_polars_merge/
```

### Inspect Delta Table
```python
# Quick Python check
import polars as pl
from deltalake import DeltaTable

# Load bronze table
dt = DeltaTable("data/delta/pubsub_bronze_append")
df = pl.from_arrow(dt.to_pyarrow_table())

print(f"Total records: {len(df)}")
print(f"Unique composite hashes: {df['composite_hash'].n_unique()}")
print(df.head())
```

## Common Issues

### "Config file not found"
```bash
# Fix: Set explicit path
export PIPELINE_CONFIG="${PWD}/labs/day05_configdriven/config/pipeline.yaml"
```

### "Module not found: day05_configdriven"
```bash
# Fix: Ensure PYTHONPATH is set
export PYTHONPATH="${PWD}/labs:${PYTHONPATH}"

# Or reload .envrc
direnv allow
```

### "Missing psutil"
```bash
# Optional: Install for memory/CPU metrics
pip install psutil

# Or ignore (metrics will show N/A)
```

## Next Steps

1. **Read the README**: [README.md](README.md) for full documentation
2. **Customize config**: Edit `config/pipeline.yaml` for your needs
3. **Add a source**: See "Adding New Sources" in README
4. **Add a strategy**: See "Adding New Strategies" in README
5. **Run tests**: `pytest tests/test_config.py -v`

## Quick Performance Test

Benchmark all strategies with 100K records:

```bash
# Edit config
vim config/pipeline.yaml

# Set scale to 100000
generation:
  scale: 100000

# Enable all strategies
strategies:
  enabled:
    - bronze_append
    - polars_merge
    - eager_dedup

# Run
python run_pipeline.py --source polars_synthetic --mode speed

# Compare results in performance table
```

## Questions?

- Check [README.md](README.md) for detailed docs
- Check [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for architecture
- Review config files in `config/` directory
- Look at source code (well-documented, not verbose)

---

**Total setup time:** 30 seconds
**First run:** 30 seconds (small scale)
**Fast run:** 2 seconds (1M records)

Happy pipelining! ⚡
