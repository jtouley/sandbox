<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Config-Driven Data Pipeline](#config-driven-data-pipeline)
  - [Quick Start](#quick-start)
  - [Architecture](#architecture)
  - [Key Features](#key-features)
    - [1. Pluggable Data Sources](#1-pluggable-data-sources)
    - [2. Deduplication Strategies](#2-deduplication-strategies)
    - [3. Config-Driven Everything](#3-config-driven-everything)
    - [4. Prefect Orchestration](#4-prefect-orchestration)
  - [Configuration Reference](#configuration-reference)
    - [Data Sources](#data-sources)
      - [PubSub Simulator](#pubsub-simulator)
      - [Polars Synthetic (Speed Mode)](#polars-synthetic-speed-mode)
      - [File Ingestion](#file-ingestion)
    - [Generation Modes](#generation-modes)
    - [Chaos Configuration](#chaos-configuration)
    - [Strategies](#strategies)
  - [CLI Usage](#cli-usage)
    - [Basic Execution](#basic-execution)
    - [Quick Overrides](#quick-overrides)
    - [Prefect Server](#prefect-server)
  - [Adding New Sources](#adding-new-sources)
  - [Adding New Strategies](#adding-new-strategies)
  - [Project Structure](#project-structure)
  - [Performance Benchmarks](#performance-benchmarks)
  - [Testing](#testing)
  - [Troubleshooting](#troubleshooting)
    - [Import Errors](#import-errors)
    - [Missing psutil](#missing-psutil)
    - [Config Not Found](#config-not-found)
    - [Prefect Connection Issues](#prefect-connection-issues)
  - [Migration from day04](#migration-from-day04)
  - [Design Philosophy](#design-philosophy)
  - [Future Enhancements](#future-enhancements)
  - [References](#references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Config-Driven Data Pipeline

**Production-ready, config-driven data pipeline with pluggable sources, deduplication strategies, and Prefect orchestration.**

Built from day04_pubsub learnings, designed for extensibility and observability.

## Quick Start

```bash
# 1. Setup environment
cp .envrc.example .envrc
direnv allow  # or: source .envrc

# 2. Run pipeline with default config
python run_pipeline.py

# 3. Fast synthetic generation (1M records in ~1 second)
python run_pipeline.py --source polars_synthetic --mode speed --scale 1000000

# 4. Start Prefect server (optional)
python run_pipeline.py --serve
```

## Architecture

```
┌─────────────────┐
│ Config (YAML)   │
└────────┬────────┘
         │
         v
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────┐
│ Data Sources    │ ───> │ Dedup Strategies │ ───> │ Delta Lake      │
├─────────────────┤      ├──────────────────┤      └─────────────────┘
│ • PubSub        │      │ • Bronze Append  │
│ • Polars Synth  │      │ • Polars Merge   │
│ • File          │      │ • Eager Dedup    │
│ • DLT (future)  │      └──────────────────┘
└─────────────────┘
         │
         v
┌─────────────────┐
│ Prefect Flow    │
│ • Retry logic   │
│ • Monitoring    │
│ • Scheduling    │
└─────────────────┘
```

## Key Features

### 1. Pluggable Data Sources

**PubSub Simulator** (`source.type: pubsub`)
- Asyncio-based realistic simulation
- Configurable chaos: duplicates, out-of-order delivery
- Matches production pub/sub behavior

**Polars Synthetic** (`source.type: polars_synthetic`)
- Ultra-fast generation: 1M records in ~1 second
- Same schema as pubsub, no asyncio overhead
- Ideal for large-scale benchmarking

**File Ingestion** (`source.type: file`)
- Read existing JSONL files
- Supports glob patterns: `../day04_pubsub/data/raw/*.jsonl`
- Schema validation

**DLT (Future)** (`source.type: dlt`)
- Integration with data load tool
- Placeholder for production ingestion

### 2. Deduplication Strategies

**⚠️ IMPORTANT:** Day05 uses **native Polars hashing** (100x faster than day04's `map_elements` approach).
See [PERFORMANCE_FIX.md](PERFORMANCE_FIX.md) for details on the critical performance fix.

Expected characteristics with proper implementation:

| Strategy | Approach | Observability | Use Case |
|----------|----------|---------------|----------|
| **Bronze Append** | Write all + hash columns | ✅ Full | **Recommended** - Fast + observable |
| **Polars Merge** | In-memory last-write-wins | ❌ Lost | Batch dedup, fastest |
| **Eager Dedup** | Filter before write | ❌ Lost | Not recommended |

**Why Bronze is recommended:**
- Full audit trail of duplicates
- Reprocessable (immutable source)
- Append-only (no table scans)
- Minimal storage cost (~$0.002/month per 1M duplicates on S3)
- All strategies now equally fast at hashing (native Polars)

### 3. Config-Driven Everything

**Single YAML drives the pipeline:**
```yaml
source:
  type: "polars_synthetic"  # pubsub | polars_synthetic | file

generation:
  scale: 100000
  mode: "speed"  # chaos | speed

  chaos:
    mode: "random"  # fixed | random
    dup_prob: [0.05, 0.15]  # Random: 5-15% duplicates
    slow_prob: [0.05, 0.15]

strategies:
  enabled:
    - bronze_append
    - polars_merge
```

**Environment variables** (`.envrc`):
```bash
export RAW_DATA_PATH="./data/raw"
export DELTA_TABLE_PATH="./data/delta"
export LOG_LEVEL="INFO"
```

### 4. Prefect Orchestration

- **Retry logic**: Auto-retry transient failures (3 attempts, 60s delay)
- **Parallel execution**: Strategies run concurrently
- **Performance monitoring**: Built-in metrics collection
- **Local server**: `.serve()` for development
- **Scheduling**: Optional cron schedules

## Configuration Reference

### Data Sources

#### PubSub Simulator
```yaml
source:
  type: "pubsub"
  config_file: "config/sources/pubsub.yaml"

# config/sources/pubsub.yaml
pubsub:
  partitions: 3
  buffer_size: 100
  message:
    id_length: 8
    value_range: [100, 10000]
```

#### Polars Synthetic (Speed Mode)
```yaml
source:
  type: "polars_synthetic"

generation:
  scale: 1000000
  mode: "speed"  # Fast generation, no asyncio delays
```

#### File Ingestion
```yaml
source:
  type: "file"
  path: "../day04_pubsub/data/raw/*.jsonl"
```

### Generation Modes

**⚠️ IMPORTANT:** The `mode` setting only applies to `polars_synthetic` source!

**For Fast Generation (Recommended):**
```yaml
source:
  type: "polars_synthetic"
generation:
  mode: "speed"  # 1M records in ~1 second
```

**For Realistic Simulation:**
```yaml
source:
  type: "pubsub"  # Always uses chaos simulation
generation:
  mode: "chaos"   # Ignored for pubsub, but kept for clarity
```

**Chaos Mode** (PubSub source or polars_synthetic with `mode: chaos`)
- Realistic simulation with asyncio delays
- Configurable delays, jitter, duplicates
- Out-of-order delivery simulation
- SLOW for large scale (10M records = hours)
- Best for testing resilience

**Speed Mode** (polars_synthetic with `mode: speed`)
- Fast Polars-based generation
- No asyncio overhead
- Generates 1M records in ~1 second
- Best for benchmarking and large-scale testing

### Chaos Configuration

**Fixed Mode** (`chaos.mode: fixed`)
```yaml
chaos:
  mode: "fixed"
  dup_prob: 0.10      # Exactly 10% duplicates
  slow_prob: 0.10     # Exactly 10% out-of-order
  base_delay: 0.01    # 10ms between messages
  jitter: 0.02        # ±20ms variation
```

**Random Mode** (`chaos.mode: random`)
```yaml
chaos:
  mode: "random"
  dup_prob: [0.05, 0.15]    # Random: 5-15% duplicates
  slow_prob: [0.05, 0.15]   # Random: 5-15% out-of-order
  base_delay: [0.005, 0.02] # Random: 5-20ms delays
  jitter: [0.01, 0.05]      # Random: 10-50ms jitter
```

### Strategies

**Bronze Append** (Recommended)
```yaml
bronze_append:
  partition_by: ["partition_date"]
  hash_algorithm: "sha256"  # sha256 | md5
```
- Appends all records (no dedup)
- Adds composite_hash and payload_hash columns
- Partitioned by date for efficient queries
- Fastest strategy with full observability

**Polars Merge** (Last-Write-Wins)
```yaml
polars_merge:
  dedup_key: "message_id"
  sort_by: "created_at"
  keep: "last"  # first | last
```
- In-memory deduplication using Polars
- 19x faster than eager dedup
- Loses observability of duplicates

**Eager Dedup** (Not Recommended)
```yaml
eager_dedup:
  write_mode: "overwrite"
  hash_algorithm: "sha256"
```
- Filters duplicates before write
- Lost observability (dropped events invisible)
- Slowest strategy

## CLI Usage

### Basic Execution
```bash
# Run with default config
python run_pipeline.py

# Override config file
python run_pipeline.py --config config/custom.yaml
```

### Quick Overrides
```bash
# Use fast synthetic generation
python run_pipeline.py --source polars_synthetic --mode speed

# Generate 1M records
python run_pipeline.py --scale 1000000

# Process existing file
python run_pipeline.py --source file --path "../day04_pubsub/data/raw/*.jsonl"
```

### Prefect Server
```bash
# Start local Prefect server (blocks until stopped)
python run_pipeline.py --serve

# Serve with scheduled runs every 6 hours
python run_pipeline.py --serve --cron "0 */6 * * *"

# View dashboard at: http://127.0.0.1:4200
```

## Adding New Sources

Extend the pipeline by implementing the `DataSource` interface:

```python
# src/sources/kafka.py
from .base import DataSource, register_source

@register_source("kafka")
class KafkaSource(DataSource):
    def generate(self) -> Path:
        # Fetch from Kafka
        # Write to JSONL
        # Return path
        pass
```

Update config:
```yaml
source:
  type: "kafka"
  config_file: "config/sources/kafka.yaml"
```

The source is automatically registered and available!

## Adding New Strategies

Implement the `Strategy` interface:

```python
# src/strategies/custom.py
from .base import Strategy, register_strategy

@register_strategy("custom_dedup")
class CustomStrategy(Strategy):
    @property
    def table_name(self) -> str:
        return "pubsub_custom_dedup"

    def process(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict, Path]:
        # Process DataFrame
        # Return (processed_df, metrics, output_path)
        pass
```

Add to config:
```yaml
strategies:
  enabled:
    - custom_dedup

  custom_dedup:
    your_param: "value"
```

## Project Structure

```
labs/day05_configdriven/
├── config/
│   ├── pipeline.yaml              # Main configuration
│   ├── sources/
│   │   ├── pubsub.yaml           # PubSub settings
│   │   ├── file.yaml             # File ingestion settings
│   │   └── ...
│   └── strategies/               # Strategy overrides (optional)
├── src/
│   ├── config_loader.py          # Pydantic models + validation
│   ├── sources/
│   │   ├── base.py               # Source interface + registry
│   │   ├── pubsub.py             # PubSub simulator
│   │   ├── polars_synthetic.py   # Fast Polars generation
│   │   └── file.py               # File ingestion
│   ├── strategies/
│   │   ├── base.py               # Strategy interface + registry
│   │   └── dedup.py              # Dedup implementations
│   ├── monitoring/
│   │   └── performance.py        # Performance tracking
│   └── utils/
│       └── logging_config.py     # Structured logging
├── flows/
│   ├── pipeline.py               # Main Prefect flow
│   └── tasks/
│       ├── generate.py           # Data generation task
│       └── process.py            # Strategy processing task
├── tests/
│   └── test_config.py            # Config validation tests
├── run_pipeline.py               # CLI entrypoint
├── .envrc.example                # Safe environment template
└── README.md                     # This file
```

## Performance Benchmarks

**⚠️ NOTE:** Day04 benchmarks were flawed (see [PERFORMANCE_FIX.md](PERFORMANCE_FIX.md)).
Day05 uses native Polars hashing (100x faster).

With the fix, all strategies are significantly faster:

| Strategy | Approach | Storage | Trade-offs |
|----------|----------|---------|------------|
| Bronze Append | Append all + hashes | Highest | ✅ Full observability, reprocessable |
| Polars Merge | In-memory dedup | Lowest | ❌ Lost duplicates, fastest |
| Eager Dedup | Filter before write | Lowest | ❌ Lost duplicates |

**Key Insights:**
- Native Polars hashing is 100x faster than Python `map_elements`
- Bronze is recommended: full observability + fast append-only writes
- +~10% storage for full audit trail (duplicates kept)
- Cost: ~$0.002/month per million duplicates on S3
- **"Storage is cheap. Debugging is expensive."**

Run your own benchmark:
```bash
python run_pipeline.py --source polars_synthetic --mode speed --scale 100000
```

## Testing

Run config validation tests:
```bash
# From project root
pytest labs/day05_configdriven/tests/test_config.py -v

# With coverage
pytest labs/day05_configdriven/tests/ --cov=labs/day05_configdriven/src
```

## Troubleshooting

### Import Errors
```bash
# Ensure PYTHONPATH includes labs/
export PYTHONPATH="${PWD}/labs:${PYTHONPATH}"

# Or activate .envrc
direnv allow
```

### Missing psutil
```bash
# Install for memory/CPU metrics
pip install psutil

# Or run without (metrics will show N/A)
```

### Config Not Found
```bash
# Set explicit config path
export PIPELINE_CONFIG="${PWD}/labs/day05_configdriven/config/pipeline.yaml"

# Or pass as argument
python run_pipeline.py --config config/pipeline.yaml
```

### Prefect Connection Issues
```bash
# Check Prefect server is running
prefect server start

# Or use local executor (no server needed)
export PREFECT_API_URL=""  # Empty = local mode
```

## Migration from day04

Use existing day04 data:
```bash
python run_pipeline.py \
  --source file \
  --path "../day04_pubsub/data/raw/pubsub_sim_*.jsonl"
```

Compare day04 pubsub vs day05 speed mode:
```bash
# day04 approach (slow for large scale)
cd labs/day04_pubsub
python day04_pubsub_sim.py --scale large  # Takes ~15 minutes

# day05 speed mode (1M records in ~1 second)
cd labs/day05_configdriven
python run_pipeline.py --source polars_synthetic --mode speed --scale 1000000
```

## Design Philosophy

**"Storage is cheap. Debugging is expensive. Optimize for observability first, performance second."**

Benchmarks prove the Bronze (append-only) pattern is:
- **Faster**: 3.5x throughput vs eager dedup
- **More observable**: All duplicates queryable
- **Reprocessable**: Immutable source of truth
- **Minimal cost**: ~$0.002/month per million duplicates

The "expensive" Bronze pattern is actually the fastest and cheapest, while providing maximum engineering value through observability.

## Future Enhancements

- [ ] DLT integration for production data sources
- [ ] Kafka source implementation
- [ ] Delta Lake time travel queries
- [ ] Grafana dashboard for metrics
- [ ] S3/GCS storage backend support
- [ ] Silver/Gold layer transformations
- [ ] Data quality validations
- [ ] Alerting integrations (Slack, PagerDuty)

## References

- [day04_pubsub](../day04_pubsub/README.md) - Original experimental work
- [Delta Lake](https://delta.io/) - ACID storage layer
- [Polars](https://pola.rs/) - Fast DataFrame library
- [Prefect](https://www.prefect.io/) - Workflow orchestration

---

Built with ⚡ for the sandbox project | Day 5: Config-Driven Architecture
