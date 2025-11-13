<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Day 02: Delta vs Parquet Trade-offs](#day-02-delta-vs-parquet-trade-offs)
  - [Quick Start](#quick-start)
  - [Experiments](#experiments)
    - [1. Schema Enforcement](#1-schema-enforcement)
    - [2. Partitioning Strategy](#2-partitioning-strategy)
    - [3. Multi-file Aggregation](#3-multi-file-aggregation)
    - [4. Schema Evolution](#4-schema-evolution)
    - [5. Compression Trade-offs](#5-compression-trade-offs)
  - [Project Structure](#project-structure)
  - [Output](#output)
  - [Key Questions Answered](#key-questions-answered)
  - [Next Steps](#next-steps)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Day 02: Delta vs Parquet Trade-offs

Comprehensive experiments comparing Parquet and Delta Lake across multiple dimensions.

## Quick Start

```bash
# Install dependencies
uv sync

# Run all experiments
python -m labs.day02_delta_vs_parquet.run_all

# Run specific experiments
python -m labs.day02_delta_vs_parquet.run_all --experiments 1,4
```

## Experiments

### 1. Schema Enforcement
**File:** `exp01_schema_enforcement.py`

Compares schema-on-read (Parquet) vs schema-on-write (Delta).

**Run individually:**
```bash
python -m labs.day02_delta_vs_parquet.exp01_schema_enforcement
```

**What it tests:**
- Write/read performance
- Storage overhead
- Schema validation timing

**Key insight:** Delta adds ~2-5% overhead but catches schema issues at write time.

---

### 2. Partitioning Strategy
**File:** `exp02_partitioning.py`

Compares manual (Parquet) vs automatic (Delta) partitioning.

**Run individually:**
```bash
python -m labs.day02_delta_vs_parquet.exp02_partitioning
```

**What it tests:**
- Partition write performance
- Partition-pruned read performance
- Metadata management

**Key insight:** Delta's _delta_log centralizes partition metadata, simplifying management at scale.

---

### 3. Multi-file Aggregation
**File:** `exp03_multi_file_aggregation.py`

Compares query performance across many small files.

**Run individually:**
```bash
python -m labs.day02_delta_vs_parquet.exp03_multi_file_aggregation
```

**What it tests:**
- Incremental write patterns
- Aggregation performance
- Metadata overhead

**Key insight:** Delta's centralized metadata reduces query planning time with 100s+ of files.

---

### 4. Schema Evolution
**File:** `exp04_schema_evolution.py`

Compares adding new columns to existing datasets.

**Run individually:**
```bash
python -m labs.day02_delta_vs_parquet.exp04_schema_evolution
```

**What it tests:**
- Rewrite cost (Parquet) vs append cost (Delta)
- Schema versioning
- Backward compatibility

**Key insight:** Delta schema evolution is ~10-100x faster (append vs full rewrite).

---

### 5. Compression Trade-offs
**File:** `exp05_compression.py`

Compares compression codecs (uncompressed, snappy, gzip).

**Run individually:**
```bash
python -m labs.day02_delta_vs_parquet.exp05_compression
```

**What it tests:**
- Compression ratios
- Write/read performance
- Storage vs CPU trade-offs

**Key insight:** Snappy provides the best balance for most workloads.

---

## Project Structure

```
labs/day02_delta_vs_parquet/
├── __init__.py                      # Package initialization
├── config.py                        # Shared config and utilities
├── exp01_schema_enforcement.py      # Experiment 1
├── exp02_partitioning.py            # Experiment 2
├── exp03_multi_file_aggregation.py  # Experiment 3
├── exp04_schema_evolution.py        # Experiment 4
├── exp05_compression.py             # Experiment 5
├── run_all.py                       # Orchestrator
└── README.md                        # This file
```

## Output

All experiments generate:
- **Verbose logging** of each step
- **Performance metrics** (timing, file sizes)
- **Comparison tables** (Parquet vs Delta)
- **Key insights** summary

Data files are written to `labs/data/`.

## Key Questions Answered

1. **How does schema enforcement differ?**
   - Parquet: Schema-on-read (no write-time validation)
   - Delta: Schema-on-write (enforces and versions schema)

2. **What's the performance cost of Delta overhead?**
   - Typically 2-5% storage overhead for _delta_log
   - Write time: Similar or slightly slower
   - Read time: Comparable

3. **How do partition strategies affect performance?**
   - Parquet: Manual path management
   - Delta: Automatic filter pushdown and pruning

4. **What's the memory footprint for large datasets?**
   - Both are columnar and memory-efficient
   - Delta's centralized metadata reduces query planning memory

## Next Steps

After running these experiments, consider:
1. Testing with your own data patterns
2. Exploring Delta time travel (`dt.load_version(0)`)
3. Benchmarking concurrent reads/writes
4. Measuring compaction performance
