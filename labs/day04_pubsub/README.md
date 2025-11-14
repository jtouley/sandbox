<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Day 4: Pub/Sub Deduplication Strategies](#day-4-pubsub-deduplication-strategies)
  - [Files](#files)
  - [Deduplication Patterns](#deduplication-patterns)
    - [Pattern 1: Eager Dedup (Pub/Sub Layer)](#pattern-1-eager-dedup-pubsub-layer)
    - [Pattern 2: Immutable Bronze (Append-Only)](#pattern-2-immutable-bronze-append-only)
    - [Pattern 3: Delta MERGE (Idempotent Upsert)](#pattern-3-delta-merge-idempotent-upsert)
  - [Benchmarks](#benchmarks)
  - [Usage](#usage)
    - [Generate test data](#generate-test-data)
    - [Run benchmark comparison](#run-benchmark-comparison)
  - [Design Decisions](#design-decisions)
    - [Why Bronze is the default](#why-bronze-is-the-default)
    - [Dedup key design](#dedup-key-design)
    - [Partitioning strategy](#partitioning-strategy)
  - [Architecture Evolution](#architecture-evolution)
  - [Technical Notes](#technical-notes)
    - [Hash function choice](#hash-function-choice)
    - [Polars vs Pandas](#polars-vs-pandas)
    - [Delta Lake mode selection](#delta-lake-mode-selection)
    - [Performance profiling](#performance-profiling)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Day 4: Pub/Sub Deduplication Strategies

Empirical comparison of deduplication patterns for event-driven systems. Tests eager dedup (at ingestion) vs immutable Bronze (dedup downstream) with real performance data.

## Files

- `day04_pubsub_sim.py` - Asyncio pub/sub simulator with configurable chaos (duplicates, out-of-order, missing messages)
- `dedup_strategies.py` - Three dedup implementations (eager, bronze, merge)
- `run_comparison.py` - Benchmark harness with performance instrumentation
- `performance_monitor.py` - Metrics capture (time, memory, CPU, disk, throughput)

---

## Deduplication Patterns

### Pattern 1: Eager Dedup (Pub/Sub Layer)

**Flow:** `Publisher → [Hash Set Filter] → Delta (Deduped)`

**Implementation:**
```python
# Track seen composite hashes, drop duplicates before write
seen = set()
for msg in stream:
    hash = SHA256(msg_id + seq_id + partition_id)
    if hash not in seen:
        write_to_delta(msg)
        seen.add(hash)
```

**Trade-offs:**
- Storage: Minimal (only unique records)
- Observability: None (dropped events invisible)
- Reprocessing: Impossible (data lost)
- Performance: 32k rec/s @ 10K scale

### Pattern 2: Immutable Bronze (Append-Only)

**Flow:** `Publisher → JSONL (all) → Bronze Delta (+hashes, partitioned) → Silver (deduped)`

**Implementation:**
```python
# Write everything, add metadata for downstream dedup
df = df.with_columns([
    composite_hash(msg_id, seq_id, partition_id),
    payload_hash(entire_record),
    partition_date(created_at)
])
write_deltalake(path, df, partition_by=["partition_date"])
```

**Trade-offs:**
- Storage: +62% (0.71MB overhead @ 10K, 993 dupes)
- Observability: Full (query all duplicates, out-of-order, anomalies)
- Reprocessing: Always possible (immutable source)
- Performance: 112k rec/s @ 10K scale (3.5x faster than eager!)

### Pattern 3: Delta MERGE (Idempotent Upsert)

**Flow:** `Publisher → Delta MERGE (message_id PK, created_at LWW) → Delta (Deduped)`

**Implementation:**
```python
# Last-write-wins by message_id
deduped = df.sort("created_at").unique(subset=["message_id"], keep="last")
write_deltalake(path, deduped, mode="overwrite")
```

**Trade-offs:**
- Storage: Minimal (deduped)
- Observability: None (duplicates merged away)
- Reprocessing: Possible if source preserved separately
- Performance: 16k-453k rec/s depending on impl (Polars 28x faster than Delta native)

---

## Benchmarks

**Test environment:**
- 10,000 messages
- ~10% duplicate rate (993 dupes)
- ~10% out-of-order messages
- 3 partitions

**Results:**

| Strategy      | Records | Time   | Memory | Disk   | Throughput   | Notes |
|---------------|---------|--------|--------|--------|--------------|-------|
| eager_dedup   | 9,999   | 0.337s | 145MB  | 1.15MB | 32k rec/s    | Baseline |
| bronze_append | 10,992  | 0.098s | 167MB  | 1.87MB | 112k rec/s   | **3.5x faster** |
| merge_polars  | 9,999   | 0.024s | 170MB  | 1.15MB | 453k rec/s   | 19x faster |
| merge_delta   | 10,097  | 0.660s | 238MB  | 1.15MB | 16k rec/s    | Slowest |

**Key findings:**
1. **Bronze is faster, not slower** - No dedup logic = straight-through processing
2. **Storage overhead is negligible** - 0.71MB absolute (+62% relative)
3. **Memory overhead is acceptable** - +15% (22MB)
4. **Polars outperforms native Delta MERGE** - 28x throughput difference

**Projected at 1M records:**
- Bronze storage overhead: ~72MB
- S3 cost: $0.002/month
- Engineering cost to debug 1 lost duplicate: $500-$5000
- **ROI: Instant**

---

## Usage

### Generate test data

```bash
# Scale presets: small=100, medium=10k, large=1M, xlarge=10M
python labs/day04_pubsub/day04_pubsub_sim.py --scale medium --seed 42 --quiet

# Custom scale
python labs/day04_pubsub/day04_pubsub_sim.py --messages 50000 --seed 42
```

**Outputs:** `data/raw/pubsub_sim_<timestamp>.jsonl`

### Run benchmark comparison

```bash
# Basic comparison
python labs/day04_pubsub/run_comparison.py

# With performance metrics (requires: pip install psutil)
python labs/day04_pubsub/run_comparison.py --benchmark

# Specify input
python labs/day04_pubsub/run_comparison.py --input data/raw/pubsub_sim_*.jsonl --benchmark
```

**Outputs:**
- `data/delta/pubsub_eager_dedup/` - Pattern 1
- `data/delta/pubsub_bronze_append/` - Pattern 2 (partitioned by date)
- `data/delta/pubsub_polars_merge/` - Pattern 3A
- `data/delta/pubsub_delta_merge/` - Pattern 3B
- Performance comparison table

---

## Design Decisions

### Why Bronze is the default

**Empirical evidence:**
- 3.5x faster than eager dedup (0.098s vs 0.337s)
- Storage overhead: ~$0.002/month per million duplicates
- Full observability retained
- Reprocessing always possible

**When to deviate:**
- Storage cost > engineering cost (extremely rare)
- Compliance prohibits storing duplicates
- Downstream systems can't handle dedup (fix downstream instead)

### Dedup key design

**Composite hash:** `SHA-256(message_id + sequence_id + partition_id)`
- Detects exact duplicate messages
- Natural key for deduplication

**Payload hash:** `SHA-256(entire JSON payload)`
- Detects content changes with same composite key
- Data quality monitoring (reused IDs, late corrections)

**Difference = data quality issue**

### Partitioning strategy

Bronze table partitioned by `partition_date` (derived from `created_at`):
```
data/delta/pubsub_bronze_append/
├── partition_date=2025-01-13/
│   └── *.parquet
└── partition_date=2025-01-14/
    └── *.parquet
```

**Benefits:**
- Query-time partition pruning
- Incremental processing (process new dates only)
- Time-travel queries
- Compliance (delete by date)

---

## Architecture Evolution

**Current (Day 4):**
```
Pub/Sub Sim → JSONL → Comparison (Bronze + 3 strategies)
```

**Target architecture:**
```
Real Pub/Sub → Prefect Flow → Polars → Delta Lake (Bronze → Silver → Gold)
                                ↓
                        Observability + Retry + DLQ
```

**Next steps:**
1. Wrap strategies in Prefect `@task` + `@flow`
2. Add failure recovery (DLQ, exponential backoff)
3. Implement Silver layer (Bronze → deduplicate → Silver)
4. Add monitoring (duplicate rate, latency, out-of-order %)

---

## Technical Notes

### Hash function choice

**SHA-256 vs MD5:**
- MD5 faster but collision-prone
- SHA-256 slower but cryptographically secure
- Use SHA-256 (collision risk unacceptable in dedup)

### Polars vs Pandas

Polars used throughout for:
- Lazy evaluation (process only needed columns)
- Arrow native (zero-copy Delta Lake integration)
- 5-10x faster than Pandas at scale
- Better memory efficiency

### Delta Lake mode selection

- `mode="append"` - Bronze (incremental, immutable)
- `mode="overwrite"` - Eager/Merge (replace all data)
- `partition_by` - Only works with append mode for incremental

### Performance profiling

Install `psutil` for detailed metrics:
```bash
pip install psutil
```

Without psutil:
- Time and throughput still captured
- Memory/CPU show as "N/A"

---

**Philosophy:** Immutable source → Metadata-enriched Bronze → Transform downstream. Storage is cheap. Debugging is expensive. Optimize for observability first, performance second.
