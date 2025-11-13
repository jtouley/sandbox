<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Day 02 – Delta vs Parquet](#day-02--delta-vs-parquet)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Day 02 – Delta vs Parquet

**Goal**
Build intuition around Parquet vs Delta Lake trade-offs through hands-on experiments. Not about "which is better"—but understanding the **cost structure** of each approach and when each constraint matters.

**Read / Watch**
- [Delta Lake Python Docs](https://delta-io.github.io/delta-rs/python/)
- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Parquet Format Spec](https://parquet.apache.org/docs/)

**Lab / Code**
Refactored from monolithic script to modular experiments:

```bash
# Run all experiments
python -m labs.day02_delta_vs_parquet.run_all

# Run specific experiments
python -m labs.day02_delta_vs_parquet.run_all --experiments 1,4

# Individual experiment
python -m labs.day02_delta_vs_parquet.exp04_schema_evolution
```

**Structure:**
- `exp01_schema_enforcement.py` - Schema-on-read vs schema-on-write
- `exp02_partitioning.py` - Manual vs automatic partitioning
- `exp03_multi_file_aggregation.py` - Multi-file query performance
- `exp04_schema_evolution.py` - Adding columns (rewrite vs append)
- `exp05_compression.py` - Codec trade-offs (snappy, gzip, uncompressed)

**Reflection**

**Key Learnings:**
1. **Schema Evolution**: Delta's `schema_mode='merge'` is ~10-100x faster than Parquet's full rewrite approach
2. **Partition Management**: Delta's _delta_log centralizes metadata; Parquet requires manual path construction
3. **Storage Overhead**: Delta adds ~2-5% for transaction logs, but gains data quality guarantees
4. **Compression Sweet Spot**: Snappy balances compression ratio (~40-50%) with read/write speed
