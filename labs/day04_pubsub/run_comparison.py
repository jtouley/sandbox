#!/usr/bin/env python
"""
Run deduplication strategy comparison.

Loads JSONL from pubsub simulation and applies different dedup strategies,
writing results to separate Delta tables for comparison.

Run:
    uv run python labs/day04_pubsub/run_comparison.py \
        --input data/raw/pubsub_sim_v2_*.jsonl
    uv run python labs/day04_pubsub/run_comparison.py \
        --input data/raw/pubsub_sim_v2_*.jsonl --strategies bronze,merge
"""

import argparse
import json
import os
import time
from pathlib import Path

import polars as pl
from dedup_strategies import (
    add_hashes,
    eager_dedup,
    merge_dedup_delta,
    merge_dedup_polars,
)
from deltalake import write_deltalake
from performance_monitor import (
    monitor_performance,
    print_performance_table,
    warn_missing_psutil,
)

# Data paths from environment
RAW_DATA_PATH = Path(os.environ["RAW_DATA_PATH"])
DELTA_TABLE_PATH = Path(os.environ["DELTA_TABLE_PATH"])

# Scale presets
SCALE_PRESETS = {
    "small": 100,
    "medium": 10_000,
    "large": 1_000_000,
    "xlarge": 10_000_000,
}


def load_jsonl(file_path: Path) -> pl.DataFrame:
    """Load JSONL file into Polars DataFrame."""
    with open(file_path) as f:
        records = [json.loads(line) for line in f]
    return pl.DataFrame(records)


def find_latest_jsonl(pattern: str = "pubsub_sim_v2_*.jsonl") -> Path:
    """Find the most recent JSONL file matching pattern."""
    files = sorted(RAW_DATA_PATH.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found matching {pattern} in {RAW_DATA_PATH}")
    return files[-1]


def print_metrics(metrics: dict, strategy_name: str) -> None:
    """Pretty print strategy metrics."""
    print(f"\n{'=' * 60}")
    print(f"Strategy: {strategy_name}")
    print(f"{'=' * 60}")
    for key, value in metrics.items():
        if key != "strategy":
            print(f"  {key:30s}: {value}")


def run_all_strategies(df: pl.DataFrame) -> dict:
    """
    Run all dedup strategies and return results with performance metrics.

    Returns dict with structure:
        {
            "eager": {
                "df": DataFrame, "metrics": dict,
                "table_path": Path, "perf": PerformanceMetrics
            },
            "bronze": {
                "df": DataFrame, "metrics": dict,
                "table_path": Path, "perf": PerformanceMetrics
            },
            "merge_polars": {
                "df": DataFrame, "metrics": dict,
                "table_path": Path, "perf": PerformanceMetrics
            },
            "merge_delta": {
                "df": DataFrame, "metrics": dict,
                "table_path": Path, "perf": PerformanceMetrics
            },
        }
    """
    results = {}
    record_count = len(df)

    # Strategy 1: Eager dedup (pub/sub layer)
    print("\n[1/4] Running eager dedup strategy...")
    eager_path = DELTA_TABLE_PATH / "pubsub_eager_dedup"
    with monitor_performance("eager_dedup", record_count, eager_path) as perf:
        eager_df, eager_metrics = eager_dedup(df.clone())
        write_deltalake(
            str(eager_path),
            eager_df.to_arrow(),
            mode="overwrite",
        )
    results["eager"] = {
        "df": eager_df,
        "metrics": eager_metrics,
        "table_path": eager_path,
        "perf": perf,
    }
    print_metrics(eager_metrics, "Eager Dedup (Pub/Sub Layer)")

    # Strategy 2: Append-only Bronze (add hashes, no dedup)
    print("\n[2/4] Running bronze append-only strategy...")
    bronze_path = DELTA_TABLE_PATH / "pubsub_bronze_append"
    with monitor_performance("bronze_append", record_count, bronze_path) as perf:
        bronze_df, bronze_metrics = add_hashes(df.clone())
        write_deltalake(
            str(bronze_path),
            bronze_df.to_arrow(),
            mode="append",
            partition_by=["partition_date"],
        )
    results["bronze"] = {
        "df": bronze_df,
        "metrics": bronze_metrics,
        "table_path": bronze_path,
        "perf": perf,
    }
    print_metrics(bronze_metrics, "Bronze Append-Only (Immutable)")

    # Strategy 3A: Merge dedup with Polars
    print("\n[3/4] Running Polars merge dedup strategy...")
    merge_polars_path = DELTA_TABLE_PATH / "pubsub_polars_merge"
    with monitor_performance("merge_polars", record_count, merge_polars_path) as perf:
        merge_polars_df, merge_polars_metrics = merge_dedup_polars(df.clone(), merge_polars_path)
        write_deltalake(
            str(merge_polars_path),
            merge_polars_df.to_arrow(),
            mode="overwrite",
        )
    results["merge_polars"] = {
        "df": merge_polars_df,
        "metrics": merge_polars_metrics,
        "table_path": merge_polars_path,
        "perf": perf,
    }
    print_metrics(merge_polars_metrics, "Merge Dedup (Polars)")

    # Strategy 3B: Merge dedup with Delta native
    print("\n[4/4] Running Delta merge dedup strategy...")
    merge_delta_path = DELTA_TABLE_PATH / "pubsub_delta_merge"
    with monitor_performance("merge_delta", record_count, merge_delta_path) as perf:
        merge_delta_df, merge_delta_metrics = merge_dedup_delta(df.clone(), merge_delta_path)
    results["merge_delta"] = {
        "df": merge_delta_df,
        "metrics": merge_delta_metrics,
        "table_path": merge_delta_path,
        "perf": perf,
    }
    print_metrics(merge_delta_metrics, "Merge Dedup (Delta Lake)")

    return results


def print_comparison_summary(results: dict, original_count: int) -> None:
    """Print final comparison table."""
    print("\n" + "=" * 80)
    print("COMPARISON SUMMARY")
    print("=" * 80)
    print(f"\n{'Strategy':<25} {'Records':<12} {'vs Original':<15} {'Storage Path':<30}")
    print("-" * 80)

    for name, data in results.items():
        record_count = len(data["df"])
        vs_original = f"{(record_count / original_count * 100):.1f}%"
        table_name = data["table_path"].name
        print(f"{name:<25} {record_count:<12} {vs_original:<15} {table_name:<30}")

    print("\n" + "=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)

    # Find unique record counts
    unique_counts = set(len(data["df"]) for data in results.values())

    if len(unique_counts) == 1:
        print("✓ All strategies produced the same record count (correctness verified)")
    else:
        print("⚠ Different strategies produced different record counts:")
        for name, data in results.items():
            count = len(data["df"])
            if name == "bronze":
                print(f"  - {name}: {count} records (expected - no dedup)")
            else:
                print(f"  - {name}: {count} records")

    # Bronze observability advantage
    bronze_count = len(results["bronze"]["df"])
    deduped_count = len(results["eager"]["df"])
    lost_records = bronze_count - deduped_count

    print(f"\n✓ Bronze pattern preserves {lost_records} duplicate events for observability")
    print("✓ Downstream can query duplicates, out-of-order events, and anomalies")
    print(f"✓ Trade-off: {(bronze_count / deduped_count):.2f}x storage vs eager dedup")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Compare deduplication strategies with performance benchmarking"
    )
    parser.add_argument(
        "--input",
        type=str,
        help="Path to JSONL file (glob pattern supported)",
    )
    parser.add_argument(
        "--strategies",
        type=str,
        default="all",
        help="Comma-separated list: eager,bronze,merge (default: all)",
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Enable detailed performance benchmarking",
    )
    args = parser.parse_args()

    # Warn if psutil not available
    warn_missing_psutil()

    # Find input file
    if args.input:
        input_path = Path(args.input)
        if "*" in str(input_path):
            # Handle glob pattern
            input_path = find_latest_jsonl(input_path.name)
    else:
        # Auto-detect latest v2 simulation
        input_path = find_latest_jsonl()

    print(f"\n{'=' * 80}")
    print("DEDUPLICATION STRATEGY COMPARISON" + (" + BENCHMARKING" if args.benchmark else ""))
    print(f"{'=' * 80}")
    print(f"Input file: {input_path}")

    # Load data
    print("\nLoading JSONL data...")
    start_time = time.time()
    df = load_jsonl(input_path)
    load_time = time.time() - start_time
    print(f"✓ Loaded {len(df)} records in {load_time:.3f}s")

    # Run strategies
    print("\nRunning deduplication strategies...")
    results = run_all_strategies(df)

    # Print comparison
    print_comparison_summary(results, len(df))

    # Print performance benchmarking if enabled
    if args.benchmark:
        perf_metrics = [data["perf"] for data in results.values()]
        print_performance_table(perf_metrics)

    print(f"\n✓ All Delta tables written to: {DELTA_TABLE_PATH}/")
    print("✓ Comparison complete!\n")


if __name__ == "__main__":
    main()
