"""
Experiment 3: Multi-file Aggregation

Realistic scenario: Query across many small files (common after incremental writes).

Parquet: Must read metadata from each file individually.
Delta: Centralized metadata in _delta_log; single-pass planning.

Cost: Delta has _delta_log overhead (grows with # of files).
Benefit: Faster query planning and potential skipping.
"""

import shutil
import time

import polars as pl
from deltalake import DeltaTable, write_deltalake
from rich.table import Table

from .config import (
    DELTA_DATA,
    PROCESSED_DATA,
    console,
    format_size,
    format_time,
    generate_test_data,
    print_metric,
    print_section_header,
    print_step,
)


def _write_parquet_batches(df, batch_base):
    """Write Parquet files as multiple batches."""
    console.print("\n[yellow]→ Parquet (simulating incremental writes)...[/yellow]")
    print_step("Writing 50 batch files...")

    start = time.perf_counter()
    batches = df.partition_by("user_segment", as_dict=True)

    file_count = 0
    for _segment, segment_df in batches.items():
        batch_size = len(segment_df) // 10
        for i in range(10):
            batch_df = segment_df.slice(i * batch_size, batch_size)
            batch_dir = batch_base / f"batch_{file_count:03d}"
            batch_dir.mkdir(parents=True, exist_ok=True)
            batch_df.write_parquet(batch_dir / "data.parquet")
            file_count += 1

    write_time = time.perf_counter() - start
    print_metric("Write time", format_time(write_time))

    batch_files = list(batch_base.glob("**/data.parquet"))
    print_metric("Files created", str(len(batch_files)))

    size = sum(f.stat().st_size for f in batch_base.rglob("*") if f.is_file())
    print_metric("Total size", format_size(size))

    return write_time, batch_files, size


def _aggregate_parquet_files(batch_files):
    """Aggregate across all Parquet files."""
    print_step("Aggregating across all files...")
    start = time.perf_counter()
    aggregated = (
        pl.concat([pl.read_parquet(f) for f in batch_files])
        .group_by("user_segment")
        .agg(pl.col("value").mean())
    )
    agg_time = time.perf_counter() - start

    print_metric("Aggregation time", format_time(agg_time))
    print_metric("Result rows", str(len(aggregated)))
    console.print("  [dim]Note: Must read metadata from each file individually[/dim]")

    return agg_time


def _write_delta_batches(df, delta_path):
    """Write Delta table as incremental appends."""
    console.print("\n[cyan]→ Delta (incremental appends)...[/cyan]")
    print_step("Writing initial Delta table...")

    start = time.perf_counter()
    batches = df.partition_by("user_segment", as_dict=True)
    first_batch = True

    for _segment, segment_df in batches.items():
        batch_size = len(segment_df) // 10
        for i in range(10):
            batch_df = segment_df.slice(i * batch_size, batch_size)
            mode = "overwrite" if first_batch else "append"
            write_deltalake(str(delta_path), batch_df.to_arrow(), mode=mode)
            first_batch = False

    write_time = time.perf_counter() - start

    dt = DeltaTable(str(delta_path))
    print_metric("Write time", format_time(write_time))

    delta_files = list(delta_path.glob("**/*.parquet"))
    print_metric("Files created", str(len(delta_files)))

    size = sum(f.stat().st_size for f in delta_path.rglob("*") if f.is_file())
    print_metric("Total size", format_size(size))
    print_metric("Delta version", str(dt.version()))

    return write_time, delta_files, size, dt


def _aggregate_delta_table(dt):
    """Aggregate using Delta table."""
    print_step("Aggregating using Delta table...")
    start = time.perf_counter()
    delta_agg = (
        pl.from_arrow(dt.to_pyarrow_table()).group_by("user_segment").agg(pl.col("value").mean())
    )
    agg_time = time.perf_counter() - start

    print_metric("Aggregation time", format_time(agg_time))
    print_metric("Result rows", str(len(delta_agg)))
    console.print("  [dim]Metadata centralized in _delta_log (single read)[/dim]")

    return agg_time


def _print_comparison(parquet_metrics, delta_metrics):
    """Print comparison table and insights.

    Args:
        parquet_metrics: Tuple of (write_time, agg_time, files, size)
        delta_metrics: Tuple of (write_time, agg_time, files, size)
    """
    parquet_write, parquet_agg, batch_files, parquet_size = parquet_metrics
    delta_write, delta_agg, delta_files, delta_size = delta_metrics
    console.print("\n[bold]Multi-file Aggregation Comparison:[/bold]")
    comparison = Table()
    comparison.add_column("Metric", style="cyan")
    comparison.add_column("Parquet", style="yellow")
    comparison.add_column("Delta", style="blue")

    comparison.add_row("Write Time", format_time(parquet_write), format_time(delta_write))
    comparison.add_row("Aggregation Time", format_time(parquet_agg), format_time(delta_agg))
    comparison.add_row("Files Created", str(len(batch_files)), str(len(delta_files)))
    comparison.add_row("Storage Size", format_size(parquet_size), format_size(delta_size))
    comparison.add_row("Metadata Reads", f"{len(batch_files)} files", "1 _delta_log")
    comparison.add_row("Query Planning", "❌ Per-file scan", "✅ Centralized")

    console.print(comparison)

    console.print("\n[bold green]Key Insights:[/bold green]")
    console.print(f"  • Parquet: Scanned {len(batch_files)} files individually")
    console.print("  • Delta: Single metadata read from _delta_log")
    console.print("  • Aggregation performance similar (dominated by data scan)")
    console.print("  • Delta advantage grows with file count (100s-1000s of files)")


def run():
    print_section_header("Experiment 3: Multi-file Aggregation")

    # Generate test data
    print_step("Generating test dataset (1M rows)...")
    df = generate_test_data(1_000_000)
    print_metric("Rows generated", f"{len(df):,}")

    # Define paths - Use environment-configured directories
    batch_base = PROCESSED_DATA / "exp3_batch_data"
    delta_path = DELTA_DATA / "exp3_delta_batches"

    # Clean up
    if batch_base.exists():
        shutil.rmtree(batch_base)
    if delta_path.exists():
        shutil.rmtree(delta_path)

    batch_base.mkdir(exist_ok=True)

    # Write and aggregate Parquet
    parquet_write, batch_files, parquet_size = _write_parquet_batches(df, batch_base)
    parquet_agg = _aggregate_parquet_files(batch_files)

    # Write and aggregate Delta
    delta_write, delta_files, delta_size, dt = _write_delta_batches(df, delta_path)
    delta_agg = _aggregate_delta_table(dt)

    # Print comparison
    parquet_metrics = (parquet_write, parquet_agg, batch_files, parquet_size)
    delta_metrics = (delta_write, delta_agg, delta_files, delta_size)
    _print_comparison(parquet_metrics, delta_metrics)


if __name__ == "__main__":
    run()
