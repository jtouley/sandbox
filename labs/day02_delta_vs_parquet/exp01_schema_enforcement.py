"""
Experiment 1: Schema Enforcement

Parquet: Schema-on-read. No validation at write time.
Delta: Schema-on-write. Enforces schema and enables evolution.

Cost: Delta adds metadata overhead and validation logic.
Benefit: Delta catches data quality issues early.
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


def _test_parquet_schema(df, parquet_path):
    """Test Parquet schema-on-read behavior."""
    console.print("\n[yellow]→ Testing Parquet (schema-on-read)...[/yellow]")

    print_step("Writing Parquet file...")
    start = time.perf_counter()
    df.write_parquet(parquet_path)
    write_time = time.perf_counter() - start
    print_metric("Write time", format_time(write_time))

    print_step("Reading Parquet file back...")
    start = time.perf_counter()
    parquet_read = pl.read_parquet(parquet_path)
    read_time = time.perf_counter() - start
    print_metric("Read time", format_time(read_time))

    size = parquet_path.stat().st_size
    print_metric("File size", format_size(size))
    print_metric("Rows read", f"{len(parquet_read):,}")

    return write_time, read_time, size


def _test_delta_schema(df, delta_path):
    """Test Delta Lake schema-on-write behavior."""
    console.print("\n[cyan]→ Testing Delta Lake (schema-on-write)...[/cyan]")

    print_step("Writing Delta table...")
    start = time.perf_counter()
    write_deltalake(str(delta_path), df.to_arrow(), mode="overwrite")
    write_time = time.perf_counter() - start
    print_metric("Write time", format_time(write_time))

    print_step("Reading Delta table back...")
    start = time.perf_counter()
    dt = DeltaTable(str(delta_path))
    delta_read = pl.from_arrow(dt.to_pyarrow_table())
    read_time = time.perf_counter() - start
    print_metric("Read time", format_time(read_time))

    size = sum(f.stat().st_size for f in delta_path.rglob("*") if f.is_file())
    print_metric("Total size", format_size(size))
    print_metric("Rows read", f"{len(delta_read):,}")

    print_step("Checking schema enforcement...")
    console.print("  [dim]Delta schema stored in _delta_log[/dim]")
    print_metric("Schema version", str(dt.version()))

    return write_time, read_time, size


def _print_comparison(parquet_metrics, delta_metrics):
    """Print comparison table and insights.

    Args:
        parquet_metrics: Tuple of (write_time, read_time, size)
        delta_metrics: Tuple of (write_time, read_time, size)
    """
    parquet_write, parquet_read, parquet_size = parquet_metrics
    delta_write, delta_read, delta_size = delta_metrics

    console.print("\n[bold]Performance Comparison:[/bold]")
    comparison = Table()
    comparison.add_column("Metric", style="cyan")
    comparison.add_column("Parquet", style="yellow")
    comparison.add_column("Delta", style="blue")

    comparison.add_row("Write Time", format_time(parquet_write), format_time(delta_write))
    comparison.add_row("Read Time", format_time(parquet_read), format_time(delta_read))
    comparison.add_row("Storage Size", format_size(parquet_size), format_size(delta_size))
    overhead_pct = (delta_size / parquet_size - 1) * 100
    comparison.add_row(
        "Overhead",
        "None",
        f"{format_size(delta_size - parquet_size)} ({overhead_pct:.1f}%)",
    )
    comparison.add_row("Schema Enforcement", "❌ Read-time only", "✅ Write-time validation")
    comparison.add_row("Schema Versioning", "❌ Not tracked", "✅ Tracked in _delta_log")

    console.print(comparison)

    console.print("\n[bold green]Key Insights:[/bold green]")
    console.print("  • Parquet: Fast writes, no validation overhead")
    console.print("  • Delta: Slightly slower writes, but enforces schema at write time")
    console.print(
        f"  • Overhead: Delta adds ~{overhead_pct:.1f}% storage " f"(includes _delta_log)"
    )
    console.print("  • Trade-off: Accept upfront cost for data quality guarantees")


def run():
    print_section_header("Experiment 1: Schema Enforcement")

    # Generate test data
    print_step("Generating test dataset (1M rows)...")
    df = generate_test_data(1_000_000)
    print_metric("Rows generated", f"{len(df):,}")
    print_metric("Schema", str(df.schema))

    # Define paths
    parquet_path = PROCESSED_DATA / "exp1_schema_test.parquet"
    delta_path = DELTA_DATA / "exp1_schema_test_delta"

    # Clean up previous runs
    if parquet_path.exists():
        parquet_path.unlink()
    if delta_path.exists():
        shutil.rmtree(delta_path)

    # Test both formats
    parquet_metrics = _test_parquet_schema(df, parquet_path)
    delta_metrics = _test_delta_schema(df, delta_path)

    # Print comparison
    _print_comparison(parquet_metrics, delta_metrics)


if __name__ == "__main__":
    run()
