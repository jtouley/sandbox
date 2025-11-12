"""
Experiment 2: Partitioning Strategy

Partitioning trade-off:
- Parquet: User must manually manage partition folders; no unified metadata.
- Delta: Automatic partition tracking and pruning; metadata in _delta_log.

Cost: Delta adds _delta_log overhead.
Benefit: Automatic partition discovery and query optimization.
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


def _write_parquet_partitions(df, parquet_base):
    """Manually partition Parquet files by region."""
    console.print("\n[yellow]→ Parquet (manual partitioning)...[/yellow]")

    print_step("Manually partitioning by region...")
    start = time.perf_counter()
    for region in ["US-EAST", "US-WEST", "EU", "APAC"]:
        region_df = df.filter(pl.col("region") == region)
        region_path = parquet_base / f"region={region}" / "data.parquet"
        region_path.parent.mkdir(parents=True, exist_ok=True)
        region_df.write_parquet(region_path)
        console.print(f"    [dim]→ Wrote {len(region_df):,} rows to region={region}[/dim]")

    write_time = time.perf_counter() - start
    print_metric("Partition write time", format_time(write_time))

    files = list(parquet_base.glob("**/data.parquet"))
    print_metric("Files created", str(len(files)))

    size = sum(f.stat().st_size for f in parquet_base.rglob("*") if f.is_file())
    print_metric("Total size", format_size(size))

    return write_time, files, size


def _query_parquet_partition(parquet_base):
    """Query a single Parquet partition."""
    print_step("Querying single partition (US-EAST)...")
    start = time.perf_counter()
    us_east_files = list((parquet_base / "region=US-EAST").glob("*.parquet"))
    us_east_data = pl.concat([pl.read_parquet(f) for f in us_east_files])
    read_time = time.perf_counter() - start

    print_metric("Pruned read time", format_time(read_time))
    print_metric("Rows read", f"{len(us_east_data):,}")

    console.print("  [dim]Note: Manual path construction required for partition pruning[/dim]")

    return read_time


def _write_delta_partitions(df, delta_path):
    """Write Delta table with automatic partitioning."""
    console.print("\n[cyan]→ Delta (automatic partitioning)...[/cyan]")

    print_step("Writing Delta table with partition column...")
    start = time.perf_counter()
    write_deltalake(str(delta_path), df.to_arrow(), mode="overwrite", partition_by=["region"])
    write_time = time.perf_counter() - start

    dt = DeltaTable(str(delta_path))
    print_metric("Partition write time", format_time(write_time))

    files = list(delta_path.glob("**/*.parquet"))
    print_metric("Files created", str(len(files)))

    size = sum(f.stat().st_size for f in delta_path.rglob("*") if f.is_file())
    print_metric("Total size", format_size(size))

    return write_time, files, size, dt


def _query_delta_partition(dt):
    """Query a single Delta partition using filter pushdown."""
    print_step("Querying single partition (US-EAST) with filter...")
    start = time.perf_counter()
    delta_pruned = pl.from_arrow(dt.to_pyarrow_table(filters=[("region", "=", "US-EAST")]))
    read_time = time.perf_counter() - start

    print_metric("Pruned read time", format_time(read_time))
    print_metric("Rows read", f"{len(delta_pruned):,}")

    console.print("  [dim]Partition pruning handled automatically by Delta[/dim]")

    return read_time


def _print_comparison(parquet_metrics, delta_metrics):
    """Print partitioning comparison table and insights.

    Args:
        parquet_metrics: Tuple of (write_time, read_time, files, size)
        delta_metrics: Tuple of (write_time, read_time, files, size)
    """
    parquet_write, parquet_read, parquet_files, parquet_size = parquet_metrics
    delta_write, delta_read, delta_files, delta_size = delta_metrics
    console.print("\n[bold]Partitioning Comparison:[/bold]")
    comparison = Table()
    comparison.add_column("Metric", style="cyan")
    comparison.add_column("Parquet", style="yellow")
    comparison.add_column("Delta", style="blue")

    comparison.add_row("Write Time", format_time(parquet_write), format_time(delta_write))
    comparison.add_row("Pruned Read", format_time(parquet_read), format_time(delta_read))
    comparison.add_row("Files Created", str(len(parquet_files)), str(len(delta_files)))
    comparison.add_row("Storage Size", format_size(parquet_size), format_size(delta_size))
    comparison.add_row("Partition Logic", "❌ Manual path mgmt", "✅ Automatic via filter")
    comparison.add_row("Metadata", "❌ Scattered", "✅ Centralized _delta_log")

    console.print(comparison)

    console.print("\n[bold green]Key Insights:[/bold green]")
    console.print("  • Parquet: Manual partition management (path construction)")
    console.print("  • Delta: Automatic partition pruning via filter pushdown")
    console.print("  • Delta _delta_log tracks partition locations centrally")
    console.print("  • At scale, Delta's metadata makes partition management easier")


def run():
    print_section_header("Experiment 2: Partitioning Strategy")

    # Generate test data
    print_step("Generating test dataset (2M rows)...")
    df = generate_test_data(2_000_000)
    print_metric("Rows generated", f"{len(df):,}")
    print_metric("Partitions", "US-EAST, US-WEST, EU, APAC (by region)")

    # Define paths - Use environment-configured directories
    parquet_base = PROCESSED_DATA / "exp2_parquet_partitioned"
    delta_path = DELTA_DATA / "exp2_delta_partitioned"

    # Clean up
    if parquet_base.exists():
        shutil.rmtree(parquet_base)
    if delta_path.exists():
        shutil.rmtree(delta_path)

    parquet_base.mkdir(exist_ok=True)

    # Write and query Parquet partitions
    parquet_write, parquet_files, parquet_size = _write_parquet_partitions(df, parquet_base)
    parquet_read = _query_parquet_partition(parquet_base)

    # Write and query Delta partitions
    delta_write, delta_files, delta_size, dt = _write_delta_partitions(df, delta_path)
    delta_read = _query_delta_partition(dt)

    # Print comparison
    parquet_metrics = (parquet_write, parquet_read, parquet_files, parquet_size)
    delta_metrics = (delta_write, delta_read, delta_files, delta_size)
    _print_comparison(parquet_metrics, delta_metrics)


if __name__ == "__main__":
    run()
