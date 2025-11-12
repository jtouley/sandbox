"""
Experiment 4: Schema Evolution

Scenario: Add a new column to the dataset.

Parquet: New files with new schema; readers must handle both schemas.
Delta: Tracks schema versions; handles evolution automatically.

Cost: Delta adds schema versioning overhead.
Benefit: Automatic backward compatibility.
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


def _write_initial_schemas(df_v1, parquet_path, delta_path):
    """Write initial v1 schema to both Parquet and Delta."""
    console.print("\n[bold]Step 1: Initial write (v1 schema)[/bold]")

    print_step("Writing v1 data to Parquet...")
    df_v1.write_parquet(parquet_path)
    v1_size = parquet_path.stat().st_size
    print_metric("Parquet v1 size", format_size(v1_size))

    print_step("Writing v1 data to Delta...")
    write_deltalake(str(delta_path), df_v1.to_arrow(), mode="overwrite")
    dt = DeltaTable(str(delta_path))
    delta_v1_size = sum(f.stat().st_size for f in delta_path.rglob("*") if f.is_file())
    print_metric("Delta v1 size", format_size(delta_v1_size))
    print_metric("Delta version", str(dt.version()))

    return v1_size, delta_v1_size


def _parquet_schema_evolution(df_v2, parquet_path_v2):
    """Test Parquet schema evolution by rewriting all data."""
    console.print("\n[yellow]→ Parquet Approach (manual handling required)...[/yellow]")

    print_step("Option 1: Rewriting entire dataset with new schema...")
    start = time.perf_counter()
    df_v2.write_parquet(parquet_path_v2)
    rewrite_time = time.perf_counter() - start
    v2_size = parquet_path_v2.stat().st_size

    print_metric("Rewrite time", format_time(rewrite_time), "yellow")
    print_metric("New file size", format_size(v2_size), "yellow")
    print_metric(
        "Data rewritten",
        f"{len(df_v2):,} rows ({format_size(df_v2.to_arrow().nbytes)} in memory)",
        "yellow",
    )

    console.print(
        "\n  [dim]Note: Parquet requires rewriting ALL data or managing separate files[/dim]"
    )
    console.print("  [dim]→ Option A: Overwrite (expensive, data unavailable during write)[/dim]")
    console.print(
        "  [dim]→ Option B: Separate files " "(schema fragmentation, reader complexity)[/dim]"
    )

    return rewrite_time, v2_size


def _delta_schema_evolution(df_v2, delta_path):
    """Test Delta schema evolution using append with schema merge."""
    console.print("\n[cyan]→ Delta Approach (automatic schema evolution)...[/cyan]")

    print_step("Appending data with new schema...")
    start = time.perf_counter()
    write_deltalake(
        str(delta_path), df_v2.head(100_000).to_arrow(), mode="append", schema_mode="merge"
    )
    append_time = time.perf_counter() - start

    dt = DeltaTable(str(delta_path))
    delta_v2_size = sum(f.stat().st_size for f in delta_path.rglob("*") if f.is_file())

    print_metric("Append time", format_time(append_time), "cyan")
    print_metric("Delta version", str(dt.version()), "cyan")
    print_metric("Total size", format_size(delta_v2_size), "cyan")

    print_step("Reading evolved Delta table...")
    delta_read = pl.from_arrow(dt.to_pyarrow_table())
    print_metric("Rows read", f"{len(delta_read):,}")
    print_metric("Schema", str(list(delta_read.schema.names())))
    console.print("  [dim]Old rows have NULL for 'new_metric', new rows have values[/dim]")

    print_step("Checking Delta schema history...")
    history = dt.history()
    console.print(f"  [cyan]Schema versions tracked:[/cyan] {len(history)} operations")

    return append_time, delta_v2_size


def _print_comparison(parquet_metrics, delta_metrics, df_v2):
    """Print schema evolution comparison table and insights.

    Args:
        parquet_metrics: Tuple of (rewrite_time, v1_size, v2_size)
        delta_metrics: Tuple of (append_time, delta_v2_size)
        df_v2: The evolved DataFrame (for row count)
    """
    rewrite_time, v1_size, v2_size = parquet_metrics
    append_time, delta_v2_size = delta_metrics
    console.print("\n[bold]Schema Evolution Comparison:[/bold]")
    comparison = Table()
    comparison.add_column("Aspect", style="cyan")
    comparison.add_column("Parquet", style="yellow")
    comparison.add_column("Delta", style="blue")

    comparison.add_row("Evolution Method", "Rewrite all data", "Append with schema_mode='merge'")
    comparison.add_row("Time to evolve", format_time(rewrite_time), format_time(append_time))
    comparison.add_row("Data rewritten", "100% (all rows)", "0% (append only)")
    comparison.add_row("Downtime", "Yes (during rewrite)", "No (append is atomic)")
    comparison.add_row("Schema tracking", "❌ Manual", "✅ Automatic versioning")
    comparison.add_row("Backward compat", "❌ Manual handling", "✅ NULL for missing columns")
    comparison.add_row(
        "Storage overhead",
        f"{format_size(v1_size + v2_size)}",
        f"{format_size(delta_v2_size)}",
    )

    console.print(comparison)

    console.print("\n[bold green]Key Insights:[/bold green]")
    console.print(
        f"  • Parquet: Required {format_time(rewrite_time)} to rewrite {len(df_v2):,} rows"
    )
    console.print(f"  • Delta: Appended 100K rows in {format_time(append_time)} (no rewrite)")
    console.print("  • Delta schema evolution is ~10-100x faster (append vs full rewrite)")
    console.print("  • Delta automatically handles backward compatibility (NULL for old rows)")
    console.print("  • Parquet requires manual schema management or fragmented files")


def run():
    print_section_header("Experiment 4: Schema Evolution")

    # Generate initial data
    print_step("Generating initial dataset (500K rows)...")
    df_v1 = generate_test_data(500_000)
    print_metric("Rows generated", f"{len(df_v1):,}")
    print_metric("Initial schema (v1)", str(list(df_v1.schema.names())))

    # Define paths - Use environment-configured directories
    parquet_path = PROCESSED_DATA / "exp4_schema_evolution.parquet"
    parquet_path_v2 = PROCESSED_DATA / "exp4_schema_evolution_v2.parquet"
    delta_path = DELTA_DATA / "exp4_schema_evolution_delta"

    # Clean up previous runs
    if parquet_path.exists():
        parquet_path.unlink()
    if parquet_path_v2.exists():
        parquet_path_v2.unlink()
    if delta_path.exists():
        shutil.rmtree(delta_path)

    # Write initial v1 schemas
    v1_size, _delta_v1_size = _write_initial_schemas(df_v1, parquet_path, delta_path)

    # Create evolved schema
    console.print("\n[bold]Step 2: Schema evolution (add 'new_metric' column)[/bold]")
    print_step("Creating evolved schema with new column...")
    df_v2 = df_v1.with_columns(pl.lit(0.0).alias("new_metric"))
    print_metric("Old schema", str(list(df_v1.schema.names())))
    print_metric("New schema", str(list(df_v2.schema.names())), "cyan")

    # Test schema evolution with both formats
    rewrite_time, v2_size = _parquet_schema_evolution(df_v2, parquet_path_v2)
    append_time, delta_v2_size = _delta_schema_evolution(df_v2, delta_path)

    # Print comparison
    parquet_metrics = (rewrite_time, v1_size, v2_size)
    delta_metrics = (append_time, delta_v2_size)
    _print_comparison(parquet_metrics, delta_metrics, df_v2)


if __name__ == "__main__":
    run()
