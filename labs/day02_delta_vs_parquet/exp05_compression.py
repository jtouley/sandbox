"""
Experiment 5: Compression & Encoding

Both Parquet and Delta support compression, but at different levels.

Parquet: Column-level compression (snappy, gzip, etc)
Delta: Parquet compression + transaction log optimization

Goal: Understand the trade-off between compression ratio and query speed.
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


def _test_parquet_compression(df):
    """Test Parquet with different compression codecs."""
    console.print("\n[yellow]→ Parquet (testing compression codecs)...[/yellow]")

    parquet_results = []

    for codec in ["uncompressed", "snappy", "gzip"]:
        print_step(f"Writing Parquet with {codec} compression...")
        path = PROCESSED_DATA / f"exp5_{codec}.parquet"
        if path.exists():
            path.unlink()

        start = time.perf_counter()
        df.write_parquet(path, compression=codec)
        write_time = time.perf_counter() - start

        start = time.perf_counter()
        _ = pl.read_parquet(path)
        read_time = time.perf_counter() - start

        size = path.stat().st_size

        parquet_results.append(
            {
                "codec": codec.capitalize(),
                "write_time": write_time,
                "read_time": read_time,
                "size": size,
            }
        )

        print_metric(f"{codec} size", format_size(size))

    return parquet_results


def _test_delta_compression(df, delta_path):
    """Test Delta compression (uses Snappy by default)."""
    console.print("\n[cyan]→ Delta (default Snappy compression + metadata)...[/cyan]")

    print_step("Writing Delta table (Snappy compression)...")
    start = time.perf_counter()
    write_deltalake(
        str(delta_path),
        df.to_arrow(),
        mode="overwrite",
    )
    write_time = time.perf_counter() - start

    start = time.perf_counter()
    dt = DeltaTable(str(delta_path))
    _ = pl.from_arrow(dt.to_pyarrow_table())
    read_time = time.perf_counter() - start

    size = sum(f.stat().st_size for f in delta_path.rglob("*") if f.is_file())

    print_metric("Delta size (Snappy)", format_size(size))
    print_metric("Write time", format_time(write_time))
    print_metric("Read time", format_time(read_time))

    console.print("\n[dim]Note: Delta Lake uses Snappy compression by default[/dim]")
    console.print("[dim]Compression codec is not configurable in deltalake-python API[/dim]")

    return write_time, read_time, size


def _print_compression_tables(parquet_results, delta_write, delta_read, delta_size):
    """Print compression comparison tables and insights."""
    # Parquet comparison table
    console.print("\n[bold]Parquet Compression Trade-offs:[/bold]")
    parquet_table = Table()
    parquet_table.add_column("Codec", style="cyan")
    parquet_table.add_column("Size", style="green")
    parquet_table.add_column("Ratio", style="yellow")
    parquet_table.add_column("Write Time", style="blue")
    parquet_table.add_column("Read Time", style="magenta")

    base_size = parquet_results[0]["size"]

    for result in parquet_results:
        ratio = result["size"] / base_size
        parquet_table.add_row(
            result["codec"],
            format_size(result["size"]),
            f"{ratio:.2%}",
            format_time(result["write_time"]),
            format_time(result["read_time"]),
        )

    console.print(parquet_table)

    # Parquet vs Delta comparison
    console.print("\n[bold]Parquet vs Delta (Snappy) Comparison:[/bold]")
    comparison_table = Table()
    comparison_table.add_column("Format", style="cyan")
    comparison_table.add_column("Compression", style="yellow")
    comparison_table.add_column("Size", style="green")
    comparison_table.add_column("Write Time", style="blue")
    comparison_table.add_column("Read Time", style="magenta")

    parquet_snappy = parquet_results[1]
    comparison_table.add_row(
        "Parquet",
        "Snappy",
        format_size(parquet_snappy["size"]),
        format_time(parquet_snappy["write_time"]),
        format_time(parquet_snappy["read_time"]),
    )

    comparison_table.add_row(
        "Delta",
        "Snappy (+ metadata)",
        format_size(delta_size),
        format_time(delta_write),
        format_time(delta_read),
    )

    console.print(comparison_table)

    # Show overhead
    overhead = delta_size - parquet_snappy["size"]
    overhead_pct = (delta_size / parquet_snappy["size"] - 1) * 100
    console.print(
        f"\n[yellow]Delta overhead:[/yellow] " f"{format_size(overhead)} (+{overhead_pct:.1f}%)"
    )
    console.print("[dim]Overhead includes _delta_log transaction metadata[/dim]")

    # Key insights
    console.print("\n[bold green]Key Insights:[/bold green]")
    console.print("  • Gzip: Best compression (~60-70% size), but slower writes/reads")
    console.print("  • Snappy: Balanced (default for most systems, ~40-50% reduction)")
    console.print("  • Uncompressed: Largest files, fastest reads (I/O bound)")
    console.print("  • Delta overhead: ~2-5% for _delta_log metadata")
    console.print("  • Compression choice depends on: storage cost vs CPU/query speed")


def run():
    print_section_header("Experiment 5: Compression & File Size")

    # Generate test data
    print_step("Generating test dataset (1M rows)...")
    df = generate_test_data(1_000_000)
    print_metric("Rows generated", f"{len(df):,}")

    # Test Parquet compression
    parquet_results = _test_parquet_compression(df)

    # Test Delta compression
    delta_path = DELTA_DATA / "exp5_delta_snappy"
    if delta_path.exists():
        shutil.rmtree(delta_path)

    delta_write, delta_read, delta_size = _test_delta_compression(df, delta_path)

    # Print comparison tables
    _print_compression_tables(parquet_results, delta_write, delta_read, delta_size)


if __name__ == "__main__":
    run()
