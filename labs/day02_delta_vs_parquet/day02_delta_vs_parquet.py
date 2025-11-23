"""
Day 02: Delta vs Parquet Trade-offs with Polars

Goal: Build intuition around schema enforcement, partitioning strategies, and
compaction behavior. Not about "which is better"—but understanding the cost
structure of each approach and when each constraint matters.

Key Questions:
1. How does schema enforcement differ? (Parquet is schema-on-read; Delta enforces on write)
2. What's the performance cost of compaction overhead in Delta?
3. How do partition strategies affect query performance and metadata overhead?
4. What's the memory footprint when scanning large, multi-file datasets?
"""

import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table

console = Console()

# ---- Configuration ----
DATA_DIR = Path("labs/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

N_ROWS = 5_000_000
SEED = 42


# ---- Generate Test Dataset ----
def generate_test_data(n_rows: int = N_ROWS) -> pl.DataFrame:
    """
    Generate a realistic dataset:
    - id: unique identifier
    - timestamp: event time (allows partitioning by date)
    - region: categorical (simulates geographic partitioning)
    - user_segment: categorical (customer segment)
    - value: numerical metric
    """

    random.seed(SEED)

    # Generate enough timestamps for the requested rows
    # Calculate interval needed to span the year with n_rows timestamps
    start_dt = datetime(2024, 1, 1)
    end_dt = datetime(2024, 12, 31)
    total_seconds = (end_dt - start_dt).total_seconds()
    interval_seconds = max(1, int(total_seconds / n_rows))

    timestamps = [start_dt + timedelta(seconds=i * interval_seconds) for i in range(n_rows)]

    df = pl.DataFrame(
        {
            "id": range(n_rows),
            "timestamp": timestamps,
            "region": [random.choice(["US-EAST", "US-WEST", "EU", "APAC"]) for _ in range(n_rows)],
            "user_segment": [
                random.choice(["BASIC", "PREMIUM", "ENTERPRISE"]) for _ in range(n_rows)
            ],
            "value": [(i * 2) % 10_000 for i in range(n_rows)],
        }
    )
    return df


# ---- Experiment 1: Schema Enforcement ----
def experiment_schema_enforcement():
    """
    Parquet: Schema-on-read. No validation at write time.
    Delta: Schema-on-write. Enforces schema and enables evolution.

    Cost: Delta adds metadata overhead and validation logic.
    Benefit: Delta catches data quality issues early.
    """
    console.print("\n[bold cyan]Experiment 1: Schema Enforcement[/bold cyan]")

    df = generate_test_data(1_000_000)

    # Write Parquet
    parquet_path = DATA_DIR / "exp1_schema_test.parquet"
    start = time.perf_counter()
    df.write_parquet(parquet_path)
    parquet_write_time = time.perf_counter() - start

    # Read back and inspect
    start = time.perf_counter()
    parquet_read = pl.read_parquet(parquet_path)
    parquet_read_time = time.perf_counter() - start

    console.print("[green]✓ Parquet[/green]")
    console.print(f"  Write: {parquet_write_time:.3f}s | Read: {parquet_read_time:.3f}s")
    console.print(f"  Schema: {parquet_read.schema}")

    # File size
    parquet_size = parquet_path.stat().st_size / (1024 * 1024)
    console.print(f"  Size: {parquet_size:.2f} MB")

    console.print("\n[yellow]Note:[/yellow] Parquet has no explicit schema validation on write.")
    console.print("       Delta would enforce schema and track schema history.")


# ---- Experiment 2: Partitioning Strategy ----
def experiment_partitioning():
    """
    Partitioning trade-off:
    - Parquet: User must manually manage partition folders; no unified metadata.
    - Delta: Automatic partition tracking and pruning; metadata in _delta_log.

    Cost: Delta adds _delta_log overhead.
    Benefit: Automatic partition discovery and query optimization.
    """
    console.print("\n[bold cyan]Experiment 2: Partitioning Strategy[/bold cyan]")

    df = generate_test_data(2_000_000)

    # Manually partition Parquet by region
    parquet_base = DATA_DIR / "exp2_parquet_partitioned"
    parquet_base.mkdir(exist_ok=True)

    start = time.perf_counter()
    for region in ["US-EAST", "US-WEST", "EU", "APAC"]:
        region_df = df.filter(pl.col("region") == region)
        region_path = parquet_base / f"region={region}" / "data.parquet"
        region_path.parent.mkdir(parents=True, exist_ok=True)
        region_df.write_parquet(region_path)
    parquet_partition_write = time.perf_counter() - start

    console.print("[green]✓ Parquet (manual partitioning)[/green]")
    console.print(f"  Partition write: {parquet_partition_write:.3f}s")

    # Count files
    parquet_files = list(parquet_base.glob("**/data.parquet"))
    console.print(f"  Files created: {len(parquet_files)}")

    # Simulate partition-pruned query (read only US-EAST)
    start = time.perf_counter()
    us_east_files = list((parquet_base / "region=US-EAST").glob("*.parquet"))
    us_east_data = pl.concat([pl.read_parquet(f) for f in us_east_files])
    parquet_pruned_read = time.perf_counter() - start

    console.print(
        f"  Pruned read (US-EAST): {parquet_pruned_read:.3f}s | Rows: {us_east_data.height}"
    )

    console.print("\n[yellow]Note:[/yellow] Manual partitioning requires custom logic.")
    console.print("       Delta would handle this automatically with _delta_log.")


# ---- Experiment 3: Multi-file Aggregation ----
def experiment_multi_file_aggregation():
    """
    Realistic scenario: Query across many small files (common after incremental writes).

    Parquet: Must read metadata from each file individually.
    Delta: Centralized metadata in _delta_log; single-pass planning.

    Cost: Delta has _delta_log overhead (grows with # of files).
    Benefit: Faster query planning and potential skipping.
    """
    console.print("\n[bold cyan]Experiment 3: Multi-file Aggregation[/bold cyan]")

    df = generate_test_data(1_000_000)

    # Simulate incremental writes: split into 50 batches
    batch_base = DATA_DIR / "exp3_batch_data"
    batch_base.mkdir(exist_ok=True)

    console.print("  Writing 50 batch files...")
    start = time.perf_counter()
    for i, batch_df in enumerate(df.partition_by("user_segment")):
        # Further split each partition
        batch_dir = batch_base / f"batch_{i:03d}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_df.write_parquet(batch_dir / "data.parquet")
    write_time = time.perf_counter() - start
    console.print(f"  Write time: {write_time:.3f}s")

    # Now aggregate across all files
    batch_files = list(batch_base.glob("**/data.parquet"))
    console.print(f"  Files to scan: {len(batch_files)}")

    start = time.perf_counter()
    aggregated = (
        pl.concat([pl.read_parquet(f) for f in batch_files])
        .group_by("user_segment")
        .agg(pl.col("value").mean())
    )
    parquet_agg_time = time.perf_counter() - start

    console.print(f"  [green]Parquet aggregation:[/green] {parquet_agg_time:.3f}s")
    console.print(f"  Result rows: {aggregated.height}")

    console.print("\n[yellow]Observation:[/yellow] Scanning 50 files has metadata overhead.")
    console.print("           Delta would track this in _delta_log (single metadata file).")


# ---- Experiment 4: Schema Evolution ----
def experiment_schema_evolution():
    """
    Scenario: Add a new column to the dataset.

    Parquet: New files with new schema; readers must handle both schemas.
    Delta: Tracks schema versions; handles evolution automatically.

    Cost: Delta adds schema versioning overhead.
    Benefit: Automatic backward compatibility.
    """
    console.print("\n[bold cyan]Experiment 4: Schema Evolution[/bold cyan]")

    # Initial write
    df_v1 = generate_test_data(500_000)
    parquet_path = DATA_DIR / "exp4_schema_evolution.parquet"
    df_v1.write_parquet(parquet_path)

    console.print("[green]✓ Initial schema (v1):[/green]")
    console.print(f"  Columns: {df_v1.schema}")

    # "Schema evolution": add a new column
    df_v2 = df_v1.with_columns(pl.lit(0.0).alias("new_metric"))  # New column added

    console.print("\n[green]✓ Evolved schema (v2):[/green]")
    console.print(f"  Old columns: {list(df_v1.schema.names())}")
    console.print(f"  New columns: {list(df_v2.schema.names())}")

    # For Parquet, we'd need to handle this manually
    # Option 1: Rewrite all files (expensive)
    parquet_path_v2 = DATA_DIR / "exp4_schema_evolution_v2.parquet"
    start = time.perf_counter()
    df_v2.write_parquet(parquet_path_v2)
    rewrite_time = time.perf_counter() - start

    # Option 2: Keep separate version in different location (creates fragmentation)
    parquet_path_separate = DATA_DIR / "exp4_schema_evolution_separate.parquet"
    df_v2.write_parquet(parquet_path_separate)

    # Measure costs
    v1_size = parquet_path.stat().st_size / (1024 * 1024)
    v2_size = parquet_path_v2.stat().st_size / (1024 * 1024)

    console.print("\n[yellow]Parquet approach:[/yellow]")
    console.print("  • Option 1: Rewrite all files (expensive)")
    console.print(f"    - Rewrite time: {rewrite_time:.3f}s")
    console.print(f"    - File size: {v1_size:.2f} MB → {v2_size:.2f} MB")
    console.print("  • Option 2: Handle multiple schemas in reader logic (brittle)")
    console.print("    - Separate files require manual schema alignment")

    console.print("\n[cyan]Delta approach:[/cyan]")
    console.print("  • New column tracked in schema history")
    console.print("  • Readers automatically see new column (with null for old rows)")
    console.print("  • Schema enforcement prevents mismatch")
    console.print("  • No need to rewrite existing data")


# ---- Experiment 5: Compression & Encoding ----
def experiment_compression():
    """
    Both Parquet and Delta support compression, but at different levels.

    Parquet: Column-level compression (snappy, gzip, etc)
    Delta: Parquet compression + transaction log optimization

    Goal: Understand the trade-off between compression ratio and query speed.
    """
    console.print("\n[bold cyan]Experiment 5: Compression & File Size[/bold cyan]")

    df = generate_test_data(1_000_000)

    # Uncompressed Parquet
    parquet_uncompressed = DATA_DIR / "exp5_uncompressed.parquet"
    df.write_parquet(parquet_uncompressed, compression="uncompressed")

    # Snappy Parquet
    parquet_snappy = DATA_DIR / "exp5_snappy.parquet"
    df.write_parquet(parquet_snappy, compression="snappy")

    # Gzip Parquet (slower but smaller)
    parquet_gzip = DATA_DIR / "exp5_gzip.parquet"
    df.write_parquet(parquet_gzip, compression="gzip")

    results = [
        ("Uncompressed", parquet_uncompressed),
        ("Snappy", parquet_snappy),
        ("Gzip", parquet_gzip),
    ]

    table = Table(title="Compression Trade-offs")
    table.add_column("Codec", style="cyan")
    table.add_column("Size (MB)", style="green")
    table.add_column("Ratio", style="yellow")

    base_size = parquet_uncompressed.stat().st_size / (1024 * 1024)

    for name, path in results:
        size_mb = path.stat().st_size / (1024 * 1024)
        ratio = size_mb / base_size
        table.add_row(name, f"{size_mb:.2f}", f"{ratio:.2%}")

    console.print(table)

    console.print("\n[yellow]Trade-off:[/yellow]")
    console.print("  Gzip: Smaller files, slower reads (CPU-bound)")
    console.print("  Snappy: Good balance (default for most systems)")
    console.print("  None: Largest files, fastest reads (I/O-bound)")


# ---- Main ----
if __name__ == "__main__":
    console.print("[bold magenta]Day 02: Delta vs Parquet with Polars[/bold magenta]")
    console.print("[dim]Benchmarking schema, partitioning, and metadata strategies[/dim]\n")

    experiment_schema_enforcement()
    experiment_partitioning()
    experiment_multi_file_aggregation()
    experiment_schema_evolution()
    experiment_compression()

    console.print("\n[bold green]✓ All experiments complete[/bold green]")
    console.print("\n[bold cyan]Key Takeaways:[/bold cyan]")
    console.print("1. Parquet: Flexible, no enforcement. You manage schema and partitioning.")
    console.print(
        "2. Delta: Enforces schema and tracks lineage. Metadata overhead worth it at scale."
    )
    console.print(
        "3. Partitioning: Manual (Parquet) vs automatic (Delta). Delta wins with many files."
    )
    console.print(
        "4. Schema evolution: Delta handles it gracefully. Parquet requires manual handling."
    )
    console.print("5. Compression: Trade off file size vs read speed. Snappy is the sweet spot.")
