#!/usr/bin/env python3
"""
Run all Delta vs Parquet experiments.

Usage:
    python -m labs.day02_delta_vs_parquet.run_all
    python -m labs.day02_delta_vs_parquet.run_all --experiments 1,4
"""

import argparse
import time

from rich.console import Console
from rich.panel import Panel

from . import (
    exp01_schema_enforcement,
    exp02_partitioning,
    exp03_multi_file_aggregation,
    exp04_schema_evolution,
    exp05_compression,
)

console = Console()

EXPERIMENTS = {
    1: ("Schema Enforcement", exp01_schema_enforcement.run),
    2: ("Partitioning Strategy", exp02_partitioning.run),
    3: ("Multi-file Aggregation", exp03_multi_file_aggregation.run),
    4: ("Schema Evolution", exp04_schema_evolution.run),
    5: ("Compression Trade-offs", exp05_compression.run),
}


def main():
    parser = argparse.ArgumentParser(description="Run Delta vs Parquet experiments")
    parser.add_argument(
        "--experiments",
        "-e",
        type=str,
        help="Comma-separated list of experiment numbers to run (e.g., '1,3,5'). Defaults to all.",
    )
    args = parser.parse_args()

    # Determine which experiments to run
    if args.experiments:
        try:
            to_run = [int(x.strip()) for x in args.experiments.split(",")]
            invalid = [x for x in to_run if x not in EXPERIMENTS]
            if invalid:
                console.print(f"[red]Invalid experiment numbers: {invalid}[/red]")
                console.print(f"Valid options: {list(EXPERIMENTS.keys())}")
                return
        except ValueError:
            console.print("[red]Invalid format. Use comma-separated numbers (e.g., '1,3,5')[/red]")
            return
    else:
        to_run = list(EXPERIMENTS.keys())

    # Print header
    console.print(
        Panel.fit(
            "[bold magenta]Day 02: Delta vs Parquet with Polars[/bold magenta]\n"
            "[dim]Benchmarking schema, partitioning, and metadata strategies[/dim]",
            border_style="magenta",
        )
    )

    console.print(f"\n[cyan]Running {len(to_run)} experiment(s):[/cyan]")
    for exp_num in to_run:
        console.print(f"  • Experiment {exp_num}: {EXPERIMENTS[exp_num][0]}")

    # Run experiments
    results = []
    total_start = time.perf_counter()

    for exp_num in to_run:
        name, run_func = EXPERIMENTS[exp_num]

        console.print(f"\n{'=' * 80}")
        start = time.perf_counter()

        try:
            run_func()
            duration = time.perf_counter() - start
            results.append((exp_num, name, duration, "✅"))
        except Exception as e:
            duration = time.perf_counter() - start
            results.append((exp_num, name, duration, "❌"))
            console.print(f"\n[red]Error in Experiment {exp_num}:[/red]")
            console.print(f"[red]{e}[/red]")

    total_duration = time.perf_counter() - total_start

    # Print summary
    console.print(f"\n{'=' * 80}")
    console.print("\n[bold green]Experiment Summary:[/bold green]\n")

    for exp_num, name, duration, status in results:
        console.print(f"  {status} Experiment {exp_num}: {name} ({duration:.2f}s)")

    console.print(f"\n[bold]Total time:[/bold] {total_duration:.2f}s")

    # Print key takeaways
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

    console.print("\n[bold green]✓ All experiments complete[/bold green]")


if __name__ == "__main__":
    main()
