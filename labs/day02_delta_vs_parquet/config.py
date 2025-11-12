"""
Shared configuration and utilities for Delta vs Parquet experiments.
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from rich.console import Console

# Configuration - Use environment variables from .envrc
RAW_DATA = Path(os.getenv("RAW_DATA_PATH", "data/raw"))
PROCESSED_DATA = Path(os.getenv("PROCESSED_DATA_PATH", "data/processed"))
DELTA_DATA = Path(os.getenv("DELTA_TABLE_PATH", "data/delta"))

# Create directories if they don't exist
RAW_DATA.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA.mkdir(parents=True, exist_ok=True)
DELTA_DATA.mkdir(parents=True, exist_ok=True)

# Backward compatibility (deprecated, use PROCESSED_DATA or DELTA_DATA)
DATA_DIR = PROCESSED_DATA

N_ROWS = 5_000_000
SEED = 42

console = Console()


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


def print_section_header(title: str):
    """Print a formatted section header."""
    console.print(f"\n[bold cyan]{title}[/bold cyan]")


def print_step(step: str):
    """Print a verbose step description."""
    console.print(f"  [dim]→[/dim] {step}")


def print_metric(label: str, value: str, color: str = "green"):
    """Print a formatted metric."""
    console.print(f"  [{color}]✓ {label}:[/{color}] {value}")


def format_time(seconds: float) -> str:
    """Format time in seconds to human-readable string."""
    if seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    return f"{seconds:.3f}s"


def format_size(size_bytes: int) -> str:
    """Format size in bytes to human-readable string."""
    size_mb = size_bytes / (1024 * 1024)
    if size_mb < 1:
        return f"{size_bytes / 1024:.2f} KB"
    return f"{size_mb:.2f} MB"
