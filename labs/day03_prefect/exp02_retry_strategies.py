"""
Experiment 2: Retry Policies & Exponential Backoff

Goal: Inject failures, verify retry + backoff behavior.
Learn: @task(retry, retry_delay_seconds), state handling.
"""

import random
from pathlib import Path

import polars as pl
from prefect import flow, task
from rich.console import Console

DATA_DIR = Path("data/processed")


def get_console():
    """Create a fresh console instance (avoids pickle issues)."""
    return Console()


class TemporaryNetworkError(Exception):
    """Simulated transient failure."""

    pass


class SchemaValidationError(Exception):
    """Simulated schema mismatch (can be retried)."""

    pass


@task(name="fetch_data_with_retry", retries=3, retry_delay_seconds=2)
def fetch_data_with_retry(attempt_count: int = 0) -> pl.DataFrame:
    """
    Simulate data fetch with transient failures.
    Retries with exponential backoff (base 2s, jitter ±1s).
    """
    console = get_console()
    console.print(f"[cyan]→ fetch_data attempt {attempt_count + 1}[/cyan]")

    # Inject failure on first 2 attempts
    if random.random() < 0.5:
        raise TemporaryNetworkError("Simulated network timeout")

    console.print("[green]✓ fetch_data succeeded[/green]")
    return pl.DataFrame({"id": range(100), "value": range(100)})


@task(
    name="validate_schema",
    retries=2,
    retry_delay_seconds=2.5,
)
def validate_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Validate schema; fail on schema mismatch.
    Retryable (caller can fix upstream and retry).
    """
    console = get_console()
    console.print("[cyan]→ Validating schema...[/cyan]")

    if random.random() < 0.25:
        expected_columns = {"id", "values"}  # Wrong column name
    else:
        expected_columns = {"id", "value"}
    actual_columns = set(df.schema.names())

    if expected_columns != actual_columns:
        raise SchemaValidationError(
            f"Schema mismatch: expected {expected_columns}, got {actual_columns}"
        )

    console.print("[green]✓ Schema valid[/green]")
    return df


@task(name="write_with_fallback")
def write_with_fallback(df: pl.DataFrame, primary_path: Path, fallback_path: Path) -> dict:
    """
    Try primary write; fall back to alternative on failure.
    Demonstrates error handling beyond retries.
    """
    console = get_console()
    console.print("[cyan]→ Writing to primary location...[/cyan]")
    try:
        if random.random() < 0.3:
            raise OSError("Simulated primary write failure")

        df.write_parquet(primary_path)
        console.print(f"[green]✓ Wrote to primary: {primary_path}[/green]")
        return {"location": str(primary_path), "attempt": "primary"}
    except Exception as e:
        console.print(f"[yellow]⚠ Primary write failed: {e}[/yellow]")
        console.print(f"[cyan]→ Falling back to {fallback_path}[/cyan]")
        df.write_parquet(fallback_path)
        console.print(f"[green]✓ Wrote to fallback: {fallback_path}[/green]")
        return {"location": str(fallback_path), "attempt": "fallback"}


@flow(name="resilient_pipeline")
def run_resilient_pipeline() -> dict:
    """Pipeline with explicit retry + fallback strategy."""
    console = get_console()
    console.print("[bold magenta]Starting Resilient Pipeline[/bold magenta]")

    try:
        df = fetch_data_with_retry()
        validated = validate_schema(df)
        result = write_with_fallback(
            validated,
            DATA_DIR / "output_primary.parquet",
            DATA_DIR / "output_fallback.parquet",
        )
        return {"status": "success", "result": result}
    except Exception as e:
        console.print(f"[red]✗ Pipeline failed: {e}[/red]")
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    result = run_resilient_pipeline.serve(name="day 3 - prefect testing", cron="*/1 * * * *")
    console = get_console()
    console.print(f"\n[bold green]Final Result:[/bold green]\n{result}")
