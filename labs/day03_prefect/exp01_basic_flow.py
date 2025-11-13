"""
Basic Prefect flow that reads a CSV file using Polars and converts it to Delta format.
Following the local data organization: data/raw -> data/delta
"""

from pathlib import Path

import polars as pl
from prefect import flow, task
from prefect.logging import get_run_logger


@task(name="read_csv")
def read_csv(csv_path: str) -> pl.DataFrame:
    """
    Read CSV file using Polars.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Polars DataFrame
    """
    logger = get_run_logger()
    logger.info(f"Reading CSV from: {csv_path}")

    df = pl.read_csv(csv_path, truncate_ragged_lines=True)
    logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    logger.info(f"Columns: {df.columns}")

    return df


@task(name="write_delta")
def write_delta(df: pl.DataFrame, output_path: str) -> str:
    """
    Write DataFrame to Delta format.

    Args:
        df: Polars DataFrame to write
        output_path: Path where Delta table should be written

    Returns:
        Path to the written Delta table
    """
    logger = get_run_logger()
    logger.info(f"Writing Delta table to: {output_path}")

    # Ensure output directory exists
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write to Delta format
    df.write_delta(output_path, mode="overwrite")

    logger.info(f"Successfully wrote Delta table with {len(df)} rows")
    return output_path


@flow(name="csv_to_delta_flow", log_prints=True)
def csv_to_delta_flow(
    csv_path: str = "data/raw/breast_cancer.csv", delta_path: str = "data/delta/breast_cancer_delta"
):
    """
    Main flow that orchestrates CSV to Delta conversion.

    Args:
        csv_path: Path to input CSV file
        delta_path: Path to output Delta table
    """
    logger = get_run_logger()
    logger.info("Starting CSV to Delta conversion flow")

    # Read CSV
    df = read_csv(csv_path)

    # Write to Delta
    output_path = write_delta(df, delta_path)

    logger.info(f"Flow completed successfully! Delta table available at: {output_path}")
    print(f" Delta table created at: {output_path}")

    return output_path


if __name__ == "__main__":
    # Run the flow with default parameters
    csv_to_delta_flow()
