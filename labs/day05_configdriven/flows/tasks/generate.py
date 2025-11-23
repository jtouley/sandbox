"""
Data generation Prefect task.
"""

from pathlib import Path

import polars as pl
from day05_configdriven.src.config_loader import Config, merge_source_config
from day05_configdriven.src.sources import get_source
from day05_configdriven.src.utils.logging_config import get_logger
from prefect import task

logger = get_logger(__name__)


@task(
    name="generate_data",
    description="Generate or fetch data from configured source",
    retries=2,
    retry_delay_seconds=60,
)
def generate_data_task(config: Config) -> tuple[Path, int]:
    """
    Generate data using configured source.

    Args:
        config: Pipeline configuration

    Returns:
        Tuple of (output_path, record_count)
    """
    logger.info(
        "generating_data",
        source_type=config.source.type,
        mode=config.generation.mode,
        scale=config.generation.scale,
    )

    # Merge source-specific config
    full_config = {
        "source": config.source.model_dump(),
        "generation": config.generation.model_dump(),
        "storage": config.storage.model_dump(),
    }

    # Add source-specific config if available
    source_config = merge_source_config(config)
    full_config.update(source_config)

    # Get and execute source
    source = get_source(config.source.type, full_config)
    output_path = source.generate()

    # Count records
    df = pl.read_ndjson(output_path)
    record_count = len(df)

    logger.info(
        "data_generated",
        path=str(output_path),
        records=record_count,
    )

    return output_path, record_count
