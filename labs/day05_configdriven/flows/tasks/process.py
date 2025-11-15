"""
Strategy processing Prefect task.
"""

from pathlib import Path
from typing import Any

import polars as pl
from day05_configdriven.src.config_loader import Config
from day05_configdriven.src.monitoring import monitor_performance
from day05_configdriven.src.strategies import get_strategy
from day05_configdriven.src.utils.logging_config import get_logger
from prefect import task

logger = get_logger(__name__)


@task(
    retries=3,
    retry_delay_seconds=60,
    task_run_name="{strategy_name}",
)
def process_strategy_task(
    input_path: Path,
    strategy_name: str,
    config: Config,
    record_count: int,
) -> dict[str, Any]:
    """
    Process data with specified strategy.

    Args:
        input_path: Path to input JSONL file
        strategy_name: Name of strategy to execute
        config: Pipeline configuration
        record_count: Number of input records

    Returns:
        Combined metrics dict (strategy + performance)
    """
    logger.info(
        "processing_strategy",
        strategy=strategy_name,
        input_path=str(input_path),
    )

    # Load input data with explicit string types for IDs
    # Polars can misinterpret alphanumeric strings as numbers
    df = pl.read_ndjson(input_path)

    # Ensure message_id and zipcode are strings (not inferred as int)
    if "message_id" in df.columns:
        df = df.with_columns(pl.col("message_id").cast(pl.Utf8))
    if "zipcode" in df.columns:
        df = df.with_columns(pl.col("zipcode").cast(pl.Utf8))

    # Get strategy config
    strategy_config_obj = getattr(config.strategies, strategy_name)
    strategy_config = strategy_config_obj.model_dump()

    # Get strategy instance
    strategy = get_strategy(strategy_name, strategy_config, Path(config.storage.delta_base_path))

    # Execute with performance monitoring
    with monitor_performance(
        strategy_name, record_count, strategy.get_table_path()
    ) as perf_metrics:
        processed_df, strategy_metrics, output_path = strategy.process(df)

    # Combine metrics
    combined_metrics = {
        **strategy_metrics,
        "performance": {
            "execution_time_s": round(perf_metrics.execution_time_s, 3),
            "throughput_rec_per_sec": int(perf_metrics.throughput_rec_per_sec),
            "peak_memory_mb": (
                round(perf_metrics.peak_memory_mb, 1) if perf_metrics.peak_memory_mb else None
            ),
            "disk_size_mb": (
                round(perf_metrics.disk_size_mb, 2) if perf_metrics.disk_size_mb else None
            ),
        },
        "output_path": str(output_path),
    }

    logger.info(
        "strategy_completed",
        strategy=strategy_name,
        final_records=strategy_metrics.get("final_records", len(processed_df)),
        throughput_rps=int(perf_metrics.throughput_rec_per_sec),
    )

    return combined_metrics
