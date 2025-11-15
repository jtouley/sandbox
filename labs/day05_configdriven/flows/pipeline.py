"""
Main Prefect flow for config-driven pipeline.
"""

from typing import Any

from day05_configdriven.src.config_loader import load_config
from day05_configdriven.src.monitoring import PerformanceMetrics, print_performance_table
from day05_configdriven.src.utils.logging_config import setup_logging
from prefect import flow, get_run_logger

from .tasks.generate import generate_data_task
from .tasks.process import process_strategy_task


@flow(
    name="config-driven-pipeline",
    description="Config-driven data pipeline with pluggable sources and strategies",
    log_prints=True,
)
def run_pipeline(
    config_path: str | None = None,
    override_source: str | None = None,
    override_mode: str | None = None,
) -> dict[str, Any]:
    """
    Execute the full config-driven pipeline.

    Args:
        config_path: Path to pipeline.yaml (uses $PIPELINE_CONFIG if None)
        override_source: Override source type from config
        override_mode: Override generation mode from config

    Returns:
        Pipeline results with all strategy metrics
    """
    logger = get_run_logger()

    # Setup logging
    setup_logging()

    # Load configuration
    config = load_config(config_path)

    # Apply overrides if provided
    if override_source:
        config.source.type = override_source
        logger.info(f"Overriding source type: {override_source}")

    if override_mode:
        config.generation.mode = override_mode
        logger.info(f"Overriding generation mode: {override_mode}")

    logger.info(
        f"Starting pipeline: {config.pipeline.name} v{config.pipeline.version}",
    )
    logger.info(
        f"Source: {config.source.type}, Mode: {config.generation.mode}, "
        f"Scale: {config.generation.scale}"
    )

    # Task 1: Generate data
    logger.info("Step 1: Generating data...")
    input_path, record_count = generate_data_task(config)

    # Task 2: Process with each enabled strategy (parallel)
    logger.info(f"Step 2: Processing with {len(config.strategies.enabled)} strategies...")

    # Submit all strategies in parallel using .submit()
    futures = {}
    for strategy_name in config.strategies.enabled:
        logger.info(f"  → Submitting strategy: {strategy_name}")

        # Use .submit() to run tasks concurrently
        future = process_strategy_task.submit(
            input_path,
            strategy_name,
            config,
            record_count,
        )
        futures[strategy_name] = future

    # Wait for all strategies to complete
    results = {}
    for strategy_name, future in futures.items():
        results[strategy_name] = future.result()

    # Task 3: Aggregate and report
    logger.info("Step 3: Aggregating results...")

    summary = {
        "pipeline": config.pipeline.name,
        "source": config.source.type,
        "generation_mode": config.generation.mode,
        "input_records": record_count,
        "strategies": results,
    }

    # Print performance table if multiple strategies
    if len(results) > 1:
        perf_metrics = []
        for strategy_name, metrics in results.items():
            perf = metrics.get("performance", {})
            perf_metrics.append(
                PerformanceMetrics(
                    strategy_name=strategy_name,
                    execution_time_s=perf.get("execution_time_s", 0),
                    peak_memory_mb=perf.get("peak_memory_mb"),
                    cpu_percent=None,
                    disk_size_mb=perf.get("disk_size_mb"),
                    throughput_rec_per_sec=perf.get("throughput_rec_per_sec", 0),
                    record_count=record_count,
                )
            )
        print_performance_table(perf_metrics)

    logger.info("Pipeline completed successfully!")

    return summary


if __name__ == "__main__":
    # Run the flow
    run_pipeline()
