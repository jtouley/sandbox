"""
Performance monitoring utilities for benchmarking strategies.
Tracks execution time, memory usage, CPU utilization, and disk I/O.

Copied and adapted from day04_pubsub.
"""

import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import structlog

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

logger = structlog.get_logger()


@dataclass
class PerformanceMetrics:
    """Performance metrics captured during strategy execution"""

    strategy_name: str
    execution_time_s: float
    peak_memory_mb: float | None
    cpu_percent: float | None
    disk_size_mb: float | None
    throughput_rec_per_sec: float
    record_count: int


@contextmanager
def monitor_performance(strategy_name: str, record_count: int, table_path: Path | None = None):
    """
    Context manager to monitor performance during strategy execution.

    Usage:
        with monitor_performance("bronze_append", 10000, table_path) as metrics:
            # Execute strategy
            pass
        # metrics.execution_time_s is now populated
    """
    start_time = time.perf_counter()
    process = psutil.Process() if HAS_PSUTIL else None
    start_memory = process.memory_info().rss / 1024 / 1024 if process else None

    # Initialize metrics holder
    metrics = PerformanceMetrics(
        strategy_name=strategy_name,
        execution_time_s=0.0,
        peak_memory_mb=start_memory,
        cpu_percent=None,
        disk_size_mb=None,
        throughput_rec_per_sec=0.0,
        record_count=record_count,
    )

    try:
        yield metrics
    finally:
        # Capture ending state
        end_time = time.perf_counter()
        execution_time = end_time - start_time

        # Calculate metrics
        if process:
            end_memory = process.memory_info().rss / 1024 / 1024
            metrics.peak_memory_mb = max(start_memory or 0, end_memory)
            metrics.cpu_percent = process.cpu_percent(interval=0.1)

        # Calculate disk size if table path provided
        if table_path and table_path.exists():
            total_size = sum(f.stat().st_size for f in table_path.rglob("*.parquet") if f.is_file())
            metrics.disk_size_mb = total_size / 1024 / 1024

        metrics.execution_time_s = execution_time
        metrics.throughput_rec_per_sec = record_count / execution_time if execution_time > 0 else 0

        # Log metrics
        logger.info(
            "performance_metrics",
            strategy=strategy_name,
            time_s=round(execution_time, 3),
            throughput_rps=int(metrics.throughput_rec_per_sec),
            disk_mb=round(metrics.disk_size_mb, 2) if metrics.disk_size_mb else None,
        )


def format_metrics_dict(metrics: PerformanceMetrics) -> dict:
    """Format metrics as dict for structured output"""
    return {
        "strategy": metrics.strategy_name,
        "execution_time_s": round(metrics.execution_time_s, 3),
        "peak_memory_mb": round(metrics.peak_memory_mb, 1) if metrics.peak_memory_mb else None,
        "cpu_percent": round(metrics.cpu_percent, 1) if metrics.cpu_percent else None,
        "disk_size_mb": round(metrics.disk_size_mb, 2) if metrics.disk_size_mb else None,
        "throughput_rec_per_sec": int(metrics.throughput_rec_per_sec),
        "record_count": metrics.record_count,
    }


def print_performance_table(all_metrics: list[PerformanceMetrics]) -> None:
    """Print formatted performance comparison table"""
    if not all_metrics:
        return

    print("\n" + "=" * 120)
    print("PERFORMANCE BENCHMARKING RESULTS")
    print("=" * 120)

    # Header
    header = (
        f"{'Strategy':<20} {'Records':<12} {'Time':<10} {'Memory':<12} "
        f"{'CPU':<8} {'Disk Size':<12} {'Throughput':<15}"
    )
    print(f"\n{header}")
    print("-" * 120)

    # Rows
    for m in all_metrics:
        memory_str = f"{m.peak_memory_mb:.1f}MB" if m.peak_memory_mb else "N/A"
        cpu_str = f"{m.cpu_percent:.1f}%" if m.cpu_percent else "N/A"
        disk_str = f"{m.disk_size_mb:.2f}MB" if m.disk_size_mb else "N/A"
        throughput_str = f"{m.throughput_rec_per_sec:,.0f} rec/s"

        print(
            f"{m.strategy_name:<20} "
            f"{m.record_count:<12,} "
            f"{m.execution_time_s:<10.3f}s "
            f"{memory_str:<12} "
            f"{cpu_str:<8} "
            f"{disk_str:<12} "
            f"{throughput_str:<15}"
        )

    print("\n" + "=" * 120)


def check_psutil_available() -> bool:
    """Check if psutil is available for detailed metrics"""
    return HAS_PSUTIL


def warn_missing_psutil() -> None:
    """Warn user if psutil is not installed"""
    if not HAS_PSUTIL:
        logger.warning(
            "psutil_not_installed",
            message="Memory and CPU metrics unavailable. Install with: pip install psutil",
        )
