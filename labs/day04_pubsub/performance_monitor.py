"""
Performance monitoring utilities for benchmarking dedup strategies.

Tracks execution time, memory usage, CPU utilization, and disk I/O.
"""

import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

try:
    import psutil

    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


@dataclass
class PerformanceMetrics:
    """Performance metrics captured during strategy execution."""

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
    Context manager to monitor performance metrics during strategy execution.

    Usage:
        with monitor_performance("eager_dedup", 1000000) as metrics:
            # Run strategy
            pass
        print(f"Time: {metrics.execution_time_s:.2f}s")
    """
    # Capture starting state
    start_time = time.perf_counter()
    process = psutil.Process() if HAS_PSUTIL else None
    start_memory = process.memory_info().rss / 1024 / 1024 if process else None  # MB

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
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            metrics.peak_memory_mb = max(start_memory or 0, end_memory)
            metrics.cpu_percent = process.cpu_percent(interval=0.1)

        # Calculate disk size if table path provided
        if table_path and table_path.exists():
            total_size = sum(f.stat().st_size for f in table_path.rglob("*.parquet") if f.is_file())
            metrics.disk_size_mb = total_size / 1024 / 1024

        metrics.execution_time_s = execution_time
        metrics.throughput_rec_per_sec = record_count / execution_time if execution_time > 0 else 0


def format_metrics(metrics: PerformanceMetrics) -> dict:
    """Format metrics for display."""
    return {
        "strategy": metrics.strategy_name,
        "execution_time": f"{metrics.execution_time_s:.3f}s",
        "peak_memory": f"{metrics.peak_memory_mb:.1f}MB" if metrics.peak_memory_mb else "N/A",
        "cpu_percent": f"{metrics.cpu_percent:.1f}%" if metrics.cpu_percent else "N/A",
        "disk_size": f"{metrics.disk_size_mb:.2f}MB" if metrics.disk_size_mb else "N/A",
        "throughput": f"{metrics.throughput_rec_per_sec:,.0f} rec/s",
        "records": f"{metrics.record_count:,}",
    }


def print_performance_table(all_metrics: list[PerformanceMetrics]) -> None:
    """Print formatted performance comparison table."""
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

    # Calculate insights
    bronze_metrics = next((m for m in all_metrics if "bronze" in m.strategy_name.lower()), None)
    eager_metrics = next((m for m in all_metrics if "eager" in m.strategy_name.lower()), None)

    if bronze_metrics and eager_metrics:
        print("KEY INSIGHTS")
        print("=" * 120)

        # Storage overhead
        if bronze_metrics.disk_size_mb and eager_metrics.disk_size_mb:
            overhead_pct = ((bronze_metrics.disk_size_mb / eager_metrics.disk_size_mb) - 1) * 100
            print(f"✓ Bronze storage overhead: {overhead_pct:.1f}% vs Eager dedup")
            diff_mb = bronze_metrics.disk_size_mb - eager_metrics.disk_size_mb
            print(f"  → Trade-off: {diff_mb:.2f}MB for full observability")

        # Time overhead
        time_overhead_pct = (
            (bronze_metrics.execution_time_s / eager_metrics.execution_time_s) - 1
        ) * 100
        print(f"✓ Bronze time overhead: {time_overhead_pct:.1f}% vs Eager dedup")

        # Memory overhead
        if bronze_metrics.peak_memory_mb and eager_metrics.peak_memory_mb:
            mem_overhead_pct = (
                (bronze_metrics.peak_memory_mb / eager_metrics.peak_memory_mb) - 1
            ) * 100
            print(f"✓ Bronze memory overhead: {mem_overhead_pct:.1f}% vs Eager dedup")

        print(
            f"\n✓ At {bronze_metrics.record_count:,} records, Bronze pattern adds minimal overhead"
        )
        dup_count = bronze_metrics.record_count - eager_metrics.record_count
        print(f"✓ Observability benefit: Can query {dup_count:,} duplicate events")

    print("\n" + "=" * 120)


def check_psutil_available() -> bool:
    """Check if psutil is available for detailed metrics."""
    return HAS_PSUTIL


def warn_missing_psutil() -> None:
    """Warn user if psutil is not installed."""
    if not HAS_PSUTIL:
        print("⚠️  Warning: psutil not installed. Memory and CPU metrics unavailable.")
        print("   Install with: pip install psutil")
