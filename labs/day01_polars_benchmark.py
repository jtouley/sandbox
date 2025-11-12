import time

import polars as pl

# 1M row test
df = pl.DataFrame(
    {
        "id": range(1_000_000),
        "group": [str(i % 100) for i in range(1_000_000)],
        "val": range(1_000_000),
    }
)

# Benchmark lazy vs eager
start = time.perf_counter()
result = df.lazy().group_by("group").agg(pl.col("val").mean()).collect()
print(f"Lazy: {time.perf_counter() - start:.3f}s")

start = time.perf_counter()
result = df.group_by("group").agg(pl.col("val").mean())
print(f"Eager: {time.perf_counter() - start:.3f}s")

# Compare query plans
lazy_query = df.lazy().group_by("group").agg(pl.col("val").mean())
print("\nLazy plan:")
print(lazy_query.explain())

# Test with a more complex query where lazy wins
lazy_complex = (
    df.lazy()
    .filter(pl.col("val") > 500_000)
    .group_by("group")
    .agg(pl.col("val").mean())
    .filter(pl.col("val") > 750_000)
)

start = time.perf_counter()
result = lazy_complex.collect()
print(f"\nLazy (with filters): {time.perf_counter() - start:.3f}s")

# Eager equivalent
start = time.perf_counter()
result = (
    df.filter(pl.col("val") > 500_000)
    .group_by("group")
    .agg(pl.col("val").mean())
    .filter(pl.col("val") > 750_000)
)
print(f"Eager (with filters): {time.perf_counter() - start:.3f}s")

path = "data/raw/yellow_tripdata_2025-01.parquet"

# Lazy evaluation with explain
print("=== LAZY PLAN ===")
lazy_query = (
    pl.scan_parquet(path)
    .filter(pl.col("passenger_count") > 2)
    .select(["trip_distance", "fare_amount", "passenger_count"])
    .group_by("passenger_count")
    .agg(pl.col("fare_amount").mean())
)
print(lazy_query.explain(optimized=True))

start = time.perf_counter()
lazy_result = lazy_query.collect()
lazy_time = time.perf_counter() - start
print(f"Lazy execution: {lazy_time:.3f}s | Rows: {lazy_result.height}\n")

# Eager evaluation (read full, then filter)
print("=== EAGER PLAN ===")
start = time.perf_counter()
eager_result = (
    pl.read_parquet(path)
    .filter(pl.col("passenger_count") > 2)
    .select(["trip_distance", "fare_amount", "passenger_count"])
    .group_by("passenger_count")
    .agg(pl.col("fare_amount").mean())
)
eager_time = time.perf_counter() - start
print(f"Eager execution: {eager_time:.3f}s | Rows: {eager_result.height}\n")

# Show the optimization difference
print("=== UNOPTIMIZED (for comparison) ===")
unopt = pl.scan_parquet(path).explain(optimized=False)
print(unopt[:500])  # Truncate for readability
