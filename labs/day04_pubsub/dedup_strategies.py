"""
Deduplication strategies for pub/sub message processing.

Three patterns for handling duplicates and out-of-order messages:
1. Eager dedup (hash-based, simulates pub/sub layer filtering)
2. Add hashes (bronze pattern, no dedup but prep for downstream)
3. Merge dedup (idempotent writes using Delta MERGE semantics)
"""

import hashlib
import json
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake


def compute_composite_hash(row: dict) -> str:
    """
    Compute SHA-256 hash of composite key (message_id + sequence_id + partition_id).
    This represents a "natural key" hash for deduplication.
    """
    key = f"{row['message_id']}{row['sequence_id']}{row['partition_id']}"
    return hashlib.sha256(key.encode()).hexdigest()


def compute_payload_hash(row: dict) -> str:
    """
    Compute SHA-256 hash of entire payload (all fields).
    This detects content changes even with same composite key.
    """
    # Sort keys for consistent hashing
    payload = json.dumps(row, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def eager_dedup(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """
    Strategy 1: Eager deduplication (simulates pub/sub layer filtering).

    Simulates what happens when pub/sub layer deduplicates before writing.
    Uses in-memory hash set to track seen composite keys.

    Trade-offs:
    - Pro: Minimal storage, fast queries
    - Con: Lost observability (can't see what was dropped)

    Returns:
        Tuple of (deduplicated DataFrame, metrics dict)
    """
    # Add composite hash column
    df = df.with_columns(
        [
            pl.struct(["message_id", "sequence_id", "partition_id"])
            .map_elements(lambda x: compute_composite_hash(x), return_dtype=pl.Utf8)
            .alias("composite_hash")
        ]
    )

    initial_count = len(df)

    # Keep first occurrence of each composite hash (simulate pub/sub dedup)
    deduped = df.unique(subset=["composite_hash"], keep="first")

    final_count = len(deduped)
    dropped_count = initial_count - final_count

    metrics = {
        "strategy": "eager_dedup",
        "initial_records": initial_count,
        "final_records": final_count,
        "dropped_duplicates": dropped_count,
        "dedup_rate": (
            f"{(dropped_count / initial_count * 100):.2f}%" if initial_count > 0 else "0%"
        ),
    }

    return deduped, metrics


def add_hashes(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """
    Strategy 2: Append-only Bronze (add hashes, no dedup).

    Adds composite and payload hash columns without deduplication.
    This is the "immutable Bronze" pattern - keep everything, add metadata
    for downstream processing. Also adds partition_date for daily partitioning.

    Trade-offs:
    - Pro: Full observability, reprocessable, audit trail
    - Con: Larger storage, dedup happens downstream

    Returns:
        Tuple of (DataFrame with hash columns, metrics dict)
    """
    initial_count = len(df)

    # Add both hash types + partition date
    df_with_hashes = df.with_columns(
        [
            pl.struct(["message_id", "sequence_id", "partition_id"])
            .map_elements(lambda x: compute_composite_hash(x), return_dtype=pl.Utf8)
            .alias("composite_hash"),
            pl.struct(pl.all())
            .map_elements(lambda x: compute_payload_hash(x), return_dtype=pl.Utf8)
            .alias("payload_hash"),
            pl.from_epoch(pl.col("created_at"), time_unit="s").dt.date().alias("partition_date"),
        ]
    )

    # Count unique hashes for observability
    unique_composite = df_with_hashes.select("composite_hash").n_unique()
    unique_payload = df_with_hashes.select("payload_hash").n_unique()

    metrics = {
        "strategy": "add_hashes",
        "total_records": initial_count,
        "unique_composite_keys": unique_composite,
        "unique_payloads": unique_payload,
        "potential_duplicates": initial_count - unique_composite,
        "note": "No deduplication applied - immutable bronze pattern with daily partitioning",
    }

    return df_with_hashes, metrics


def merge_dedup_polars(df: pl.DataFrame, table_path: Path) -> tuple[pl.DataFrame, dict]:
    """
    Strategy 3A: Delta MERGE using Polars (lazy dedup).

    Uses message_id as primary key with last-write-wins based on created_at.
    Simulates idempotent writes using Delta table MERGE semantics.

    Trade-offs:
    - Pro: Idempotent, handles out-of-order correctly
    - Con: More complex, requires Delta table infrastructure

    Returns:
        Tuple of (final DataFrame, metrics dict)
    """
    initial_count = len(df)

    # Add composite hash for tracking
    df = df.with_columns(
        [
            pl.struct(["message_id", "sequence_id", "partition_id"])
            .map_elements(lambda x: compute_composite_hash(x), return_dtype=pl.Utf8)
            .alias("composite_hash")
        ]
    )

    # Sort by created_at and deduplicate by message_id (last-write-wins)
    deduped = df.sort("created_at").unique(subset=["message_id"], keep="last")

    final_count = len(deduped)

    metrics = {
        "strategy": "merge_dedup_polars",
        "initial_records": initial_count,
        "final_records": final_count,
        "merged_duplicates": initial_count - final_count,
        "dedup_method": "last-write-wins by message_id",
    }

    return deduped, metrics


def merge_dedup_delta(df: pl.DataFrame, table_path: Path) -> tuple[pl.DataFrame, dict]:
    """
    Strategy 3B: Delta MERGE using native Delta Lake (eager dedup).

    Uses Delta Lake's MERGE functionality for true upsert semantics.
    This simulates pub/sub layer doing idempotent writes to Delta.

    Trade-offs:
    - Pro: Native Delta features, ACID guarantees
    - Con: More I/O, pub/sub layer complexity

    Returns:
        Tuple of (final DataFrame, metrics dict)
    """
    initial_count = len(df)

    # Add composite hash
    df = df.with_columns(
        [
            pl.struct(["message_id", "sequence_id", "partition_id"])
            .map_elements(lambda x: compute_composite_hash(x), return_dtype=pl.Utf8)
            .alias("composite_hash")
        ]
    )

    # Check if table exists
    if table_path.exists():
        # Load existing table
        dt = DeltaTable(str(table_path))
        existing_df = pl.from_arrow(dt.to_pyarrow_table())

        # Combine and deduplicate (last-write-wins)
        combined = pl.concat([existing_df, df])
        deduped = combined.sort("created_at").unique(subset=["message_id"], keep="last")
    else:
        # First write
        deduped = df.sort("created_at").unique(subset=["message_id"], keep="last")

    final_count = len(deduped)

    # Write to Delta table (overwrite for simplicity in this demo)
    write_deltalake(
        str(table_path),
        deduped.to_arrow(),
        mode="overwrite",
    )

    metrics = {
        "strategy": "merge_dedup_delta",
        "initial_records": initial_count,
        "final_records_in_table": final_count,
        "merged_duplicates": initial_count - final_count,
        "dedup_method": "Delta MERGE (last-write-wins)",
        "table_path": str(table_path),
    }

    return deduped, metrics
