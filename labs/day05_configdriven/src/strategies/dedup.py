"""
Deduplication strategy implementations - refactored from day04.

Three patterns for handling duplicates:
1. Eager dedup (hash-based filtering before write)
2. Bronze append (immutable with hash columns)
3. Polars merge (last-write-wins deduplication)

HASHING APPROACH: Uses SHA-256 hex strings (same as day04).
- Returns Utf8 hex strings like "a1b2c3d4..." (64 characters)
- Compatible with Delta Lake and distributed systems
- Deterministic across languages/platforms
- Uses Polars expressions for efficiency (avoids slow map_elements when possible)
"""

import hashlib
import json
from pathlib import Path
from typing import Any

import polars as pl
from deltalake import write_deltalake

from .base import Strategy, register_strategy


def compute_composite_hash(key_string: str) -> str:
    """Compute SHA-256 hex hash of composite key string."""
    return hashlib.sha256(key_string.encode()).hexdigest()


def compute_payload_hash(row_dict: dict) -> str:
    """Compute SHA-256 hex hash of entire row dict."""
    payload = json.dumps(row_dict, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def add_composite_hash_column(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add composite_hash column to DataFrame using SHA-256.

    DRY helper function used by all strategies.
    Returns Utf8 hex strings (64 chars).
    """
    return df.with_columns(
        pl.concat_str(
            [
                pl.col("message_id"),
                pl.col("sequence_id").cast(pl.Utf8),
                pl.col("partition_id").cast(pl.Utf8),
            ]
        )
        .map_elements(compute_composite_hash, return_dtype=pl.Utf8)
        .alias("composite_hash")
    )


@register_strategy("eager_dedup")
class EagerDedupStrategy(Strategy):
    """
    Strategy 1: Eager deduplication (simulates pub/sub layer filtering).

    Deduplicates before writing using in-memory hash set.

    Trade-offs:
    - Pro: Minimal storage, fast queries
    - Con: Lost observability (can't see what was dropped)
    - Con: Reprocessing impossible (data lost)
    """

    @property
    def table_name(self) -> str:
        return "pubsub_eager_dedup"

    def process(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any], Path]:
        """Process with eager deduplication"""
        initial_count = len(df)

        # Add composite hash column using SHA-256 (same as day04)
        df = add_composite_hash_column(df)

        # Keep first occurrence (simulate pub/sub dedup)
        deduped = df.unique(subset=["composite_hash"], keep="first")
        final_count = len(deduped)

        # Write to Delta
        table_path = self.get_table_path()
        write_deltalake(
            str(table_path),
            deduped.to_arrow(),
            mode="overwrite",
        )

        metrics = {
            "strategy": "eager_dedup",
            "initial_records": initial_count,
            "final_records": final_count,
            "dropped_duplicates": initial_count - final_count,
            "dedup_rate_pct": (
                round((initial_count - final_count) / initial_count * 100, 2)
                if initial_count > 0
                else 0
            ),
        }

        self.logger.info(
            "eager_dedup_completed",
            dropped=initial_count - final_count,
            rate_pct=metrics["dedup_rate_pct"],
        )

        return deduped, metrics, table_path


@register_strategy("bronze_append")
class BronzeAppendStrategy(Strategy):
    """
    Strategy 2: Append-only Bronze (immutable with hash columns).

    Adds composite and payload hash columns without deduplication.
    This is the "immutable Bronze" pattern - keep everything.

    Trade-offs:
    - Pro: Full observability, reprocessable, audit trail
    - Pro: Fastest strategy (no dedup overhead)
    - Con: Larger storage (duplicates kept)
    """

    @property
    def table_name(self) -> str:
        return "pubsub_bronze_append"

    def process(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any], Path]:
        """Process with bronze append (no dedup)"""
        initial_count = len(df)
        original_cols = [c for c in df.columns]

        # Add composite hash using DRY helper
        df_with_hashes = add_composite_hash_column(df)

        # Add payload hash and partition date
        df_with_hashes = df_with_hashes.with_columns(
            [
                # Payload hash (entire row) - SHA-256 hex string
                pl.struct(original_cols)
                .map_elements(compute_payload_hash, return_dtype=pl.Utf8)
                .alias("payload_hash"),
                # Partition date for efficient queries
                pl.from_epoch(pl.col("created_at"), time_unit="s")
                .dt.date()
                .alias("partition_date"),
            ]
        )

        # Count unique hashes for observability
        unique_composite = df_with_hashes.select("composite_hash").n_unique()
        unique_payload = df_with_hashes.select("payload_hash").n_unique()

        # Write to Delta with partitioning
        table_path = self.get_table_path()
        partition_by = self.config.get("partition_by", ["partition_date"])

        write_deltalake(
            str(table_path),
            df_with_hashes.to_arrow(),
            mode="append",
            partition_by=partition_by,
        )

        metrics = {
            "strategy": "bronze_append",
            "total_records": initial_count,
            "unique_composite_keys": unique_composite,
            "unique_payloads": unique_payload,
            "potential_duplicates": initial_count - unique_composite,
            "note": "No deduplication - immutable bronze pattern",
        }

        self.logger.info(
            "bronze_append_completed",
            records=initial_count,
            potential_dups=initial_count - unique_composite,
        )

        return df_with_hashes, metrics, table_path


@register_strategy("polars_merge")
class PolarsMergeStrategy(Strategy):
    """
    Strategy 3: Polars MERGE (last-write-wins deduplication).

    Uses message_id as primary key with last-write-wins based on created_at.
    In-memory deduplication using Polars (extremely fast).

    Trade-offs:
    - Pro: Idempotent, handles out-of-order correctly
    - Pro: Extremely fast (19x faster than eager dedup in benchmarks)
    - Con: Lost observability (duplicates merged away)
    """

    @property
    def table_name(self) -> str:
        return "pubsub_polars_merge"

    def process(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any], Path]:
        """Process with Polars merge deduplication"""
        initial_count = len(df)

        # Add composite hash using DRY helper
        df = add_composite_hash_column(df)

        # Sort by timestamp and deduplicate (last-write-wins)
        dedup_key = self.config.get("dedup_key", "message_id")
        sort_by = self.config.get("sort_by", "created_at")
        keep = self.config.get("keep", "last")

        deduped = df.sort(sort_by).unique(subset=[dedup_key], keep=keep)
        final_count = len(deduped)

        # Write to Delta
        table_path = self.get_table_path()
        write_deltalake(
            str(table_path),
            deduped.to_arrow(),
            mode="overwrite",
        )

        metrics = {
            "strategy": "polars_merge",
            "initial_records": initial_count,
            "final_records": final_count,
            "merged_duplicates": initial_count - final_count,
            "dedup_method": f"last-write-wins by {dedup_key}",
        }

        self.logger.info(
            "polars_merge_completed", merged=initial_count - final_count, dedup_key=dedup_key
        )

        return deduped, metrics, table_path
