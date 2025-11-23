"""
Fast Polars-based synthetic data generation.
Bypasses asyncio overhead - generates 1M records in ~1 second.
"""

import random
import string
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from .base import DataSource, register_source


def random_id(length: int = 8) -> str:
    """Generate random alphanumeric ID"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))


@register_source("polars_synthetic")
class PolarsSyntheticSource(DataSource):
    """
    Ultra-fast synthetic data generation using Polars.

    Generates the same schema as pubsub simulator but without asyncio delays.
    Ideal for large-scale benchmarking (1M+ records).
    """

    def generate(self) -> Path:
        """
        Generate synthetic data with Polars.

        Returns:
            Path to generated JSONL file
        """
        scale = self.config["generation"]["scale"]
        seed = self.config["generation"]["seed"]
        chaos_config = self.config["generation"]["chaos"]
        source_config = self.config.get("pubsub", {})

        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

        # Sample chaos parameters if random mode
        if chaos_config["mode"] == "random":
            sampled_chaos = self._sample_chaos(chaos_config)
        else:
            sampled_chaos = self._fixed_chaos(chaos_config)

        self.logger.info(
            "generating_synthetic_data", scale=scale, dup_prob=sampled_chaos["dup_prob"], seed=seed
        )

        # Generate base dataframe
        df = self._generate_base_data(
            n_records=scale,
            partitions=source_config.get("partitions", 3),
            message_config=source_config.get("message", {}),
        )

        # Apply chaos: duplicates
        if sampled_chaos["dup_prob"] > 0:
            df = self._add_duplicates(df, sampled_chaos["dup_prob"])

        # Apply chaos: out-of-order (shuffle some records)
        if sampled_chaos["slow_prob"] > 0:
            df = self._add_out_of_order(df, sampled_chaos["slow_prob"])

        # Write to JSONL
        output_path = self._get_output_path()
        df.write_ndjson(output_path)

        self.validate_output(output_path)

        self.logger.info(
            "synthetic_data_generated",
            path=str(output_path),
            records=len(df),
            size_mb=round(output_path.stat().st_size / 1024 / 1024, 2),
        )

        return output_path

    def _generate_base_data(
        self, n_records: int, partitions: int, message_config: dict[str, Any]
    ) -> pl.DataFrame:
        """Generate base dataframe with schema matching pubsub simulator"""

        id_length = message_config.get("id_length", 8)
        value_range = message_config.get("value_range", [100, 10000])
        zipcode_digits = message_config.get("zipcode_digits", 5)

        # Generate message IDs (expensive, so batch it)
        message_ids = [random_id(id_length) for _ in range(n_records)]

        df = pl.DataFrame(
            {
                "message_id": message_ids,
                "sequence_id": pl.arange(0, n_records, eager=True),
                "partition_id": np.random.randint(0, partitions, n_records),
                "created_at": pl.arange(0, n_records, eager=True) * 0.01,  # 10ms intervals
                "value": np.random.randint(value_range[0], value_range[1], n_records),
                "zipcode": [
                    str(random.randint(10000, 99999))[:zipcode_digits] for _ in range(n_records)
                ],
            }
        )

        return df

    def _add_duplicates(self, df: pl.DataFrame, dup_prob: float) -> pl.DataFrame:
        """Add duplicate records by sampling existing data"""
        n_dups = int(len(df) * dup_prob)

        if n_dups == 0:
            return df

        # Sample random records to duplicate
        duplicates = df.sample(n=n_dups, with_replacement=True)

        # Concatenate and sort by sequence_id
        result = pl.concat([df, duplicates]).sort("sequence_id")

        self.logger.debug("duplicates_added", count=n_dups)

        return result

    def _add_out_of_order(self, df: pl.DataFrame, slow_prob: float) -> pl.DataFrame:
        """
        Simulate out-of-order delivery by shuffling a percentage of records.
        This creates realistic timestamp skew.
        """
        n_shuffle = int(len(df) * slow_prob)

        if n_shuffle == 0:
            return df

        # Split into ordered and shuffled parts using pure Polars
        # Add temporary row index for efficient filtering
        shuffle_indices = np.random.choice(len(df), n_shuffle, replace=False)

        df_indexed = df.with_row_index("_idx")
        mask = df_indexed["_idx"].is_in(shuffle_indices)

        ordered = df_indexed.filter(~mask).drop("_idx")
        shuffled = df_indexed.filter(mask).drop("_idx").sample(fraction=1.0, shuffle=True)

        # Recombine - now sequence_id order doesn't match created_at order
        result = pl.concat([ordered, shuffled]).sort("sequence_id")

        self.logger.debug("out_of_order_added", count=n_shuffle)

        return result

    def _sample_chaos(self, chaos_config: dict[str, Any]) -> dict[str, float]:
        """Sample chaos parameters from ranges"""
        return {
            "dup_prob": self._sample_value(chaos_config["dup_prob"]),
            "slow_prob": self._sample_value(chaos_config["slow_prob"]),
        }

    def _fixed_chaos(self, chaos_config: dict[str, Any]) -> dict[str, float]:
        """Use fixed chaos parameters"""
        return {
            "dup_prob": self._get_value(chaos_config["dup_prob"]),
            "slow_prob": self._get_value(chaos_config["slow_prob"]),
        }

    @staticmethod
    def _get_value(val):
        """Extract value from float or tuple"""
        return val if isinstance(val, int | float) else val[0]

    @staticmethod
    def _sample_value(val):
        """Sample from range or return fixed value"""
        if isinstance(val, list | tuple):
            return random.uniform(val[0], val[1])
        return val

    def _get_output_path(self) -> Path:
        """Construct output file path"""
        raw_path = Path(self.config["storage"]["raw_data_path"])
        raw_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"synthetic_{timestamp}.jsonl"

        return raw_path / filename
