"""
Config loader with Pydantic validation and environment variable interpolation.
"""

import os
import random
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class ChaosConfig(BaseModel):
    """
    Chaos engineering config for realistic pub/sub simulation.

    mode='fixed': use exact values
    mode='random': sample uniformly within ranges
    """

    mode: Literal["fixed", "random"] = Field(
        default="fixed", description="fixed=exact values, random=sample from ranges"
    )
    dup_prob: float | tuple[float, float] = Field(
        default=0.10, description="Duplicate probability: 0.10 or [0.05, 0.15]"
    )
    slow_prob: float | tuple[float, float] = Field(
        default=0.10, description="Out-of-order probability: 0.10 or [0.05, 0.15]"
    )
    base_delay: float | tuple[float, float] = Field(
        default=0.01, description="Delay between messages (sec): 0.01 or [0.005, 0.02]"
    )
    jitter: float | tuple[float, float] = Field(
        default=0.02, description="Random delay variation (sec): 0.02 or [0.01, 0.05]"
    )

    def sample(self) -> dict[str, float]:
        """Return sampled values for this pipeline run"""
        if self.mode == "fixed":
            return {
                "dup_prob": self._get_value(self.dup_prob),
                "slow_prob": self._get_value(self.slow_prob),
                "base_delay": self._get_value(self.base_delay),
                "jitter": self._get_value(self.jitter),
            }
        else:
            return {
                "dup_prob": self._sample_value(self.dup_prob),
                "slow_prob": self._sample_value(self.slow_prob),
                "base_delay": self._sample_value(self.base_delay),
                "jitter": self._sample_value(self.jitter),
            }

    @staticmethod
    def _get_value(val: float | tuple[float, float]) -> float:
        return val if isinstance(val, float) else val[0]

    @staticmethod
    def _sample_value(val: float | tuple[float, float]) -> float:
        if isinstance(val, float):
            return val
        return random.uniform(val[0], val[1])


class GenerationConfig(BaseModel):
    """Data generation configuration"""

    scale: int | Literal["small", "medium", "large", "xlarge"] = Field(
        default="medium", description="small=100, medium=10k, large=1M, xlarge=10M, or custom int"
    )
    seed: int | None = Field(
        default=42, description="Random seed for reproducibility (null for random)"
    )
    mode: Literal["chaos", "speed"] = Field(
        default="chaos", description="chaos=realistic simulation, speed=fast Polars synthetic"
    )
    chaos: ChaosConfig = Field(default_factory=ChaosConfig)

    @field_validator("scale")
    @classmethod
    def resolve_scale(cls, v):
        """Convert preset names to integers"""
        presets = {
            "small": 100,
            "medium": 10_000,
            "large": 1_000_000,
            "xlarge": 10_000_000,
        }
        return presets.get(v, v) if isinstance(v, str) else v


class SourceConfig(BaseModel):
    """Data source configuration"""

    type: Literal["pubsub", "polars_synthetic", "file", "dlt"] = Field(
        default="pubsub", description="Data source type"
    )
    config_file: str | None = Field(
        default=None,
        description="Path to source-specific config (e.g., config/sources/pubsub.yaml)",
    )

    # File source specific
    path: str | None = Field(
        default=None, description="Path to input file(s) - supports glob patterns"
    )


class StrategyConfig(BaseModel):
    """Base configuration for dedup strategies"""

    enabled: bool = True


class BronzeAppendConfig(StrategyConfig):
    """Bronze layer: append-only with hash columns"""

    partition_by: list[str] = Field(
        default=["partition_date"], description="Columns to partition by in Delta Lake"
    )
    hash_algorithm: Literal["sha256", "md5"] = Field(
        default="sha256", description="Hash algorithm (sha256 recommended)"
    )


class PolarsMergeConfig(StrategyConfig):
    """Polars-based MERGE deduplication"""

    dedup_key: str = Field(default="message_id", description="Column to deduplicate on")
    sort_by: str = Field(default="created_at", description="Column to sort by before dedup")
    keep: Literal["first", "last"] = Field(
        default="last", description="Which record to keep: first or last"
    )


class EagerDedupConfig(StrategyConfig):
    """Eager deduplication at ingestion (not recommended)"""

    write_mode: Literal["overwrite", "append"] = "overwrite"
    hash_algorithm: Literal["sha256", "md5"] = "sha256"


class StrategiesConfig(BaseModel):
    """All strategy configurations"""

    enabled: list[str] = Field(
        default=["bronze_append", "polars_merge"], description="List of strategies to execute"
    )
    bronze_append: BronzeAppendConfig = Field(default_factory=BronzeAppendConfig)
    polars_merge: PolarsMergeConfig = Field(default_factory=PolarsMergeConfig)
    eager_dedup: EagerDedupConfig = Field(default_factory=EagerDedupConfig)


class StorageConfig(BaseModel):
    """Storage paths and retention"""

    raw_data_path: str = Field(default="${RAW_DATA_PATH}", description="Path for raw JSONL files")
    delta_base_path: str = Field(
        default="${DELTA_TABLE_PATH}", description="Base path for Delta Lake tables"
    )
    retention_days: int = Field(default=30, description="Data retention period (days)")


class PrefectConfig(BaseModel):
    """Prefect orchestration configuration"""

    flow_name: str = "config-driven-pipeline"

    execution: dict[str, Any] = Field(
        default={
            "task_runner": "concurrent",
            "max_retries": 3,
            "retry_delay_seconds": 60,
        }
    )

    logging: dict[str, Any] = Field(
        default={
            "level": "${LOG_LEVEL}",
            "structured": True,
        }
    )

    server: dict[str, Any] = Field(default={"enabled": True, "parameters": {"cron": None}})


class PipelineConfig(BaseModel):
    """Root pipeline configuration"""

    name: str = "configdriven-pipeline"
    version: str = "1.0"


class Config(BaseModel):
    """Complete configuration schema"""

    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    source: SourceConfig = Field(default_factory=SourceConfig)
    generation: GenerationConfig = Field(default_factory=GenerationConfig)
    strategies: StrategiesConfig = Field(default_factory=StrategiesConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    prefect: PrefectConfig = Field(default_factory=PrefectConfig)

    @model_validator(mode="after")
    def validate_strategy_names(self):
        """Ensure enabled strategies exist in config"""
        valid = {"bronze_append", "polars_merge", "eager_dedup"}
        for name in self.strategies.enabled:
            if name not in valid:
                raise ValueError(f"Unknown strategy: {name}. Valid: {valid}")
        return self


def interpolate_env_vars(data: Any) -> Any:
    """
    Recursively interpolate environment variables in config.
    Supports ${VAR_NAME} syntax.
    """
    if isinstance(data, dict):
        return {k: interpolate_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [interpolate_env_vars(item) for item in data]
    elif isinstance(data, str) and data.startswith("${") and data.endswith("}"):
        var_name = data[2:-1]
        return os.environ.get(var_name, data)
    return data


def load_config(config_path: str | None = None) -> Config:
    """
    Load and validate configuration from YAML file.

    Args:
        config_path: Path to pipeline.yaml (uses $PIPELINE_CONFIG if None)

    Returns:
        Validated Config object
    """
    if config_path is None:
        # Try $PIPELINE_CONFIG first
        config_path = os.environ.get("PIPELINE_CONFIG")

        if config_path is None:
            # Try relative to current file (works when running from day05_configdriven/)
            current_file = Path(__file__).resolve()
            default_path = current_file.parent.parent / "config" / "pipeline.yaml"

            if default_path.exists():
                config_path = str(default_path)
            else:
                # Fall back to relative path from project root
                config_path = "labs/day05_configdriven/config/pipeline.yaml"

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Config file not found: {path}\n"
            f"Try setting PIPELINE_CONFIG environment variable or run from project root"
        )

    with open(path) as f:
        raw_config = yaml.safe_load(f)

    # Interpolate environment variables
    interpolated = interpolate_env_vars(raw_config)

    # Validate with Pydantic
    config = Config(**interpolated)

    return config


def merge_source_config(config: Config) -> dict[str, Any]:
    """
    Load and merge source-specific config file if specified.

    Returns:
        Combined source configuration dict
    """
    base = config.source.model_dump()

    if config.source.config_file:
        source_path = Path(config.source.config_file)
        if source_path.exists():
            with open(source_path) as f:
                source_config = yaml.safe_load(f)
                base.update(interpolate_env_vars(source_config))

    return base
