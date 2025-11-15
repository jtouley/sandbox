"""
Base interface for deduplication strategies.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger()


class Strategy(ABC):
    """
    Abstract base class for all deduplication strategies.

    Strategies process a DataFrame and return:
    - Processed DataFrame
    - Metrics dict
    - Output path (optional, if written to storage)
    """

    def __init__(self, config: dict[str, Any], delta_base_path: Path):
        """
        Initialize strategy with configuration.

        Args:
            config: Strategy-specific configuration
            delta_base_path: Base path for Delta Lake tables
        """
        self.config = config
        self.delta_base_path = Path(delta_base_path)
        self.logger = logger.bind(strategy=self.__class__.__name__)

    @abstractmethod
    def process(self, df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any], Path]:
        """
        Process DataFrame with this strategy.

        Args:
            df: Input DataFrame to process

        Returns:
            Tuple of (processed_df, metrics_dict, output_path)
        """
        pass

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the Delta table name for this strategy"""
        pass

    def get_table_path(self) -> Path:
        """Get the Delta table path for this strategy"""
        return self.delta_base_path / self.table_name


# Strategy registry for dynamic loading
STRATEGY_REGISTRY: dict[str, type[Strategy]] = {}


def register_strategy(name: str):
    """
    Decorator to register a strategy implementation.

    Usage:
        @register_strategy("bronze_append")
        class BronzeAppendStrategy(Strategy):
            ...
    """

    def decorator(cls):
        STRATEGY_REGISTRY[name] = cls
        return cls

    return decorator


def get_strategy(strategy_type: str, config: dict[str, Any], delta_base_path: Path) -> Strategy:
    """
    Factory function to instantiate a strategy by type.

    Args:
        strategy_type: Type of strategy (bronze_append, polars_merge, etc.)
        config: Strategy configuration
        delta_base_path: Base path for Delta tables

    Returns:
        Initialized Strategy instance
    """
    if strategy_type not in STRATEGY_REGISTRY:
        available = ", ".join(STRATEGY_REGISTRY.keys())
        raise ValueError(f"Unknown strategy type: {strategy_type}. Available: {available}")

    strategy_class = STRATEGY_REGISTRY[strategy_type]
    return strategy_class(config, delta_base_path)
