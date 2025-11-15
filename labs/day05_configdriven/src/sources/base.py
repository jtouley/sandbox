"""
Base interface for pluggable data sources.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


class DataSource(ABC):
    """
    Abstract base class for all data sources.

    Sources generate or fetch data and return the path to a JSONL file.
    This allows the pipeline to work with any ingestion method.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize source with configuration.

        Args:
            config: Source-specific configuration dict
        """
        self.config = config
        self.logger = logger.bind(source=self.__class__.__name__)

    @abstractmethod
    def generate(self) -> Path:
        """
        Generate or fetch data and write to JSONL.

        Returns:
            Path to the generated JSONL file
        """
        pass

    def validate_output(self, path: Path) -> bool:
        """
        Validate that output file exists and has data.

        Args:
            path: Path to JSONL file

        Returns:
            True if valid, raises exception otherwise
        """
        if not path.exists():
            raise FileNotFoundError(f"Output file not found: {path}")

        if path.stat().st_size == 0:
            raise ValueError(f"Output file is empty: {path}")

        self.logger.info(
            "output_validated", path=str(path), size_mb=round(path.stat().st_size / 1024 / 1024, 2)
        )
        return True


# Source registry for dynamic loading
SOURCE_REGISTRY: dict[str, type[DataSource]] = {}


def register_source(name: str):
    """
    Decorator to register a source implementation.

    Usage:
        @register_source("pubsub")
        class PubSubSource(DataSource):
            ...
    """

    def decorator(cls):
        SOURCE_REGISTRY[name] = cls
        return cls

    return decorator


def get_source(source_type: str, config: dict[str, Any]) -> DataSource:
    """
    Factory function to instantiate a source by type.

    Args:
        source_type: Type of source (pubsub, file, dlt, etc.)
        config: Source configuration

    Returns:
        Initialized DataSource instance
    """
    if source_type not in SOURCE_REGISTRY:
        available = ", ".join(SOURCE_REGISTRY.keys())
        raise ValueError(f"Unknown source type: {source_type}. Available: {available}")

    source_class = SOURCE_REGISTRY[source_type]
    return source_class(config)
