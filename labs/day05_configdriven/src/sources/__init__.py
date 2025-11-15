"""
Data source implementations.
All sources are auto-registered via decorators.
"""

from .base import SOURCE_REGISTRY, DataSource, get_source, register_source

# Import all sources to trigger registration
from .file import FileSource
from .polars_synthetic import PolarsSyntheticSource
from .pubsub import PubSubSource

__all__ = [
    "DataSource",
    "get_source",
    "register_source",
    "SOURCE_REGISTRY",
    "FileSource",
    "PolarsSyntheticSource",
    "PubSubSource",
]
