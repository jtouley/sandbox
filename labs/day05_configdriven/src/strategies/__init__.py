"""
Deduplication strategy implementations.
All strategies are auto-registered via decorators.
"""

from .base import STRATEGY_REGISTRY, Strategy, get_strategy, register_strategy

# Import all strategies to trigger registration
from .dedup import (
    BronzeAppendStrategy,
    EagerDedupStrategy,
    PolarsMergeStrategy,
)

__all__ = [
    "Strategy",
    "get_strategy",
    "register_strategy",
    "STRATEGY_REGISTRY",
    "BronzeAppendStrategy",
    "EagerDedupStrategy",
    "PolarsMergeStrategy",
]
