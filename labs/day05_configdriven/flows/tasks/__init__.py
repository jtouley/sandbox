"""
Prefect tasks for pipeline stages.
"""

from .generate import generate_data_task
from .process import process_strategy_task

__all__ = ["generate_data_task", "process_strategy_task"]
