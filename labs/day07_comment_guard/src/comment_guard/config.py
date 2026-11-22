"""Paths and settings, all overridable with environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

GRAPH_HOST = "https://graph.instagram.com"
# Graph API versions live ~2 years. Override with IG_API_VERSION when Meta retires this one.
DEFAULT_API_VERSION = "v24.0"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"


@dataclass(frozen=True)
class Settings:
    home: Path  # where the token lives (outside the repo)
    data: Path  # landing / lake / meta / warehouse
    api_version: str

    @property
    def landing(self) -> Path:
        return self.data / "landing"

    @property
    def runs(self) -> Path:
        return self.data / "meta" / "runs"

    @property
    def lake(self) -> Path:
        return self.data / "lake"

    @classmethod
    def from_env(cls) -> Settings:
        home = os.environ.get("COMMENT_GUARD_HOME", str(Path.home() / ".config" / "comment-guard"))
        data = os.environ.get("COMMENT_GUARD_DATA", str(PROJECT_ROOT / "data"))
        return cls(
            home=Path(home).expanduser(),
            data=Path(data).expanduser().resolve(),
            api_version=os.environ.get("IG_API_VERSION", DEFAULT_API_VERSION),
        )
