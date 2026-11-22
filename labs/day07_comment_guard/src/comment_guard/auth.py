"""Token storage: one long-lived Instagram User access token per install.

Lookup order: ``IG_ACCESS_TOKEN`` env var (CI / one-offs) -> OS keyring -> 0600 file.
The token never goes in the repo, the data lake, logs, or command-line arguments.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

KEYRING_SERVICE = "comment-guard"
KEYRING_USER = "instagram"
ENV_TOKEN = "IG_ACCESS_TOKEN"  # noqa: S105 - env var name, not a secret
ENV_BACKEND = "COMMENT_GUARD_TOKEN_BACKEND"  # "keyring" (default) or "file"

# Long-lived tokens last 60 days and can be refreshed once they are >= 24h old.
REFRESH_MIN_AGE = timedelta(hours=24)
REFRESH_WHEN_LEFT = timedelta(days=10)


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class StoredToken:
    access_token: str = field(repr=False)
    obtained_at: str
    expires_at: str | None = None
    user_id: str | None = None
    username: str | None = None
    source: str = "store"

    def needs_refresh(self, now: datetime | None = None) -> bool:
        now = now or utcnow()
        if self.source == "env":
            return False
        if now - datetime.fromisoformat(self.obtained_at) < REFRESH_MIN_AGE:
            return False
        if self.expires_at is None:
            return True  # refresh once to learn the expiry
        return datetime.fromisoformat(self.expires_at) - now < REFRESH_WHEN_LEFT


class TokenStore:
    def __init__(self, home: Path):
        self.home = home
        self.file = home / "token.json"

    # -- public -------------------------------------------------------------
    def load(self) -> StoredToken | None:
        env = os.environ.get(ENV_TOKEN)
        if env:
            return StoredToken(access_token=env, obtained_at=utcnow().isoformat(), source="env")
        raw = self._keyring_get() if self._use_keyring() else None
        if raw is None and self.file.exists():
            raw = self.file.read_text(encoding="utf-8")
        return StoredToken(**json.loads(raw)) if raw else None

    def save(self, token: StoredToken) -> str:
        """Persist the token; returns the backend used ("keyring" or "file")."""
        payload = json.dumps({k: v for k, v in asdict(token).items() if k != "source"})
        if self._use_keyring() and self._keyring_set(payload):
            self.file.unlink(missing_ok=True)
            return "keyring"
        self._write_private(payload)
        return "file"

    def clear(self) -> None:
        if self._use_keyring():
            try:
                import keyring  # noqa: PLC0415

                keyring.delete_password(KEYRING_SERVICE, KEYRING_USER)
            except Exception:  # noqa: S110, BLE001 - nothing stored / no backend
                pass
        self.file.unlink(missing_ok=True)

    # -- backends -----------------------------------------------------------
    @staticmethod
    def _use_keyring() -> bool:
        return os.environ.get(ENV_BACKEND, "keyring") == "keyring"

    @staticmethod
    def _keyring_get() -> str | None:
        try:
            import keyring  # noqa: PLC0415

            return keyring.get_password(KEYRING_SERVICE, KEYRING_USER)
        except Exception:  # noqa: BLE001 - headless Linux has no keyring backend
            return None

    @staticmethod
    def _keyring_set(payload: str) -> bool:
        try:
            import keyring  # noqa: PLC0415

            keyring.set_password(KEYRING_SERVICE, KEYRING_USER, payload)
            return True
        except Exception:  # noqa: BLE001
            return False

    def _write_private(self, payload: str) -> None:
        self.home.mkdir(parents=True, exist_ok=True, mode=0o700)
        tmp = self.file.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp, self.file)
