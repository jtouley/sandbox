"""Secret storage: the Instagram access token and the Meta app's credentials.

Each secret lives in the OS keyring, or, where there is no keyring (headless Linux), in a
0600 file under ``COMMENT_GUARD_HOME``. Environment variables override both, for CI.
Secrets never go in the repo, the data lake, logs, URLs we print, or command-line arguments.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

KEYRING_SERVICE = "comment-guard"
ENV_TOKEN = "IG_ACCESS_TOKEN"  # noqa: S105 - env var name, not a secret
ENV_APP_ID = "IG_APP_ID"
ENV_APP_SECRET = "IG_APP_SECRET"  # noqa: S105 - env var name, not a secret
ENV_REDIRECT = "IG_REDIRECT_URI"
ENV_BACKEND = "COMMENT_GUARD_TOKEN_BACKEND"  # "keyring" (default) or "file"

DEFAULT_REDIRECT_URI = "http://localhost:8765/callback/"

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
    permissions: list[str] | None = None
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


@dataclass
class AppCredentials:
    """The Meta app that "Log in with Instagram" goes through. Set up once by its owner."""

    app_id: str
    app_secret: str = field(repr=False)
    redirect_uri: str = DEFAULT_REDIRECT_URI
    source: str = "store"


class SecretStore:
    def __init__(self, home: Path):
        self.home = home

    # -- token --------------------------------------------------------------
    def load(self) -> StoredToken | None:
        env = os.environ.get(ENV_TOKEN)
        if env:
            return StoredToken(access_token=env, obtained_at=utcnow().isoformat(), source="env")
        data = self._get("instagram")
        return StoredToken(**data) if data else None

    def save(self, token: StoredToken) -> str:
        """Persist the token; returns the backend used ("keyring" or "file")."""
        return self._set("instagram", {k: v for k, v in asdict(token).items() if k != "source"})

    def clear(self) -> None:
        self._delete("instagram")

    # -- app credentials ----------------------------------------------------
    def load_app(self) -> AppCredentials | None:
        if os.environ.get(ENV_APP_ID) and os.environ.get(ENV_APP_SECRET):
            return AppCredentials(
                app_id=os.environ[ENV_APP_ID],
                app_secret=os.environ[ENV_APP_SECRET],
                redirect_uri=os.environ.get(ENV_REDIRECT, DEFAULT_REDIRECT_URI),
                source="env",
            )
        data = self._get("app")
        if not data:
            return None
        app = AppCredentials(**data)
        if os.environ.get(ENV_REDIRECT):
            app.redirect_uri = os.environ[ENV_REDIRECT]
        return app

    def save_app(self, app: AppCredentials) -> str:
        return self._set("app", {k: v for k, v in asdict(app).items() if k != "source"})

    def clear_app(self) -> None:
        self._delete("app")

    # -- backends -----------------------------------------------------------
    @staticmethod
    def _use_keyring() -> bool:
        return os.environ.get(ENV_BACKEND, "keyring") == "keyring"

    def _file(self, name: str) -> Path:
        return self.home / ("token.json" if name == "instagram" else f"{name}.json")

    def _get(self, name: str) -> dict[str, Any] | None:
        raw = None
        if self._use_keyring():
            try:
                import keyring  # noqa: PLC0415

                raw = keyring.get_password(KEYRING_SERVICE, name)
            except Exception:  # noqa: BLE001 - no keyring backend on this machine
                raw = None
        path = self._file(name)
        if raw is None and path.exists():
            raw = path.read_text(encoding="utf-8")
        return json.loads(raw) if raw else None

    def _set(self, name: str, data: dict[str, Any]) -> str:
        payload = json.dumps(data)
        if self._use_keyring():
            try:
                import keyring  # noqa: PLC0415

                keyring.set_password(KEYRING_SERVICE, name, payload)
                self._file(name).unlink(missing_ok=True)
                return "keyring"
            except Exception:  # noqa: BLE001, S110 - fall back to the private file
                pass
        self._write_private(self._file(name), payload)
        return "file"

    def _delete(self, name: str) -> None:
        if self._use_keyring():
            try:
                import keyring  # noqa: PLC0415

                keyring.delete_password(KEYRING_SERVICE, name)
            except Exception:  # noqa: S110, BLE001 - nothing stored / no backend
                pass
        self._file(name).unlink(missing_ok=True)

    def _write_private(self, path: Path, payload: str) -> None:
        self.home.mkdir(parents=True, exist_ok=True, mode=0o700)
        tmp = path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp, path)


# Backwards-compatible name used by earlier code and tests.
TokenStore = SecretStore
