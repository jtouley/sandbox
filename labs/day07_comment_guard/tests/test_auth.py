import stat
from datetime import timedelta

from comment_guard.auth import AppCredentials, StoredToken, TokenStore, utcnow


def test_file_backend_is_private(settings):
    store = TokenStore(settings.home)
    backend = store.save(StoredToken(access_token="tok", obtained_at=utcnow().isoformat()))
    assert backend == "file"
    assert stat.S_IMODE((settings.home / "token.json").stat().st_mode) == 0o600
    assert store.load().access_token == "tok"
    store.clear()
    assert store.load() is None


def test_env_token_wins(settings, monkeypatch):
    monkeypatch.setenv("IG_ACCESS_TOKEN", "from-env")
    tok = TokenStore(settings.home).load()
    assert tok.access_token == "from-env"
    assert tok.source == "env"
    assert not tok.needs_refresh()


def test_refresh_policy():
    now = utcnow()
    fresh = StoredToken(access_token="t", obtained_at=now.isoformat())
    assert not fresh.needs_refresh(now)
    old = StoredToken(access_token="t", obtained_at=(now - timedelta(days=2)).isoformat())
    assert old.needs_refresh(now)  # expiry unknown -> refresh once
    old.expires_at = (now + timedelta(days=50)).isoformat()
    assert not old.needs_refresh(now)
    old.expires_at = (now + timedelta(days=5)).isoformat()
    assert old.needs_refresh(now)


def test_app_credentials_roundtrip_and_env_override(settings, monkeypatch):
    store = TokenStore(settings.home)
    assert store.load_app() is None
    store.save_app(AppCredentials(app_id="123", app_secret="shh"))
    app = store.load_app()
    assert (app.app_id, app.app_secret) == ("123", "shh")
    assert "shh" not in repr(app)
    assert stat.S_IMODE((settings.home / "app.json").stat().st_mode) == 0o600

    monkeypatch.setenv("IG_APP_ID", "999")
    monkeypatch.setenv("IG_APP_SECRET", "env-secret")
    assert store.load_app().app_id == "999"
