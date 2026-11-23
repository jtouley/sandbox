"""Log in with Instagram: the full browser flow against a mocked Meta, plus the guards."""

import socket
import threading
import urllib.error
import urllib.request
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import respx

from comment_guard.auth import AppCredentials
from comment_guard.oauth import (
    EXCHANGE_URL,
    TOKEN_URL,
    OAuthError,
    authorize_url,
    browser_login,
    exchange_code,
    parse_callback,
)

SECRET = "app-secret-xyz"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def app():
    return AppCredentials(
        app_id="1234567890",
        app_secret=SECRET,
        redirect_uri=f"http://localhost:{_free_port()}/callback/",
    )


@pytest.fixture
def meta():
    with respx.mock(assert_all_called=False) as router:
        router.post(TOKEN_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "access_token": "SHORT",
                            "user_id": "1784",
                            "permissions": "instagram_business_basic,"
                            "instagram_business_manage_comments",
                        }
                    ]
                },
            )
        )
        router.get(EXCHANGE_URL).mock(
            return_value=httpx.Response(
                200, json={"access_token": "LONG", "token_type": "bearer", "expires_in": 5183944}
            )
        )
        yield router


def fake_browser(tamper_state=False, deny=False):
    """Plays the user: 'logs in' and follows Instagram's redirect to our loopback server."""
    seen = {}

    def open_browser(url: str):
        q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
        seen.update(q)
        state = "forged" if tamper_state else q["state"]
        params = (
            "error=access_denied&error_reason=user_denied"
            "&error_description=The+user+denied+your+request"
            if deny
            else f"code=AUTHCODE&state={state}"
        )
        target = f"{q['redirect_uri']}?{params}"

        def hit():
            # urllib, not httpx, so respx doesn't intercept the loopback request.
            try:
                with urllib.request.urlopen(target, timeout=5) as resp:  # noqa: S310
                    seen["page"] = resp.read().decode()
            except urllib.error.URLError as exc:
                seen["page_error"] = exc

        threading.Thread(target=hit, daemon=True).start()

    return open_browser, seen


def test_browser_login_happy_path(app, meta):
    open_browser, seen = fake_browser()
    result = browser_login(app, open_browser=open_browser, timeout=10, announce=lambda _m: None)

    assert result.access_token == "LONG"
    assert result.expires_in == 5183944
    assert result.user_id == "1784"
    assert "instagram_business_manage_comments" in result.permissions

    # What we asked Instagram for: read-only scopes, our redirect, a strong random state.
    assert seen["scope"] == "instagram_business_basic,instagram_business_manage_comments"
    assert seen["response_type"] == "code"
    assert len(seen["state"]) >= 40
    assert "Logged in" in seen["page"]
    assert "AUTHCODE" not in seen["page"]

    # Code exchange sent the secret only to Meta's token endpoint, in the POST body.
    token_call = meta.calls[0].request
    assert token_call.url == TOKEN_URL
    form = parse_qs(token_call.content.decode())
    assert form["code"] == ["AUTHCODE"]
    assert form["client_secret"] == [SECRET]
    assert form["redirect_uri"] == [app.redirect_uri]
    exchange = meta.calls[1].request.url.params
    assert exchange["grant_type"] == "ig_exchange_token"
    assert exchange["access_token"] == "SHORT"


def test_state_mismatch_is_rejected(app, meta):
    open_browser, _ = fake_browser(tamper_state=True)
    with pytest.raises(OAuthError, match="state mismatch"):
        browser_login(app, open_browser=open_browser, timeout=10, announce=lambda _m: None)
    assert not meta.calls  # never exchanged a forged code


def test_user_denial_is_reported(app, meta):
    open_browser, _ = fake_browser(deny=True)
    with pytest.raises(OAuthError, match="user denied"):
        browser_login(app, open_browser=open_browser, timeout=10, announce=lambda _m: None)


def test_timeout_when_nobody_logs_in(app, meta):
    with pytest.raises(OAuthError, match="Timed out"):
        browser_login(app, open_browser=lambda _u: None, timeout=1, announce=lambda _m: None)


def test_paste_mode_for_non_loopback_redirects(meta):
    app = AppCredentials(app_id="1", app_secret=SECRET, redirect_uri="https://example.org/cb/")
    captured = {}

    def open_browser(url):
        captured["state"] = parse_qs(urlsplit(url).query)["state"][0]

    def paste(_prompt):
        return f"https://example.org/cb/?code=PASTED&state={captured['state']}#_"

    result = browser_login(
        app, open_browser=open_browser, read_pasted_url=paste, announce=lambda _m: None
    )
    assert result.access_token == "LONG"
    assert parse_qs(meta.calls[0].request.content.decode())["code"] == ["PASTED"]


def test_parse_callback_strips_fragment_marker():
    assert parse_callback("https://x/cb/?code=abc&state=s#_", "s") == "abc"
    assert parse_callback("code=abc%23_&state=s", "s") == "abc"


def test_authorize_url_force_reauth():
    app = AppCredentials(app_id="1", app_secret="x")
    assert "force_reauth=true" in authorize_url(app, "s", force_reauth=True)
    assert SECRET not in authorize_url(app, "s")
    assert "x" not in parse_qs(urlsplit(authorize_url(app, "s")).query).get("client_secret", [])


def test_token_exchange_error_is_readable(app):
    with respx.mock() as router:
        router.post(TOKEN_URL).mock(
            return_value=httpx.Response(
                400,
                json={
                    "error_type": "OAuthException",
                    "code": 400,
                    "error_message": "Matching code was not found or was already used",
                },
            )
        )
        with pytest.raises(OAuthError, match="already used"):
            exchange_code(app, "stale")


def test_cg_login_end_to_end(settings, app, meta, monkeypatch):
    """`cg login` with app creds from env: browser -> loopback -> exchange -> /me -> stored."""
    from comment_guard import cli  # noqa: PLC0415 - after env is set by fixtures
    from comment_guard.auth import SecretStore  # noqa: PLC0415

    from .conftest import BASE  # noqa: PLC0415

    monkeypatch.setenv("IG_APP_ID", app.app_id)
    monkeypatch.setenv("IG_APP_SECRET", app.app_secret)
    monkeypatch.setenv("IG_REDIRECT_URI", app.redirect_uri)
    open_browser, _ = fake_browser()
    monkeypatch.setattr(cli.webbrowser, "open", open_browser)
    meta.get(f"{BASE}/me").mock(
        return_value=httpx.Response(
            200,
            json={"user_id": "1784", "username": "test_creator", "account_type": "MEDIA_CREATOR"},
        )
    )

    assert cli.main(["login"]) == 0
    stored = SecretStore(settings.home).load()
    assert stored.access_token == "LONG"
    assert stored.username == "test_creator"
    assert stored.expires_at is not None
    assert "instagram_business_manage_comments" in stored.permissions
