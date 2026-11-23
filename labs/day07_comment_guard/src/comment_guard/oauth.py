"""Log in with Instagram: browser-based OAuth 2.0 authorization-code flow.

Flow (Business Login for Instagram):

1. Open ``https://www.instagram.com/oauth/authorize`` in the user's browser with a random
   ``state``. They log in to Instagram and approve read access to their comments.
2. Instagram redirects to ``redirect_uri?code=...&state=...``. For a ``http://localhost``
   redirect we catch it on a one-shot server bound to 127.0.0.1; for any other redirect URI
   the user pastes the URL they landed on.
3. Exchange the code (single use, 1 hour) for a short-lived token, then for a 60-day token.

Security properties:

- ``state`` is 256 random bits and must match, so a forged redirect can't log you in as
  someone else (CSRF). The callback server answers one request on loopback only, then exits.
- The app secret is used only in the two server-to-server exchanges, over HTTPS, from this
  machine. Meta requires that it never ships inside client code, which is why each operator
  registers their own app once (see README) instead of the secret living in this repo.
- Nothing secret is printed, logged, or put in the browser's history except the one-time code.
"""

from __future__ import annotations

import secrets
import threading
import time
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx

from .auth import AppCredentials
from .config import GRAPH_HOST

AUTHORIZE_URL = "https://www.instagram.com/oauth/authorize"
TOKEN_URL = "https://api.instagram.com/oauth/access_token"  # noqa: S105 - endpoint, not a secret
EXCHANGE_URL = f"{GRAPH_HOST}/access_token"
# Read-only: profile + comments. Nothing that can post or message.
SCOPES = ("instagram_business_basic", "instagram_business_manage_comments")
LOGIN_TIMEOUT_S = 300

_DONE_PAGE = b"""<!doctype html><meta charset="utf-8"><title>comment-guard</title>
<body style="font:16px system-ui;margin:4rem auto;max-width:32rem">
<h1>%s</h1><p>%s</p><p>You can close this tab and go back to the terminal.</p></body>"""


class OAuthError(RuntimeError):
    pass


@dataclass
class LoginResult:
    access_token: str
    expires_in: int
    user_id: str | None
    permissions: list[str]


def authorize_url(app: AppCredentials, state: str, *, force_reauth: bool = False) -> str:
    params = {
        "client_id": app.app_id,
        "redirect_uri": app.redirect_uri,
        "response_type": "code",
        "scope": ",".join(SCOPES),
        "state": state,
    }
    if force_reauth:
        params["force_reauth"] = "true"
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def parse_callback(url_or_query: str, expected_state: str) -> str:
    """Pull the auth code out of the redirect URL (or its query string)."""
    raw = url_or_query.strip()
    query = urlsplit(raw).query if "?" in raw or raw.startswith("http") else raw
    params = {k: v[0] for k, v in parse_qs(query).items()}
    if "error" in params:
        reason = params.get("error_description") or params.get("error_reason") or params["error"]
        raise OAuthError(f"Instagram login was not approved: {reason.replace('+', ' ')}")
    if not secrets.compare_digest(params.get("state", ""), expected_state):
        raise OAuthError("Login response didn't match this login attempt (state mismatch).")
    code = params.get("code", "").removesuffix("#_")
    if not code:
        raise OAuthError("No authorization code in the redirect.")
    return code


def _is_loopback(redirect_uri: str) -> bool:
    parts = urlsplit(redirect_uri)
    return parts.scheme == "http" and parts.hostname in {"localhost", "127.0.0.1"}


def wait_for_callback(redirect_uri: str, expected_state: str, timeout: float) -> str:
    """Serve exactly one redirect on 127.0.0.1 and return the auth code."""
    parts = urlsplit(redirect_uri)
    path = parts.path or "/"
    result: dict[str, str] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            req = urlsplit(self.path)
            if req.path.rstrip("/") != path.rstrip("/"):
                self.send_error(404)
                return
            try:
                result["code"] = parse_callback(req.query, expected_state)
                title, msg = b"Logged in", b"comment-guard has your Instagram login."
            except OAuthError as exc:
                result["error"] = str(exc)
                title, msg = b"Login failed", str(exc).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(_DONE_PAGE % (title, msg))

        def log_message(self, *_args: object) -> None:  # keep the code out of the terminal
            return

    server = HTTPServer(("127.0.0.1", parts.port or 80), Handler)
    server.timeout = 0.5
    deadline = time.monotonic() + timeout
    try:
        while not result and time.monotonic() < deadline:
            server.handle_request()
    finally:
        server.server_close()
    if "error" in result:
        raise OAuthError(result["error"])
    if "code" not in result:
        raise OAuthError(f"Timed out after {int(timeout)}s waiting for the Instagram login.")
    return result["code"]


def exchange_code(
    app: AppCredentials, code: str, *, transport: httpx.BaseTransport | None = None
) -> tuple[str, str | None, list[str]]:
    """Code -> short-lived token. Returns (token, instagram user_id, granted permissions)."""
    with httpx.Client(timeout=30.0, transport=transport) as http:
        resp = http.post(
            TOKEN_URL,
            data={
                "client_id": app.app_id,
                "client_secret": app.app_secret,
                "grant_type": "authorization_code",
                "redirect_uri": app.redirect_uri,
                "code": code,
            },
        )
    body = _json_or_raise(resp, "exchange the login code")
    data = body["data"][0] if isinstance(body.get("data"), list) else body
    perms = data.get("permissions") or ""
    perms_list = perms.split(",") if isinstance(perms, str) else list(perms)
    user_id = data.get("user_id")
    return data["access_token"], str(user_id) if user_id else None, [p for p in perms_list if p]


def exchange_long_lived(
    app: AppCredentials, short_token: str, *, transport: httpx.BaseTransport | None = None
) -> tuple[str, int]:
    """Short-lived (1h) -> long-lived (60 day) token. Returns (token, expires_in seconds)."""
    with httpx.Client(timeout=30.0, transport=transport) as http:
        resp = http.get(
            EXCHANGE_URL,
            params={
                "grant_type": "ig_exchange_token",
                "client_secret": app.app_secret,
                "access_token": short_token,
            },
        )
    body = _json_or_raise(resp, "get a long-lived token")
    return body["access_token"], int(body.get("expires_in", 0))


def browser_login(
    app: AppCredentials,
    *,
    force_reauth: bool = False,
    open_browser: Callable[[str], object] = webbrowser.open,
    read_pasted_url: Callable[[str], str] = input,
    timeout: float = LOGIN_TIMEOUT_S,
    transport: httpx.BaseTransport | None = None,
    announce: Callable[[str], None] = print,
) -> LoginResult:
    state = secrets.token_urlsafe(32)
    url = authorize_url(app, state, force_reauth=force_reauth)

    if _is_loopback(app.redirect_uri):
        box: dict[str, object] = {}

        def serve() -> None:
            try:
                box["code"] = wait_for_callback(app.redirect_uri, state, timeout)
            except Exception as exc:  # noqa: BLE001 - re-raised on the main thread
                box["error"] = exc

        t = threading.Thread(target=serve, daemon=True)
        t.start()
        time.sleep(0.05)  # let the socket bind before the browser can hit it
        announce("Opening Instagram in your browser to log in...")
        announce(f"If it doesn't open, visit:\n  {url}")
        open_browser(url)
        t.join(timeout + 5)
        if "error" in box:
            raise box["error"]  # type: ignore[misc]
        if "code" not in box:
            raise OAuthError("Login did not complete.")
        code = str(box["code"])
    else:
        announce("Opening Instagram in your browser to log in...")
        announce(f"If it doesn't open, visit:\n  {url}")
        open_browser(url)
        code = parse_callback(
            read_pasted_url("After approving, paste the full URL you were sent to: "), state
        )

    short, user_id, perms = exchange_code(app, code, transport=transport)
    token, expires_in = exchange_long_lived(app, short, transport=transport)
    return LoginResult(token, expires_in, user_id, perms)


def _json_or_raise(resp: httpx.Response, what: str) -> dict:
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if resp.is_success and "error" not in body and "error_type" not in body:
        return body
    err = body.get("error")
    msg = (
        err.get("message")
        if isinstance(err, dict)
        else body.get("error_message") or resp.text[:200]
    )
    raise OAuthError(f"Couldn't {what}: {msg} (HTTP {resp.status_code})")
