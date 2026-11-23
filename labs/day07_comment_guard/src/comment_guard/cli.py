"""``cg``: log in with Instagram, pull comments by post, build the DuckDB lake."""

from __future__ import annotations

import argparse
import getpass
import os
import sys
import webbrowser
from datetime import timedelta

from .auth import (
    DEFAULT_REDIRECT_URI,
    AppCredentials,
    SecretStore,
    StoredToken,
    utcnow,
)
from .capture import ME_FIELDS, run_capture
from .client import GraphClient, GraphError, refresh_long_lived_token
from .config import DBT_DIR, Settings
from .oauth import SCOPES, OAuthError, browser_login

APP_HELP = """\
One-time setup: connect comment-guard to a Meta app you own (about 5 minutes).
  1. developers.facebook.com/apps -> Create app -> use case "Manage messaging & content
     on Instagram" (app type Business).
  2. Instagram -> "API setup with Instagram login" -> "Set up Instagram business login".
     Under Business login settings, add this OAuth redirect URI exactly:
       {redirect}
  3. App roles -> add the Instagram account(s) that will log in as Instagram testers, and
     accept the invite in Instagram (Settings -> Website permissions -> Tester invites).
  4. Copy the *Instagram* app ID and app secret from the same page (not the Facebook ones).
They're stored in your OS keyring (or a 0600 file); nobody who logs in ever sees them.
"""


def _client(settings: Settings, token: str) -> GraphClient:
    return GraphClient(token, settings.api_version)


def cmd_app(args: argparse.Namespace, settings: Settings) -> int:
    store = SecretStore(settings.home)
    if args.clear:
        store.clear_app()
        print("App credentials removed.")
        return 0
    if args.show:
        app = store.load_app()
        if app is None:
            print("No app configured. Run: cg app")
            return 1
        print(f"app_id={app.app_id}  redirect_uri={app.redirect_uri}  source={app.source}")
        return 0
    _setup_app(store, args.redirect_uri)
    return 0


def _setup_app(store: SecretStore, redirect_uri: str | None = None) -> AppCredentials:
    redirect = redirect_uri or os.environ.get("IG_REDIRECT_URI") or DEFAULT_REDIRECT_URI
    print(APP_HELP.format(redirect=redirect))
    app_id = input("Instagram app ID: ").strip()
    app_secret = getpass.getpass("Instagram app secret (hidden): ").strip()
    if not app_id.isdigit() or not app_secret:
        sys.exit("That doesn't look like an Instagram app ID and secret.")
    app = AppCredentials(app_id=app_id, app_secret=app_secret, redirect_uri=redirect)
    backend = store.save_app(app)
    print(f"Saved app {app_id} ({backend}).\n")
    return app


def cmd_login(args: argparse.Namespace, settings: Settings) -> int:
    store = SecretStore(settings.home)
    if args.token_stdin:  # headless / CI escape hatch
        token = sys.stdin.readline().strip()
        if not token:
            print("No token given.", file=sys.stderr)
            return 2
        return _finish_login(settings, store, token, expires_in=0, permissions=None)

    app = store.load_app() or _setup_app(store)
    try:
        result = browser_login(
            app,
            force_reauth=args.switch_account,
            open_browser=(lambda _url: None) if args.no_browser else webbrowser.open,
        )
    except OAuthError as exc:
        print(f"Login failed: {exc}", file=sys.stderr)
        return 1
    missing = [p for p in SCOPES if p not in result.permissions]
    if result.permissions and missing:
        print(f"Warning: not granted {', '.join(missing)}; comments can't be read.")
    return _finish_login(
        settings, store, result.access_token, result.expires_in, result.permissions
    )


def _finish_login(
    settings: Settings,
    store: SecretStore,
    token: str,
    expires_in: int,
    permissions: list[str] | None,
) -> int:
    with _client(settings, token) as client:
        try:
            me = client.get("me", {"fields": ME_FIELDS}).json()
        except GraphError as exc:
            print(f"Token rejected: {exc}", file=sys.stderr)
            return 1
    now = utcnow()
    stored = StoredToken(
        access_token=token,
        obtained_at=now.isoformat(),
        expires_at=(now + timedelta(seconds=expires_in)).isoformat() if expires_in else None,
        user_id=me.get("user_id") or me.get("id"),
        username=me.get("username"),
        permissions=permissions,
    )
    backend = store.save(stored)
    print(f"Logged in as @{stored.username} ({me.get('account_type', '?')}); token in {backend}.")
    return 0


def cmd_logout(args: argparse.Namespace, settings: Settings) -> int:
    store = SecretStore(settings.home)
    store.clear()
    if args.all:
        store.clear_app()
    print("Logged out." + (" App credentials removed too." if args.all else ""))
    return 0


def _require_token(settings: Settings) -> StoredToken:
    token = SecretStore(settings.home).load()
    if token is None:
        sys.exit("Not logged in. Run: cg login")
    if token.needs_refresh():
        _refresh(settings, token)
    return token


def _refresh(settings: Settings, token: StoredToken) -> None:
    new, expires_in = refresh_long_lived_token(token.access_token)
    token.access_token = new
    token.obtained_at = utcnow().isoformat()
    if expires_in:
        token.expires_at = (utcnow() + timedelta(seconds=expires_in)).isoformat()
    SecretStore(settings.home).save(token)


def cmd_whoami(_args: argparse.Namespace, settings: Settings) -> int:
    token = _require_token(settings)
    with _client(settings, token.access_token) as client:
        me = client.get("me", {"fields": ME_FIELDS}).json()
    print(
        f"@{me.get('username')}  user_id={me.get('user_id')}  type={me.get('account_type')}  "
        f"posts={me.get('media_count')}  token_source={token.source}  "
        f"expires={token.expires_at or 'unknown (refreshes after 24h)'}"
    )
    return 0


def cmd_refresh(_args: argparse.Namespace, settings: Settings) -> int:
    token = SecretStore(settings.home).load()
    if token is None:
        sys.exit("Not logged in. Run: cg login")
    _refresh(settings, token)
    print(f"Token refreshed; expires {token.expires_at}.")
    return 0


def cmd_capture(args: argparse.Namespace, settings: Settings) -> int:
    token = _require_token(settings)
    max_posts = None if args.all else args.max_posts
    with _client(settings, token.access_token) as client:
        manifest = run_capture(settings, client, max_posts=max_posts)
    items = manifest["items"]
    posts = sum(1 for f in manifest["files"] if f["stream"] == "comment_lines")
    print(
        f"run {manifest['run_id']}: {items.get('media', 0)} posts checked, "
        f"{items.get('comments', 0)} comments + {items.get('replies', 0)} replies "
        f"({manifest['http_requests']} requests)\n"
        f"  one line per comment: {settings.data / 'comments'} ({posts} post files)\n"
        f"  raw API responses:    {settings.landing}"
    )
    return 0


def cmd_build(args: argparse.Namespace, settings: Settings) -> int:
    if not any(settings.landing.glob("**/*.jsonl")):
        sys.exit("Nothing landed yet. Run: cg capture")
    from dbt.cli.main import dbtRunner  # noqa: PLC0415 - dbt import is slow

    os.environ["COMMENT_GUARD_DATA"] = str(settings.data)
    for layer in ("bronze", "silver", "gold", "meta"):  # DuckDB COPY won't create dirs
        (settings.lake / layer).mkdir(parents=True, exist_ok=True)
    res = dbtRunner().invoke(
        [
            args.command_name,
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
            *args.dbt_args,
        ]
    )
    return 0 if res.success else 1


def cmd_peek(args: argparse.Namespace, settings: Settings) -> int:
    """Query the raw landing zone directly, no build needed."""
    import duckdb  # noqa: PLC0415

    glob = str(settings.landing / "**" / "*.jsonl").replace("'", "''")
    sql = args.sql or (
        "select stream, count(*) as pages, min(captured_at) as first, max(captured_at) as last "
        "from landing group by all order by stream"
    )
    con = duckdb.connect()
    # Views can't take prepared parameters, so the (quote-escaped) path is inlined.
    con.execute(
        f"create view landing as select * from read_ndjson('{glob}', filename=true, "  # noqa: S608
        "columns={run_id:'VARCHAR', seq:'BIGINT', stream:'VARCHAR', media_id:'VARCHAR', "
        "parent_comment_id:'VARCHAR', captured_at:'TIMESTAMPTZ', status_code:'INTEGER', "
        "redactions:'INTEGER', body_sha256:'VARCHAR', body:'VARCHAR'})"
    )
    con.sql(sql).show(max_width=160)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cg", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("login", help="log in with Instagram in your browser")
    s.add_argument(
        "--switch-account", action="store_true", help="ask Instagram for credentials again"
    )
    s.add_argument("--no-browser", action="store_true", help="print the login URL only")
    s.add_argument(
        "--token-stdin", action="store_true", help="headless: read an access token from stdin"
    )
    s.set_defaults(func=cmd_login)

    s = sub.add_parser("app", help="one-time: connect to your Meta app (ID + secret)")
    s.add_argument("--redirect-uri", help=f"OAuth redirect URI (default {DEFAULT_REDIRECT_URI})")
    s.add_argument("--show", action="store_true", help="show the configured app (no secret)")
    s.add_argument("--clear", action="store_true", help="remove the stored app credentials")
    s.set_defaults(func=cmd_app)

    s = sub.add_parser("logout", help="forget the stored token")
    s.add_argument("--all", action="store_true", help="also forget the app credentials")
    s.set_defaults(func=cmd_logout)
    sub.add_parser("whoami", help="show the logged-in account").set_defaults(func=cmd_whoami)
    sub.add_parser("refresh", help="refresh the 60-day token").set_defaults(func=cmd_refresh)

    s = sub.add_parser("capture", help="pull every comment + reply, one line each, by post")
    s.add_argument("--max-posts", type=int, default=25, help="most recent N posts (default 25)")
    s.add_argument("--all", action="store_true", help="every post (slow on big accounts)")
    s.set_defaults(func=cmd_capture)

    for name in ("build", "test", "run"):
        s = sub.add_parser(name, help=f"dbt {name}: bronze -> silver -> gold")
        s.add_argument("dbt_args", nargs=argparse.REMAINDER)
        s.set_defaults(func=cmd_build, command_name=name)

    s = sub.add_parser("peek", help="SQL over raw landing files (view: landing)")
    s.add_argument("sql", nargs="?")
    s.set_defaults(func=cmd_peek)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args, Settings.from_env())


if __name__ == "__main__":
    raise SystemExit(main())
