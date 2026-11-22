"""``cg`` command line: login, capture, build, peek."""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from datetime import timedelta

from .auth import StoredToken, TokenStore, utcnow
from .capture import ME_FIELDS, run_capture
from .client import GraphClient, GraphError, refresh_long_lived_token
from .config import DBT_DIR, Settings

LOGIN_HELP = """\
Paste an Instagram access token for YOUR OWN professional (Creator/Business) account.
Get one in about two minutes:
  1. developers.facebook.com -> My Apps -> Create app -> "Manage messaging & content on Instagram"
  2. Instagram -> "API setup with Instagram login" -> Generate access tokens -> Add account
  3. Log in as the creator, approve, and copy the token shown.
The token is stored in your OS keyring (or a 0600 file if there is no keyring).
"""


def _client(settings: Settings, token: str) -> GraphClient:
    return GraphClient(token, settings.api_version)


def cmd_login(args: argparse.Namespace, settings: Settings) -> int:
    if args.token_stdin:
        token = sys.stdin.readline().strip()
    else:
        print(LOGIN_HELP)
        token = getpass.getpass("Access token (hidden): ").strip()
    if not token:
        print("No token given.", file=sys.stderr)
        return 2
    with _client(settings, token) as client:
        try:
            me = client.get("me", {"fields": ME_FIELDS}).json()
        except GraphError as exc:
            print(f"Token rejected: {exc}", file=sys.stderr)
            return 1
    stored = StoredToken(
        access_token=token,
        obtained_at=utcnow().isoformat(),
        user_id=me.get("user_id") or me.get("id"),
        username=me.get("username"),
    )
    backend = TokenStore(settings.home).save(stored)
    print(f"Logged in as @{stored.username} ({me.get('account_type', '?')}); token in {backend}.")
    return 0


def cmd_logout(_args: argparse.Namespace, settings: Settings) -> int:
    TokenStore(settings.home).clear()
    print("Token removed.")
    return 0


def _require_token(settings: Settings) -> StoredToken:
    token = TokenStore(settings.home).load()
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
    TokenStore(settings.home).save(token)


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
    token = TokenStore(settings.home).load()
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
    print(
        f"run {manifest['run_id']}: {manifest['items']} via {manifest['http_requests']} requests "
        f"-> {settings.landing}"
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

    s = sub.add_parser("login", help="store an Instagram access token")
    s.add_argument("--token-stdin", action="store_true", help="read the token from stdin")
    s.set_defaults(func=cmd_login)
    sub.add_parser("logout", help="forget the stored token").set_defaults(func=cmd_logout)
    sub.add_parser("whoami", help="show the logged-in account").set_defaults(func=cmd_whoami)
    sub.add_parser("refresh", help="refresh the 60-day token").set_defaults(func=cmd_refresh)

    s = sub.add_parser("capture", help="land raw API responses as JSONL")
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
