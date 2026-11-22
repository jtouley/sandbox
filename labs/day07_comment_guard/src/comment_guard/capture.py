"""Initial data capture: land every API response verbatim as one JSONL line.

This is the "read-memories" pattern: append raw JSON lines to files, and let DuckDB's
``read_ndjson(..., filename=true)`` turn them into tables later. Nothing is parsed or
dropped at capture time, so the landing zone is the evidence of record.

Layout::

    data/landing/ingest_date=YYYY-MM-DD/run_id=<run>/<stream>.jsonl
    data/meta/runs/<run>.json          # run manifest: counts, file hashes, versions

Each line (an "envelope")::

    {schema_version, run_id, seq, stream, media_id, parent_comment_id, captured_at,
     request: {method, url}, status_code, elapsed_ms, headers: {...},
     redactions, body_sha256, body}

``body`` is the response text with secrets redacted; ``body_sha256`` is its SHA-256, so
any later edit to a landed line is detectable (dbt checks this on every build).
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import secrets
import socket
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import httpx

from . import __version__
from .client import KEEP_HEADERS, GraphClient
from .config import Settings
from .redact import redact_text

SCHEMA_VERSION = 1
ME_FIELDS = "id,user_id,username,account_type,media_count"
MEDIA_FIELDS = (
    "id,caption,media_type,media_product_type,permalink,shortcode,timestamp,"
    "comments_count,like_count,username"
)
COMMENT_FIELDS = "id,text,timestamp,username,from,like_count,hidden,parent_id"
COMMENTS_WITH_REPLIES = f"{COMMENT_FIELDS},replies.limit(50){{{COMMENT_FIELDS}}}"
PAGE_SIZE = 50


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    return f"{now:%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class Context:
    stream: str = "me"
    media_id: str | None = None
    parent_comment_id: str | None = None


@dataclass
class Landing:
    """Writes envelopes for one run. Files are ``.part`` until the run closes."""

    settings: Settings
    run_id: str
    token: str
    started: datetime
    ctx: Context = field(default_factory=Context)
    seq: int = 0
    pages: Counter[str] = field(default_factory=Counter)
    failures: int = 0
    redactions: int = 0
    _files: dict[str, IO[str]] = field(default_factory=dict)

    @property
    def run_dir(self) -> Path:
        return (
            self.settings.landing / f"ingest_date={self.started:%Y-%m-%d}" / f"run_id={self.run_id}"
        )

    def write(self, resp: httpx.Response, elapsed_ms: float) -> None:
        body, n_body = redact_text(resp.text, self.token)
        url, n_url = redact_text(str(resp.request.url), self.token)
        self.redactions += n_body + n_url
        self.seq += 1
        envelope = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "seq": self.seq,
            "stream": self.ctx.stream,
            "media_id": self.ctx.media_id,
            "parent_comment_id": self.ctx.parent_comment_id,
            "captured_at": datetime.now(UTC).isoformat(timespec="microseconds"),
            "request": {"method": resp.request.method, "url": url},
            "status_code": resp.status_code,
            "elapsed_ms": round(elapsed_ms, 1),
            "headers": {k: resp.headers[k] for k in KEEP_HEADERS if k in resp.headers},
            "redactions": n_body + n_url,
            "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            "body": body,
        }
        fh = self._files.get(self.ctx.stream)
        if fh is None:
            self.run_dir.mkdir(parents=True, exist_ok=True)
            fh = (self.run_dir / f"{self.ctx.stream}.jsonl.part").open("w", encoding="utf-8")
            self._files[self.ctx.stream] = fh
        fh.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        fh.flush()
        if resp.is_success:
            self.pages[self.ctx.stream] += 1
        else:
            self.failures += 1

    def close(self) -> list[dict[str, Any]]:
        """Finalize files (fsync + atomic rename) and return their manifest entries."""
        entries = []
        for stream, fh in sorted(self._files.items()):
            fh.flush()
            os.fsync(fh.fileno())
            fh.close()
            part = Path(fh.name)
            final = part.with_suffix("")  # drop ".part"
            os.replace(part, final)
            with final.open(encoding="utf-8") as f:
                lines = sum(1 for _ in f)
            entries.append(
                {
                    "stream": stream,
                    "path": str(final.relative_to(self.settings.data)),
                    "bytes": final.stat().st_size,
                    "lines": lines,
                    "sha256": _sha256_file(final),
                }
            )
        self._files.clear()
        return entries


def run_capture(
    settings: Settings,
    client: GraphClient,
    *,
    max_posts: int | None = 25,
    follow_replies: bool = True,
) -> dict[str, Any]:
    """Capture the account, its media, and every comment + reply. Returns the manifest."""
    started = datetime.now(UTC)
    run_id = new_run_id(started)
    landing = Landing(settings, run_id, client.token, started)
    client.on_response = landing.write
    items: Counter[str] = Counter()
    account: dict[str, Any] = {}
    status, error = "ok", None

    try:
        landing.ctx = Context("me")
        account = client.get("me", {"fields": ME_FIELDS}).json()

        landing.ctx = Context("media")
        media_ids: list[str] = []
        for page in client.pages("me/media", {"fields": MEDIA_FIELDS, "limit": PAGE_SIZE}):
            for m in page.json().get("data", []):
                media_ids.append(m["id"])
            if max_posts is not None and len(media_ids) >= max_posts:
                media_ids = media_ids[:max_posts]
                break
        items["media"] = len(media_ids)

        for media_id in media_ids:
            landing.ctx = Context("comments", media_id=media_id)
            more_replies: list[tuple[str, str]] = []
            for page in client.pages(
                f"{media_id}/comments", {"fields": COMMENTS_WITH_REPLIES, "limit": PAGE_SIZE}
            ):
                for c in page.json().get("data", []):
                    items["comments"] += 1
                    replies = c.get("replies") or {}
                    items["replies"] += len(replies.get("data", []))
                    nxt = (replies.get("paging") or {}).get("next")
                    if follow_replies and nxt:
                        more_replies.append((c["id"], nxt))

            for comment_id, first_url in more_replies:
                landing.ctx = Context("replies", media_id=media_id, parent_comment_id=comment_id)
                url: str | None = first_url
                while url:
                    body = client.get(url).json()
                    items["replies"] += len(body.get("data", []))
                    url = body.get("paging", {}).get("next")
    except Exception as exc:  # land the manifest even when a run fails
        status, error = "failed", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        files = landing.close()
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "status": status,
            "error": error,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "account": {
                "id": account.get("id"),
                "user_id": account.get("user_id"),
                "username": account.get("username"),
            },
            "params": {"max_posts": max_posts, "follow_replies": follow_replies},
            "api_base": client.base,
            "http_requests": client.requests,
            "http_failures": landing.failures,
            "redactions": landing.redactions,
            "pages": dict(landing.pages),
            "items": dict(items),
            "files": files,
            "tool": {"name": "comment-guard", "version": __version__},
            "runtime": {
                "python": sys.version.split()[0],
                "platform": platform.platform(),
                "host": socket.gethostname(),
            },
        }
        settings.runs.mkdir(parents=True, exist_ok=True)
        tmp = settings.runs / f"{run_id}.json.part"
        tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, settings.runs / f"{run_id}.json")
    return manifest
