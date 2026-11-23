<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [comment-guard](#comment-guard)
  - [Quick start](#quick-start)
  - [Logging in](#logging-in)
    - [One-time app setup (whoever runs comment-guard, once)](#one-time-app-setup-whoever-runs-comment-guard-once)
  - [What you get: one line per comment, by post](#what-you-get-one-line-per-comment-by-post)
  - [How it works](#how-it-works)
    - [Initial capture: land first, parse later](#initial-capture-land-first-parse-later)
    - [Evidence properties](#evidence-properties)
  - [Known limits (from the research)](#known-limits-from-the-research)
  - [Supply chain / OpenSSF](#supply-chain--openssf)
  - [Development](#development)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# comment-guard

Capture a creator's **own** Instagram comment sections as evidence-grade raw data, then
model them **bronze → silver → gold** with DuckDB, Parquet and dbt. This is the data layer
for the triage idea in [the research report](../../reports/Instagram%20threat%20comment%20triage.md):
classification and human review come next, on top of `gold/comment_evidence.parquet`.

> **Scope, on purpose.** It only reads comments on an account you log in as, via Meta's
> official API. No scraping, no cross-creator profiles, nothing published. Meta's Platform
> Terms forbid processing API data "for law enforcement purposes", so this is the creator's
> own moderation and safety tool; data goes to police or a lawyer only when the creator
> chooses to send it.

## Quick start

```bash
cd labs/day07_comment_guard
make setup            # uv sync --locked (Python 3.12)
make login            # browser opens -> log in to Instagram -> approve -> done
make capture          # 25 most recent posts -> data/comments/.../post_<id>.jsonl
make build            # dbt: Parquet lake + tests (hash checks, line/raw agreement, custody)
```

## Logging in

`cg login` opens Instagram's own login page in your browser. You sign in to Instagram
(password, 2FA, whatever your account uses, all on instagram.com), approve **read profile
+ read comments**, and the tab says *Logged in*. That's it. There's no key or token to
copy, and comment-guard never sees your password.

Behind that:

1. A random 256-bit `state` is generated and a one-shot server listens on
   `127.0.0.1:8765` only.
2. Instagram redirects back with a one-time code. The state must match (CSRF protection),
   and the server answers that one request and shuts down.
3. The code is exchanged for a 1-hour token, then for a **60-day token**, server-to-server
   over HTTPS.
4. The token is checked against `/me` and stored in your OS keyring (or a 0600 file if
   there's no keyring).

It refreshes itself before it expires, so you log in again only if you stop using it for
two months. `cg login --switch-account` forces Instagram to ask which account to use.

### One-time app setup (whoever runs comment-guard, once)

Meta only lets an app it knows about request a login, and it requires the app's secret to
stay off end-user code. So the person running comment-guard registers a free Meta app once.
`cg login` walks you through it the first time (or run `cg app`):

1. [developers.facebook.com/apps](https://developers.facebook.com/apps) → **Create app** →
   use case *Manage messaging & content on Instagram*.
2. **Instagram → API setup with Instagram login → Set up Instagram business login.** In
   *Business login settings*, add the OAuth redirect URI `http://localhost:8765/callback/`.
3. **App roles → Roles → Instagram testers:** add each account that will log in (e.g. yours,
   Phil's), and accept the invite in Instagram (*Settings → Website permissions → Tester
   invites*). Testers are enough for accounts you own or manage; serving other people's
   accounts at scale needs Meta App Review for Advanced Access.
4. Paste the **Instagram** app ID and app secret into `cg app` (secret input is hidden).

If Meta's dashboard won't accept an `http://localhost` redirect, register an `https://` one
instead (`cg app --redirect-uri https://localhost:8765/callback/`). Login still works: after
approving, you paste the address you landed on (the page itself won't load) and
comment-guard takes the code from it.

The account logging in must be a **Creator or Business** account. Phil Gaimon's "Athlete"
profile is a Creator account.

Headless or CI: `IG_APP_ID` / `IG_APP_SECRET` env vars replace the stored app, and
`IG_ACCESS_TOKEN` (or `cg login --token-stdin`) replaces the browser login.

## What you get: one line per comment, by post

```
data/comments/ingest_date=2026-11-23/run_id=20261123T101500Z-a1b2c3/
    post_17912345678901234.jsonl     # one file per post that has comments
    post_17998765432109876.jsonl
```

Each file is threaded: a top-level comment, then its replies oldest-first, then the next
comment. Every comment and every reply is its own line:

```json
{"run_id": "20261123T101500Z-a1b2c3", "captured_at": "2026-11-23T10:15:02.418Z",
 "post_id": "17912345678901234", "post_shortcode": "DA1b2C3", "post_permalink": "https://www.instagram.com/p/DA1b2C3/",
 "post_posted_at": "2026-11-20T15:00:00+0000", "post_caption": "Ride day",
 "comment_id": "18034...", "parent_comment_id": null, "is_reply": false,
 "username": "angry_driver", "user_id": "17841...", "text": "get off the road or I'll run you over",
 "commented_at": "2026-11-20T16:00:00+0000", "like_count": 0, "hidden": true,
 "source": {"file": "landing/ingest_date=2026-11-23/run_id=.../comments.jsonl", "seq": 14,
            "body_sha256": "9f2c..."}}
```

`source` points to the verbatim API response the line came from, so any line can be traced
back to raw evidence. `dbt build` fails if the lines and the raw responses ever disagree.
Quick looks, no build needed:

```bash
duckdb -c "from read_ndjson('data/comments/**/*.jsonl') select post_permalink, username, text"
make peek   # raw API responses
```

## How it works

```
Graph API ──cg capture──▶ landing/   (JSONL, one line per HTTP response, never rewritten)
                           comments/  (JSONL, one file per post, one line per comment/reply)
                           meta/runs/<run>.json  (manifest: counts, file SHA-256, versions)
                │
                ▼  read_ndjson(..., filename=true)   ← the "read-memories" pattern
           bronze/api_responses.parquet   every response + body hash re-verified
           bronze/comment_lines.parquet   the per-comment lines, checked against the raw
                │
                ▼  unnest JSON
           silver/comment_observations    every sighting of every comment, per run
           silver/comments                latest state + first/last seen + evidence pointer
           silver/media                   latest state of each post
                │
                ▼
           gold/comment_evidence          one row per comment, post context, possibly_removed
           gold/capture_coverage          captured vs. Instagram's comments_count, per post
           gold/commenter_activity        PRIVATE per-account rollup (never publish)
           meta/capture_runs, meta/landing_files, warehouse meta.dbt_node_runs
```

Everything is under `data/` (git-ignored; override with `COMMENT_GUARD_DATA`). Each layer is
plain Parquet you can query from anywhere, and also a view in `data/warehouse.duckdb`:

```sql
-- duckdb data/warehouse.duckdb
from gold.gld_comment_evidence where possibly_removed;
from 'data/lake/gold/capture_coverage.parquet' order by coverage_ratio;
```

### Initial capture: land first, parse later

`cg capture` parses nothing it doesn't have to. Each HTTP response becomes one JSONL
**envelope**:

| field | why |
|---|---|
| `run_id`, `seq` | identifies the response; ties it to a run manifest |
| `stream`, `media_id`, `parent_comment_id` | what was being fetched |
| `captured_at`, `elapsed_ms`, `status_code` | when, and how it went (failures are landed too) |
| `request.url` | exact request, secrets stripped |
| `headers` | `x-app-usage`, `x-fb-trace-id`, `facebook-api-version`, … for rate limits and Meta support |
| `body`, `body_sha256` | verbatim response text and its hash |
| `redactions` | how many secrets were removed from this line |

Then DuckDB reads it straight off disk, the same way the `read-memories` skill reads
Claude Code's JSONL session logs:

```sql
select stream, count(*) from read_ndjson('data/landing/**/*.jsonl', filename = true) group by 1;
```

### Evidence properties

- **Verbatim and hashed.** Bodies are stored as the API returned them, except that tokens
  are replaced with `REDACTED` (Meta embeds the token in its paging URLs). The
  hash is taken after redaction, and the count is recorded.
- **Tamper-evident.** `dbt build` recomputes every line's hash (`body_hash_ok`) and every
  file's hash (`meta_landing_files.file_intact`) against the manifest. An edit fails the
  build (see `tests/test_pipeline.py::test_tampering_fails_the_build`).
- **Nothing is overwritten.** Each run writes new files; comments that later vanish stay in
  landing/bronze and are flagged `possibly_removed` in gold.
- **Failures are recorded.** Error responses are landed, and a failed run still writes its
  manifest with `status: failed`.

This supports the evidence-preservation checklist in the report. It doesn't replace
screenshots of the live comment and profile, or a police preservation request to Meta.

## Known limits (from the research)

- The API doesn't return comments from **Restricted** users, on age-gated posts, or
  live-video comments after the broadcast. `gold/capture_coverage` shows the gap per post.
- Not yet tested on a live account: whether comments filtered by **Hidden Words**
  come back, and whether Meta accepts `http://localhost` as a redirect URI (there's a paste
  fallback if not). The client sends the token in the `Authorization` header and switches
  to Meta's documented `access_token` parameter automatically if the header is rejected.
- Deleted comments can't be recovered, so capture before moderating, and **hide rather than delete**.
- No incremental mode yet: each run re-reads all comments for the selected posts.
  `--max-posts` keeps routine runs small. Webhooks for new comments are the planned next step.

## Supply chain / OpenSSF

Set up to score well on [OpenSSF Scorecard](https://scorecard.dev) from day one:

| Check | How |
|---|---|
| Pinned-Dependencies | `uv.lock` + `uv sync --locked`; every GitHub Action pinned to a full commit SHA |
| Token-Permissions | workflows default to `contents: read`; write scopes only per job where needed |
| SAST | CodeQL on every PR + weekly (`.github/workflows/codeql.yml`), ruff `S` (bandit) rules |
| Dependency-Update-Tool | Dependabot for `uv` and GitHub Actions (`.github/dependabot.yml`) |
| Security-Policy | `SECURITY.md` at repo root |
| License | `LICENSE` (MIT) at repo root |
| CI-Tests | `.github/workflows/comment-guard-ci.yml` (lint + tests + mocked dbt build) |
| Vulnerabilities | `pip-audit` against the lockfile in CI |
| Scorecard itself | `.github/workflows/scorecard.yml`, results to code scanning |

Needs repo settings I can't change from here: branch protection on `main` (require PR +
review + CI), private vulnerability reporting, and (for Signed-Releases) signed tags.

## Development

```bash
make test   # 25 tests: browser login (real loopback server, mocked Meta), CSRF/state,
            # redaction, secret store, capture + per-comment lines, full dbt build, tampering
make lint
```
