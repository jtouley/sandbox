<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [comment-guard](#comment-guard)
  - [Quick start](#quick-start)
    - [Login (one paste)](#login-one-paste)
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
make login            # paste a token once; validated against /me, stored in your keyring
make capture          # 25 most recent posts -> data/landing/**/*.jsonl
make peek             # SQL over the raw files right away
make build            # dbt: Parquet lake + tests (hash checks, uniqueness, chain of custody)
```

### Login (one paste)

Getting the token is the only Meta-side setup:

1. [developers.facebook.com](https://developers.facebook.com/apps) → **Create app** →
   use case *Manage messaging & content on Instagram*.
2. **Instagram → API setup with Instagram login → Generate access tokens → Add account**,
   log in as the creator, approve.
3. Copy the token, run `cg login`, paste (input is hidden).

The account must be a **Creator or Business** account (Phil Gaimon's "Athlete" profile is a
Creator account). `cg login` checks the token against `/me` before storing it. It goes to
the OS keyring, or to `~/.config/comment-guard/token.json` (mode 0600) where there is no
keyring. For CI, set `IG_ACCESS_TOKEN` instead. Long-lived tokens last 60 days. `cg` refreshes
the token automatically once it is a day old and has under 10 days left; `cg refresh` forces it.

Permissions needed: `instagram_business_basic` and `instagram_business_manage_comments`.
Using it on **someone else's** account (a service for many creators) needs Meta App Review
for Advanced Access. One creator logging in to their own app needs no review.

## How it works

```
Graph API ──cg capture──▶ landing/  (JSONL, one line per HTTP response, never rewritten)
                           meta/runs/<run>.json  (manifest: counts, file SHA-256, versions)
                │
                ▼  read_ndjson(..., filename=true)   ← the "read-memories" pattern
           bronze/api_responses.parquet   every response + body hash re-verified
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
  come back, and whether `Authorization: Bearer` is accepted on every edge (the client sends
  the token only in the header). Check both the first time you run it against Phil's account.
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
make test   # 13 tests: redaction, token store, capture (mocked Graph API), full dbt build
make lint
```
