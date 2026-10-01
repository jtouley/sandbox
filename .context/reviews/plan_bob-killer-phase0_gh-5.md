# Plan review — bob-killer-phase0_gh-5 (round 1)

RUN_BY_COMMAND: plan-review
Ticket: #5 · task_id gh-5
Plan: `.context/plans/bob-killer-phase0_gh-5.plan.md`
Rules loaded: peer-contract, architecture-core-rules

## Summary

The plan is well-scoped to the Phase 0 "done when". It adopts every blocking
vibe-test gap as a recorded decision and keeps gates behind one registry
(`core:dry-extension` satisfied). Four mechanics would make the
centerpiece gates fail in practice: either they reject honest work or they
can be evaded. These need fixing before E1.

## Must-fix

| ID | Area | Finding | Required change |
|---|---|---|---|
| PR-1 | `tdd_order` | The run record must reference "that red commit's test ids", but the run is recorded **before** the red commit exists, so it can't carry the commit SHA. As written, the gate can't be satisfied honestly | Run record stores pytest node ids + sha256 of the staged `tests/` tree. It's committed **in** the red commit. The gate matches each red commit's tests-tree hash to a record showing `failed` for those node ids |
| PR-2 | `tdd_order` bootstrap | The gate lands mid-branch. CI over `merge-base..HEAD` would judge the scaffold commits and the gate's own red/green commits against a rule that didn't exist yet | Enforce from the first commit carrying a `TDD-Phase:` trailer, with `scaffold` exempt. Commits before `labs/bob_killer/` existed are out of the path filter. Add a bootstrap fixture test |
| PR-3 | `assertions_frozen` | AST diff "per test function" keyed by name lets a green commit rename or delete a test to drop its asserts | Deleting or renaming a test function in `green`/`refactor` is itself a violation. Asserts are compared by (file, function, ordinal), and the function set may only grow |
| PR-4 | Stop hook vs red phase | `Stop → gates.py` with a coverage/pytest gate blocks stopping in red state. That's correct, but the plan doesn't say so, and the hook-log entry for a blocked stop is undefined | State it outright: Stop requires a green working tree. A blocked Stop appends a `stop_blocked` entry to the hash-chained log |

## Should-fix

| ID | Finding | Change |
|---|---|---|
| PR-5 | Phase A deps include streamlit, oletools, jinja2 and openpyxl, which Phase 0 never imports. That's heavier installs and wider mutation scope for nothing | Phase 0 deps: pydantic, fastapi, python-multipart, polars (contracts dtype refs only if used, else defer). Dev: pytest, pytest-cov, hypothesis, mypy, ruff, import-linter, mutmut, httpx. Later phases add their own |
| PR-6 | Workflow uses `github.base_ref`, which is empty on `push` | `--base ${{ github.event.pull_request.base.sha \|\| github.event.before }}`. On a push with an all-zero `before`, fall back to `origin/main` |
| PR-7 | OpenAPI snapshot drifts when FastAPI is bumped, failing `contracts_versioned` for a non-contract reason | Pin FastAPI and Pydantic minor versions in `pyproject.toml`. The snapshot test message names the dependency bump as a possible cause |
| PR-8 | Cadence's `gated_prefixes` (`src/`, …) don't match `labs/bob_killer/src/`, so Cadence's own `skill_gate` won't guard E1 edits | Run `skill_gate check` manually before E1 edits (Pi protocol). Note it in the plan. No config change to Cadence |

## Nits

- PR-9: `no_oracle_literals` scope (unit + integration) should be stated in CLAUDE.md, not only in the plan.
- PR-10: Name the evidence file per run: `.context/evidence/<run_id>_cheat-rejections.txt`, so reruns don't overwrite.

## Phase order vs `stages.json`

Planning artifacts are in registry order (A5 → P1 → P2 → P3). Implementation
phases A–F are internally ordered by dependency (gates before hooks/CI that
call them). No gaps.

## Rollback / blast radius

Adequate: subtree + one path-filtered workflow + `.context/`.

Verdict: READY WITH CHANGES
