# E3 plan conformance — gh-5 (manual pass alongside architecture_review.py --phase impl)

RUN_BY_SKILL: cadence
Ticket: #5 · Plan: `.context/plans/bob-killer-phase0_gh-5.plan.md`

| Plan item | Status | Where |
|---|---|---|
| A scaffold (uv, 3.12, layout, Apache-2.0, ALLOWLIST) | Done | `labs/bob_killer/` |
| B gates: tdd_order, assertions_frozen, no_oracle_literals, no_silent_skips, exact_parity, append_only_log, contracts_versioned, types_lint, boundaries, coverage, mutation (nightly) | Done, one `@gate` registry | `scripts/bk_gates/` |
| C contracts + DDL + snapshot versioning | Done (v1 snapshot) | `src/bob_killer/contracts/` |
| D registries, service layer, CLI, `POST /runs`, upload limits | Done | `registry.py`, `service/`, `api/`, `cli.py` |
| E golden.lock + fetch_golden + oracle stub | Done; downloads blocked by egress policy | `golden.lock`, `scripts/fetch_golden.py`, `verify/oracle.py` |
| F hooks + CI + CODEOWNERS | Done | `.claude/settings.json`, `.github/workflows/bob-killer.yml`, `.github/CODEOWNERS` |
| Adversarial C1–C7 | Done, each with a cheat fixture | `tests/gates/` |
| Test-benchmark TB-1..TB-5 | Done | `tests/gates/conftest.py`, property tests |

## Deviations (recorded, not hidden)

1. **No subtree pre-commit hooks** (plan F). Local enforcement is the Claude Stop hook. CI is the backstop. The sandbox root `.pre-commit-config.yaml` is untouched.
2. **PostToolUse runs the unit + integration suites**, not an "affected tests" subset. Same safety, slower. Optimize once the suite grows.
3. **Added a `service` layer** between api/cli and the stages (import-linter `|` siblings may not import each other, so the CLI couldn't reuse the API module). That's decision 7's "one service layer", made explicit.
4. **The literal-assert lint is stricter than the spec's intent.** It also bans structural constants (`== ("SUM",)`). Tests assert against derived values instead. Revisit if it starts costing clarity.
5. **Three gate holes were found by the real-commit cheat demo and fixed** (red → gate-change): `__import__`-hidden skips, the contracts gate reading the installed package, and `.context/hooks.log` ignored by the sandbox `*.log` rule. Evidence: `.context/evidence/*_cheat-rejections_round{1,2}.txt` and the final round.

## Not yet proven

- Phase 0 done-when #1, "CI runs green", needs the workflow to run on GitHub. It triggers on pull requests (and on main). No PR exists yet.
- Golden workbooks are not downloaded: the egress policy blocks epa.gov, mdt.mt.gov, nrel.gov and nlr.gov.
