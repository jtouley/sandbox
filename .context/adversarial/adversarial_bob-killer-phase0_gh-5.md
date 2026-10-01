# Adversarial review — Bob Killer Phase 0 plan (gh-5)

RUN_BY_SKILL: de-adversarial-reviewer
Ticket: #5 · task_id gh-5
Target: `.context/plans/bob-killer-phase0_gh-5.plan.md` (incl. 2026-10-01 update), ADR-0001, SPEC.md

## Verification notes

Files inspected:
- `labs/bob_killer/SPEC.md`
- the plan
- both plan reviews
- `.context/proposals/ADR-0001_bob-killer-phase0_gh-5.md`
- sandbox `.gitignore` (`.context` not ignored, confirmed with `git check-ignore`, rc=1)
- sandbox `pyproject.toml` (`requires-python >=3.14`, which confirms the separate-project need)
- the absence of `.github/workflows` in sandbox
- Cadence `cadence.config.json` (`gated_prefixes`)

Not inspectable: the golden workbooks (hosts unreachable from the container).

Stance: assume the implementing agent is the adversary and wants green with the
least real work.

## Adversarial scenarios

| # | Attack | Handled? | Evidence |
|---|---|---|---|
| X1 | Forge a red run record: write `run.json` by hand claiming `failed` without running pytest | **Partially** | The tests-tree hash pins content but not that pytest actually ran. **Condition C1** |
| X2 | Commit the impl under `TDD-Phase: scaffold` to dodge red/green | Yes | Update PR-2: scaffold may not touch `src/**/*.py` beyond empty `__init__`/docstrings |
| X3 | Put the logic in `tests/conftest.py` (a red commit may touch tests/) and import it from src | Partially | `src` → `tests` imports aren't banned. **Condition C2** |
| X4 | Loosen an assert by changing the helper `tests/oracle_values.py` or the JSON in `tests/oracle/` during green | **No** | `assertions_frozen` only looks at assert nodes. **Condition C3** |
| X5 | Rewrite `.context/hooks.log` and recompute the whole hash chain | **No** | A self-contained chain can be fully regenerated. **Condition C4** |
| X6 | Weaken a gate itself (edit `scripts/gates.py`) in a green commit | Partially | Gates are tested by cheat fixtures, but nothing stops a commit from editing the gate and its test together. **Condition C5** |
| X7 | Bump `schema_version` on every change to silence `contracts_versioned` | Accepted | That's the intended escape hatch. Visible in the diff, and reviewers see `snapshots/vN/` growth |
| X8 | `# allow: <id>` pointing at a nonexistent or empty ALLOWLIST row | Yes | Plan B: the row must exist. Strengthened by C6 (row needs an issue link) |
| X9 | `exact_parity` evaded via `**kwargs` or a module-level `TOL` constant | Partially | **Condition C6** |
| X10 | CI base SHA wrong on force-push, so the range covers zero commits and the gate trivially passes | Partially | **Condition C7** |

## Blocking issues

None that prevent E1. Every gap above has a bounded condition that fits
inside Phase 0's scope.

## Conditions (must land in E1; tracked as findings)

- **C1 (X1):** `record_run.py` is the only writer of `run.json`. It shells out to pytest with `--junitxml` and stores the junit file's sha256 alongside. `tdd_order` re-parses the committed junit XML and checks that the node ids and outcomes agree with `run.json`. A forged pair is still possible but now needs two coordinated forgeries, which is visible in review. Document the residual risk.
- **C2 (X3):** import-linter contract: `bob_killer` may not import `tests`. `red` commits may not add non-test modules under `tests/` other than `conftest.py` and fixture data.
- **C3 (X4):** `assertions_frozen` also treats `tests/oracle/**` and `tests/oracle_values.py` as frozen in `green`/`refactor`. Any modification = violation `assertions_frozen/oracle-modified`.
- **C4 (X5):** Anchor the hook-log chain in git: each commit's trailer `Hooks-Log-Head: <sha256>` must equal the chain head at commit time. CI verifies that the chain head in commit N is a prefix-ancestor of the head in N+1. A full rewrite breaks earlier anchors already in history.
- **C5 (X6):** Changes to `scripts/gates*.py`, `scripts/check_*.py` or `tests/gates/**` require `TDD-Phase: gate-change` and a CODEOWNERS entry for `labs/bob_killer/scripts/` → @jtouley. CI labels these PRs `gate-change`.
- **C6 (X9):** `exact_parity` rejects any comparator signature with `*args`/`**kwargs`, any float default, and any module-level name matching `(?i)tol|eps|approx|margin|delta` in `verify/`.
- **C7 (X10):** When the computed range contains zero `TDD-Phase` commits but the diff touches `labs/bob_killer/src`, `tdd_order` fails with `tdd_order/empty-range`.

## Trade-offs

- C1 and C4 add plumbing to Phase 0, but they're the difference between the
  gates and decoration. The spec explicitly says gates are "enforced by a
  script or hook, not by instructions alone".
- C5 (CODEOWNERS) puts the human in the loop for gate edits. That's correct
  for a solo project that will be driven mostly by agents.
- A determined adversary with commit access can still forge everything. The
  goal is that every cheat leaves a reviewable diff, not cryptographic
  impossibility.

## core:* rule notes

- `core:dry-extension`: satisfied. One `@gate` registry, not N CLI shells.
- `core:standalone-package`: satisfied. The plan doesn't read host-global skill trees. The Pi-protocol `skill_gate` call uses `$CADENCE_HOME`.
- `core:decision-coverage`: satisfied by ADR-0001 (status proposed; names gh-5).

Verdict: APPROVE WITH CONDITIONS
