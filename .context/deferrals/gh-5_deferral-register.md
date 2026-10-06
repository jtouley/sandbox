# Deferral register — gh-5

RUN_BY_SKILL: cadence
Ticket: #5

## Approved deferrals

No approved deferrals. Phase 0 acceptance criteria in #5 ship in-PR, including adversarial conditions C1–C7 and test-benchmark TB-1..TB-5.

## Deferrals rejected

| # | Item | Reason pushed back in-PR |
|---|---|---|
| 1 | Adversarial C4 (git-anchored hook log) | Proposed as "later hardening"; rejected — without it the append-only gate is decorative (X5). Ships in Phase B. |
| 2 | Adversarial C1 (junit cross-check) | Rejected as convenience deferral; ships in Phase B. |

## Out of scope (later SPEC phases, not deferrals of #5)

Golden direct-URL resolution (vibe-test G5) and the Excel oracle runner (G18) belong to SPEC Phase 1 and will get their own ticket when Phase 1 starts. They are explicitly out of scope for Phase 0 per SPEC.md roadmap.
