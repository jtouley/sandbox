# Spec Audit: Failure Modes

Why the skill is rigid. Each rule below exists because an audit shipped a
finding that wasted a reviewer's time, and the fix was turned into process.

## The case study

An early cross-service-architecture audit produced three addenda and collapsed
four BLOCKERs down to a single DEGRADED after corrections. Roughly 40% of the
published document was apology text. Every rule in this file traces back to it.

## Failure mode 1: chat prose as the target

**Symptom.** The audit tests a summary of what the user said, not a document.
Two turns later nobody can diff the audit against the proposal, because the
proposal never existed as text.

**Rule.** The target-validity gate. A persisted spec or an in-flight PR (with a
pinned SHA) is a valid target. In-session prose must be snapshotted verbatim
into the report or written to `.context/proposals/{slug}.md` first. An idea with
no text is refused.

## Failure mode 2: gaps that were already answered

**Symptom.** "No owner is defined", "there is no CI enforcement", "this needs a
new layer" — each already answered by a design record, an open PR, or the house
model doc. The proposal simply failed to cite them.

**Rule.** Two phases, not one. Phase 0.5 SWEEP gathers adjacent artifacts and
gives each a citation key. Phase 5.5 PRIOR ART walks every surviving
BLOCKER/DEGRADED back against that table. Gathering the artifacts and never
checking findings against them was the original bug.

## Failure mode 3: confident, uncited blockers

**Symptom.** A BLOCKING finding derived entirely from reading the proposal.
It reads authoritative and is wrong, and it lands in `/plan-create` as a
Phase 0 prerequisite.

**Rule.** Severity is gated on confidence. BLOCKING requires `[V]` — a
`path:line`, a PR, or a URL you actually opened. DEGRADED requires `[L]` — two
sweep artifacts agreeing. `[U]` findings are dropped, not softened. The
validator enforces this; a BLOCKING row without `[V]` and a citation does not
publish.

## Failure mode 4: lens mismatch asserted as a defect

**Symptom.** DDD vocabulary ("anti-corruption layer", "bounded context")
applied to a project whose stated model is Control Plane / Data Plane. The
finding is coherent under the reviewer's lens and meaningless under the
project's.

**Rule.** Phase 1.5 PREMISE. Name the load-bearing lens and cite its source doc
before classifying anything. Restate every candidate BLOCKER under that lens;
if it evaporates, it was an artifact of the reviewer's lens and moves to
Dropped Findings. A genuine lens mismatch is itself a finding — but it is filed
as one, not smuggled in as five downstream defects.

## Failure mode 5: corrections as addenda

**Symptom.** The tables still assert the original wrong classifications, and
three addenda at the bottom explain why each is wrong. Readers who stop at the
tables get the wrong answer.

**Rule.** Reconcile in place. Strike through the old classification in the
original row, write the new one, and log the reason plus citation in
`## Reconcile log`. `## Addendum` is only for information that arrived after a
downstream consumer already quoted the report. Three addenda means rerun.

## Failure mode 6: findings that live only in a message

**Symptom.** A thorough audit summarized in chat. Twenty turns later the
context is compacted and the work is gone; `/plan-create` re-derives it badly.

**Rule.** Artifact-first. The first substantive tool call is `Write` creating
the stub; the last is `manifest.py register`. Size, framing, and "this is just
scoping" are not exemptions.

## Repeated misreadings this skill names

| Claim | What it usually is |
|-------|--------------------|
| "introduces a new layer" | a sub-namespace inside an existing layer |
| "needs an anti-corruption layer" | a Conformist relationship, already chosen |
| "this value needs an owner and a bus" | a hash of state that already has an owner |
| "CI enforcement is missing" | already shipped in an open or recently merged PR |
| "the contract is undefined" | defined in `schemas/` |

## Scenario smells

- "A pipeline runs" — no connector, no worker size, no mode. Finds nothing.
- Happy path only — the interesting contracts are all on failure and boundary
  paths.
- A spec doc no scenario exercises — either a blind spot in the scenario set or
  dead specification. Say which; do not leave it unmentioned.
