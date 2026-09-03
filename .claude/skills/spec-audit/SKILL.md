---
name: spec-audit
description: >
  Pressure-test specification documents before implementation -- find gaps in
  design records, stress-test design docs, check spec coherence, and do
  exploratory validation. Use on "spec audit", "pressure test the specs",
  "validate specs before implementing", "find gaps in the design record",
  "stress-test this design", "check spec coherence", "are these specs solid",
  or when the user wants to validate specs before creating an execution plan.
  Best used before /plan-create and /spec-driven in the development workflow.
---

# Spec Audit

## Execution Directives

Take your time and go step by step in your review and analysis. Available
commands: see `~/.claude/commands/` for reusable workflows.

## Overview

A spec audit validates specification documents by tracing system-level scenarios
through the spec surface, cross-referencing decisions across documents, and
flagging gaps, conflicts, and ambiguities that would block or derail
implementation. The output feeds directly into `/plan-create` as prerequisites
and risk context.

**Core principle:** If a realistic system scenario cannot be fully traced through
the specs, the specs are incomplete — and the implementation plan will hit the
same wall.

**Artifact-first principle:** An audit that lives only in an assistant message
is worth nothing twenty turns later. The report file at `.context/spec-audit/` is
the deliverable; the inline summary is only a pointer to it. This is
non-negotiable for a specific reason: the report is consumed by `/plan-create`, by
future sessions recovering context after compaction, by the manifest index, and
by the user scanning prior gap reports to avoid re-doing work. None of those
downstream uses can read an assistant message. So: the first substantive tool
call in any `/spec-audit` invocation is `Write` creating the report file (even as
a stub that gets filled in across phases), and the final step is
`manifest.py register`.

Do not rationalize around this rule. Common rationalizations that are all wrong:

- *"This is just scoping / strategy, not a ticketed task"* — scoping and strategy
  analyses are the ones with the highest context-recovery value later. They MUST
  persist.
- *"The user asked a conceptual question, not for a report"* — the `/spec-audit`
  invocation IS the request for a report. The conversational framing is
  orthogonal to the deliverable.
- *"It's small, inline is fine"* — small reports are the cheapest to write. Size
  is never a reason to skip persistence.
- *"The target isn't a spec directory, it's an idea"* — specs can be ideas,
  papers, external systems, or the harness itself. Invent a sensible
  `{target-scope}` slug and write the file.

If you find yourself about to write "I'll do this in-message" or "without
creating an artifact" in a `/spec-audit` turn, stop and write the stub file
first. You can always refine the content; you cannot retroactively create
persistence from a message that the user closed.

**Target-validity gate (run before STUB):**

An audit is only as good as the thing it tests. Ephemeral prose produced
in-session is not a valid target — it cannot be re-read, diffed, or cited, and
the audit will slowly drift from what was actually proposed. Before writing the
stub, classify the target:

| Target type | Action |
|-------------|--------|
| Persisted spec on disk (design record, spec doc, design-spec .md) | Proceed to STUB. |
| In-flight PR with a description | Treat the PR body + diff as the target. Pin the commit SHA in the report. |
| In-session proposal, chat thread, review paste | Snapshot the proposal verbatim into the report's "Proposal under test" section, OR write it to `.context/proposals/{slug}.md` and retarget. Do not audit free-floating prose. |
| Idea with no text yet | Refuse. Ask the user to write a one-page proposal first. |

When in doubt, snapshot. A frozen 300-line verbatim block is a valid target;
a vague summary of "what the user meant" is not.

**Workflow position:**

```
Specs/design records written
     |
/spec-audit <target-dir>      <- THIS SKILL (exploratory validation)
     | (gap report)
/plan-create <ticket>          <- Uses gap report as input context
     | (execution plan)
/spec-driven                   <- Executes with TDD
```

## When to Use

- Spec docs exist but implementation has not started — validate before planning
- After major spec changes — regression-test for new cross-doc gaps
- Before `/plan-create` — find blocking gaps that would stall execution
- Specs span multiple documents — test cross-doc contract coherence
- After a staff review or architecture review — verify findings against spec surface
- A new design record references or depends on prior design records — verify alignment

**When NOT to use:**

- Single-file specs with obvious scope — just review manually
- Implementation bugs — use actual tests via `/spec-driven`
- API contract validation — use schema validation tools
- The specs have already been implemented — test the code, not the docs

## Issue Tracker Gate (mandatory)

Before producing any artifacts, ensure a tracker ticket exists for this work.
Follow the protocol in `.claude/skills/_shared/ISSUE_TRACKER_GATE.md`.

**MCP server priority:** `user-tracker` first, then `plugin-tracker`
(requires `mcp_auth` call with `{}` first), then ask the user manually.

1. Check branch name for a ticket ID (case-insensitive):
   `echo "$BRANCH" | grep -oiE '[a-z]+-[0-9]+' | head -1 | tr '[:lower:]' '[:upper:]'`
2. Check `--ticket` flag or active session (`.context/sessions/*.json`)
3. Search the tracker for a matching in-progress ticket:

   ```
   CallMcpTool(server="user-tracker", toolName="list_issues", arguments={
     "query": "<target scope or spec name>",
     "team": "<TEAM-ID>",
     "state": "started",
     "assignee": "me",
     "limit": 10
   })
   ```

4. If no match, create one:

   ```
   CallMcpTool(server="user-tracker", toolName="save_issue", arguments={
     "title": "chore(specs): spec-audit <target-scope>",
     "description": "Spec audit validation of <target-scope> specs.\n\n**Triggered by:** /spec-audit\n**Target:** <spec directory or document>",
     "team": "<TEAM-ID>",
     "priority": 3,
     "state": "In Progress",
     "assignee": "me",
     "labels": ["agent-created"]
   })
   ```

5. Record the ticket ID. It must appear in:
   - The report file header (`**Ticket:** <TICKET-ID>`)
   - The manifest registration tags

On completion, update the ticket state:
- Clean report (no BLOCKING gaps) -> mark Done
- BLOCKING gaps found -> leave as In Progress or move to Todo (needs resolution)

## Core Method

```
0.   STUB      - Create the report file at .context/spec-audit/{slug}_{date}.md
                 with headers + placeholders. First substantive tool call.
0.5. SWEEP     - Enumerate adjacent artifacts that likely already answer the
                 question: candidate design records, in-flight PRs, CLAUDE.md/AGENTS.md
                 sections, internal explainers, prior audits.
1.   GATHER    - Read all spec docs in the target scope AND the sweep set;
                 fill GATHER section.
1.5. PREMISE   - Name the project's load-bearing architectural lens (CP/DP,
                 medallion, hexagonal, DDD subdomains, etc.) and cite its
                 source doc. Every finding is later tested against it.
2.   MAP       - Build cross-reference map; fill MAP section.
3.   SCENARIOS - Write 3-5 system scenarios; fill SCENARIOS section.
4.   TRACE     - Trace each scenario against specs; fill TRACE classifications.
5.   CLASSIFY  - Rate findings BLOCKING / DEGRADED / COSMETIC with confidence
                 tags [V]/[L]/[I]/[U]. BLOCKING requires [V]. Findings below
                 [L] move to the Dropped Findings section, not the main tables.
5.5. PRIOR ART - For each candidate BLOCKER/DEGRADED, answer it from the
                 SWEEP artifacts first. If the answer already exists, the
                 finding is COVERED - not a gap. Move it to Dropped with a
                 citation to the resolving artifact.
6.   RECONCILE - Publish the report. On new evidence or user correction,
                 edit the original tables in place (strike-through + new
                 classification). Addenda are reserved for genuinely new
                 information that arrived after publication.
7.   REGISTER  - Run manifest.py register. Update ticket state. Summarize inline by linking to the file.
```

The file exists from step 0 onward. This is the same pattern as write-ahead
logs: commit the intent to disk before doing the work, so a crash (or a
premature "I'll do this in-message") cannot lose it.

## Phase 0.5: SWEEP (adjacent artifacts)

Before reading the target spec, enumerate everything the project has already
written about the topic. A large fraction of "BLOCKING gaps" are not gaps at
all — they are answers the proposal failed to cite. Finding those answers up
front is cheaper than discovering them via three rounds of user correction.

**Required sweeps (all that apply):**

| Sweep | Command or source | What you are looking for |
|-------|-------------------|--------------------------|
| Design record enumeration | `ls docs/design-records/ docs/adr/ docs/rfc/ 2>/dev/null` + keyword grep on titles and bodies | Records covering the same decision, contract, or component |
| In-flight PRs | `pr list --state open --search "<keywords>"` and `pr list --state merged --search "<keywords>" --limit 20` | Changes already in progress or recently shipped that move the system toward (or away from) the proposal |
| House-model docs | `CLAUDE.md`, `AGENTS.md`, top-level `README.md`, `docs/architecture/` | The project's stated architectural premises — these feed Phase 1.5 |
| Internal explainers | `docs/notes/` / `docs/internal/` / `.context/refs/` | Concept-level prose that defines terms the proposal uses (e.g., CP/DP, medallion, MVO) |
| Prior audits | `ls .context/spec-audit/` | Previous sweeps of overlapping scope |
| Scalar / schema search | `rg -l "<symbol>" schemas services apps src` | Whether the thing the proposal claims is missing already exists in code |

**Output of this phase (written into the report under `## Sweep`):**

```markdown
## Sweep (adjacent artifacts)

| Source | Cited as | Relevance |
|--------|----------|-----------|
| DR-0012 §Phase 3 | [DR-0012] | Defines catalogue ownership; directly relevant to candidate BLOCKER G-B1 |
| PR #2150 (open, 2d ago) | [PR-2150] | Implements the contract CI enforcement G-B3 claimed missing |
| CLAUDE.md §"Shared enums live in" | [CLAUDE] | Governs whether a new enum goes in `enums/` vs API schema |
| docs/architecture/control-plane-data-plane.md | [CP-DP] | Names the load-bearing lens; feeds Phase 1.5 |
```

Every entry in this table gets a short-form citation key (`[DR-0012]`,
`[PR-2150]`). Later phases cite by key, which keeps the Prior Art Check
(Phase 5.5) auditable.

**Stop rule:** Spend at most 10 minutes on the sweep. If you cannot find a
relevant artifact after three searches, the topic is genuinely uncovered — note
that explicitly in the report.

## Phase 1: GATHER

Read all specification documents in the target scope. Spec docs include:

- Design records (ADR / EDR / RFC) — `docs/design-records/`, `docs/adr/`, `docs/rfc/`
- Staff reviews — review documents at repo root or `.context/`
- Design specs — `.specs/` domain and global specs
- Runbooks — `docs/runbooks/`
- README files with architectural context

For each document, extract:

| Field | What to capture |
|-------|-----------------|
| **Decisions** | Explicit "We will..." statements with rationale |
| **Contracts** | Cross-boundary agreements (input/output formats, ownership boundaries, field semantics) |
| **Assumptions** | Stated conditions that must hold for decisions to remain valid |
| **Deferred items** | Explicitly punted decisions ("out of scope", "future work", "deferred until") |
| **References** | Which other docs this doc cites or depends on |

## Phase 1.5: PREMISE (architectural lens)

A proposal, a design record, and a staff review can each be internally consistent and
still disagree about what shape the system is. Before classifying anything,
name the project's load-bearing architectural lens — the one the proposal
will be judged against. Without this step, findings get asserted from
whatever lens the reviewer happens to be carrying that day.

**Procedure:**

1. From the SWEEP results (`CLAUDE.md`, explainers, top-level architecture
   docs), identify the primary lens. Common house models:

   | Lens | Characteristic phrases |
   |------|------------------------|
   | Control Plane / Data Plane | "desired state vs observed state", "CP owns inventory, DP executes", "derive don't duplicate" |
   | Medallion (Bronze/Silver/Gold) | "layered transformation", "catalog-driven promotion", "contract at the layer boundary" |
   | Hexagonal / Ports & Adapters | "domain is inside", "adapters at the edge", "ports are interfaces" |
   | DDD strategic | "bounded context", "core/supporting/generic subdomain", "ubiquitous language" |
   | Schema-first / codegen | "the schema is the source of truth", "generate types, SDKs, SQL from one file" |

2. Write the declaration into the report:

   ```markdown
   ## Premise

   **Load-bearing lens:** Control Plane / Data Plane
   **Source:** `docs/architecture/control-plane-data-plane.md`
   **Core rule this audit will enforce:**
   > CP owns desired state and inventory; DP derives observed state. Findings
   > that assume DP-originated authoritative state are suspect and must cite
   > an explicit exception.
   ```

3. If the proposal uses a *different* lens than the house model (e.g.
   introduces DDD subdomain terms in a CP/DP shop), flag that mismatch as its
   own finding in the report. Do not silently re-interpret one into the other.

**Usage in later phases:**

Every candidate BLOCKER must be restated under the declared lens before it is
accepted. If a finding reads naturally under lens A but evaporates under the
declared lens B, it is an artifact of the reviewer's lens — move it to
Dropped Findings with that reason.

## Phase 2: MAP (cross-reference map)

Build the map that later phases trace against. Three passes:

1. **Contract trace.** For each contract found in GATHER, follow it across
   every doc that touches it and mark the relationship: **aligned** (docs agree),
   **tension** (docs differ in emphasis or defaults but can be reconciled), or
   **conflict** (docs cannot both be true).
2. **Assumption check.** Check each assumption against the *decisions* in other
   docs. An assumption that a decision elsewhere already invalidated is a
   finding, not an assumption.
3. **Ownership at layer boundaries.** For each boundary the target scope
   crosses, answer: what crosses it, who validates it, and what happens when
   the upstream side changes.

**Output (written into the report under `## Map`):**

```markdown
## Map (cross-reference)

| Contract | Docs touching it | Status | Note |
|----------|------------------|--------|------|
| Catalogue ownership | DR-0012 §3, DR-0021 §2, CLAUDE.md | tension | DR-0021 defaults differ; reconcilable |
| Observed-state write path | DR-0012 §5, proposal §4 | conflict | Proposal has DP writing desired state |

| Boundary | What crosses | Who validates | On upstream change |
|----------|--------------|---------------|--------------------|
| CP -> DP | desired-state manifest | DP admission check | DP rejects unknown fields |
```

In **Interactive Mode**, pause here and show the map before continuing.

## Phase 3: SCENARIOS

Write 3-5 system-level scenarios. A scenario is an execution trace, not a user
persona. It must vary along at least these axes across the set: **data shape**,
**execution mode**, **failure mode**, and **layer boundary**.

Each scenario has:

- **Context** — the concrete system state, named specifically
- **Operational mode** — cold start, incremental, backfill, replay, recovery
- **Invariant** — one sentence naming what must remain true throughout
- **Trace steps** — 5-8 numbered steps
- **Coverage matrix** — which spec docs each step should be answered by

A scenario is specific or it finds nothing:

> **Good:** "`<connector>` `audit_logs` on a 32 GiB worker, 48h buffer,
> merge-on-read table, first incremental run after a cold start."
>
> **Bad:** "a pipeline runs and writes some data."

**Phase 3b: adversarial pressure scenarios.** Spawn an adversarial reviewer
subagent (prompt: `references/simulator-prompt.md`) to generate pressure
scenarios in these classes: **factory / fan-out**, **backfill**, **memory
exhaustion (OOM)**, and **concurrency**. Merge its scenarios into the same
list — they are traced identically, not kept separate.

In **Interactive Mode**, pause here and show the proposed scenarios before
tracing so the user can edit them.

## Phase 4: TRACE

Walk each scenario step against the spec surface. Every step gets exactly one
classification:

| Classification | Meaning | Required evidence |
|----------------|---------|-------------------|
| **COVERED** | The specs answer this step | Cite the doc and section |
| **GAP** | Nobody answers this step | Name the boundary where the answer should live |
| **CONFLICT** | Two docs give answers that disagree | Cite both |
| **AMBIGUITY** | Answered, but too mushy to implement against | Cite it and say what two implementers would do differently |

Do not soften a GAP into an AMBIGUITY to avoid writing a finding, and do not
promote an AMBIGUITY to a GAP for emphasis.

A spec doc that no scenario exercises is either a blind spot in your scenario
set or dead specification. Say which.

## Phase 5: CLASSIFY

Rate each finding for severity, then gate that severity on confidence.
Confidence tags:

| Tag | Meaning |
|-----|---------|
| `[V]` | Verified — you read the `path:line`, PR, or doc section that proves it |
| `[L]` | Likely — two independent sweep artifacts agree |
| `[I]` | Inferred — proposal-internal reasoning only |
| `[U]` | Unverified — assertion with no supporting artifact |

Severity is gated on confidence. This is the rule that kills the worst failure
mode — the confident, wrong blocker:

| Severity | Minimum confidence | Otherwise |
|----------|--------------------|-----------|
| **BLOCKING** | `[V]` — you read a `path:line` or PR | Downgrade or drop |
| **DEGRADED** | `[L]` — two sweep artifacts agree | Downgrade or drop |
| **COSMETIC** | `[I]` — proposal-internal reasoning | Drop if `[U]` |

A BLOCKING finding without a citation is a process failure, not a finding.

Severity definitions:

- **BLOCKING** — implementation cannot proceed correctly without resolving this
- **DEGRADED** — implementation can proceed, but ships a known weakness
- **COSMETIC** — wording, naming, or doc-hygiene issue with no execution impact

Finding IDs: `G-B*` (blocking), `G-D*` (degraded), `G-C*` (cosmetic),
`G-X*` (dropped).

## Phase 5.5: PRIOR ART (check findings against the sweep)

Walk every remaining BLOCKER and DEGRADED finding against the Sweep table
again. If a design record, an in-flight PR, or the declared lens already answers
it, the finding is COVERED — move it to Dropped Findings with a citation to
the resolving artifact.

This is a separate phase on purpose. The original failure mode was "we
gathered the right artifacts and never checked the findings against them."
Gathering is Phase 0.5; checking is here.

## Phase 6: RECONCILE (publish and correct in place)

Fill the report in place. Then:

- **New evidence or user correction** -> edit the original table row:
  strike through the old classification, write the new one, and append a line
  to the `## Reconcile log` with the reason and the citation.
- **`## Addendum`** is reserved for information that arrived *after* a
  downstream consumer already quoted the report. A fresh report has no
  addendum.
- **Three addenda means rerun, not patch.** At that point the report's
  reasoning has been replaced piecemeal; start a new audit.

## Phase 7: VALIDATE + REGISTER

1. Run `scripts/validate-report.py <report-path>`. It must pass.
2. Run `manifest.py register` (see `~/.claude/scripts/manifest.py`) to index the
   report for later sessions, tagged with the ticket ID.
3. Update the ticket: Done if no BLOCKING gaps; leave In Progress / Todo if
   blockers remain.
4. Summarize inline by **linking to the file** — do not paste the report body
   into the message.

## Running a Spec Audit

### Interactive Mode (Default)

Runs the phases in order, pausing twice for user input:

- after Phase 2 (MAP) — show the cross-reference map for correction
- after Phase 3 (SCENARIOS) — show proposed scenarios for editing

### Automated / Subagent Mode

Generates and traces without the pauses. Use when the audit is one step in a
larger chain (e.g. invoked by `/plan-create`) or when delegating scenario
tracing to a subagent via `references/simulator-prompt.md`.

## Report Structure

The report at `.context/spec-audit/{slug}_{date}.md`:

```markdown
# Spec Audit: <target-scope>

**Ticket:** <TICKET-ID>
**Date:** <YYYY-MM-DD>
**Target:** <spec directory, document, or PR + pinned SHA>
**Mode:** interactive | automated

## Proposal under test
<verbatim snapshot, only when the target was in-session prose>

## Sweep (adjacent artifacts)
## Premise
## Gather
## Map (cross-reference)
## Scenarios
## Trace

## Findings: BLOCKING
| ID | Finding | Confidence | Evidence | Scenario |
|----|---------|-----------|----------|----------|

## Findings: DEGRADED
## Findings: COSMETIC

## Dropped Findings
| ID | Candidate finding | Why dropped | Resolving artifact |
|----|-------------------|-------------|--------------------|

## Reconcile log

## For /plan-create
### Prerequisites
### Risk context
### Suggested phase order
### Assumption ledger
```

## What the Report Is For

The last section is written for `/plan-create`:

- **Prerequisites** — BLOCKING gaps become Phase 0 of the plan
- **Risk context** — DEGRADED gaps become mitigations or required tests
- **Suggested phase order** — e.g. "resolve the DR-X contract tension before
  implementing the feature that depends on it"
- **Assumption ledger** — every assumption already checked, so `/plan-create`
  does not re-derive them

## Where Things Live

| Path | Role |
|------|------|
| `.claude/skills/spec-audit/SKILL.md` | Workflow (this file) |
| `.claude/skills/spec-audit/scripts/validate-report.py` | Structural gate before manifest register |
| `.claude/skills/spec-audit/references/failure-modes.md` | Why the hardening exists |
| `.claude/skills/spec-audit/references/simulator-prompt.md` | Prompt for delegating traces to a subagent |
| `.claude/skills/_shared/ISSUE_TRACKER_GATE.md` | Ticket gate protocol |
| `.context/spec-audit/` | Report files (the actual product) |
| `~/.claude/scripts/manifest.py` | Indexes reports for later sessions |

The validator requires `## Sweep`, `## Premise`, and the four findings tables
(BLOCKING / DEGRADED / COSMETIC / Dropped Findings). Every `G-B*` row must
contain `[V]` and a `path:line`, `PR #N`, or URL. A fresh report with
`## Addendum` and no Reconcile log fails. An empty Dropped table is a
warning — most honest runs drop at least one candidate.

## Gotchas (why the skill is this rigid)

An early cross-service-architecture run produced three addenda and collapsed
four BLOCKERs down to one DEGRADED after corrections. About 40% of the document
became apology text. The hardening is that case study turned into process:

- Treating chat prose as a spec -> **target-validity gate**
- "Missing owner / missing CI / new layer" that already existed in a design
  record or open PR -> **Sweep + Prior Art**
- BLOCKING asserted from the proposal alone -> `[V]` + `path:line` or it does
  not publish
- DDD language applied in a CP/DP shop with no declared lens -> **Premise**
- Corrections piled on as addenda -> **Reconcile in place**

Repeated misreadings this skill now names:

- "new layer" that is actually a sub-namespace
- "anti-corruption layer" that is actually Conformist
- "needs an owner and a bus" for a value that is a hash of existing state
- "CI is missing" when a PR already ships it
- "undefined contract" that already lives in `schemas/`

Other sharp edges:

- Scenarios that say "a pipeline" instead of naming connector, worker size, and
  mode find nothing
- Happy-path-only scenarios miss the interesting contracts
- A spec doc that no scenario exercises is either a blind spot or dead
  specification
