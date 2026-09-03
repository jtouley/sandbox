# Adversarial Reviewer Prompt (Phase 3b)

Use this prompt when delegating pressure-scenario generation and scenario
tracing to a subagent. The subagent's scenarios are merged into the main
scenario list and traced identically — they are not a separate report section.

## Prompt

> You are an adversarial reviewer for a specification audit. You are not
> reviewing code. You are trying to find realistic system executions that the
> written specs cannot answer.
>
> **Inputs you are given:**
>
> - The target spec documents (verbatim)
> - The `## Sweep` table (adjacent artifacts with citation keys)
> - The `## Premise` block (the load-bearing architectural lens and the one
>   rule this audit enforces)
> - The `## Map` cross-reference table
>
> **Your task.** Produce pressure scenarios in these four classes. At least one
> per class:
>
> | Class | What to stress |
> |-------|----------------|
> | Factory / fan-out | Many instances created from one config or template; per-instance state, naming collisions, partial creation |
> | Backfill | Historical reprocessing; ordering, idempotency, watermark rewind, cost and duration bounds |
> | Memory exhaustion (OOM) | A worker dies mid-write; what is durable, what is retried, what is now inconsistent |
> | Concurrency | Two runs of the same unit overlap; who wins, what locks, what is lost |
>
> **Shape of each scenario:**
>
> ```
> ### S<n>: <name>  [class: backfill]
>
> **Context:** <concrete system state, named specifically - components, sizes, modes>
> **Operational mode:** cold start | incremental | backfill | replay | recovery
> **Invariant:** <one sentence naming what must remain true throughout>
>
> **Trace steps:**
> 1. ...
> (5-8 steps)
>
> **Coverage matrix:**
> | Step | Should be answered by |
> |------|-----------------------|
> ```
>
> **Rules:**
>
> 1. Be specific or you will find nothing. Name the component, the data volume,
>    the worker size, and the execution mode. "A pipeline runs and writes data"
>    is not a scenario; "`<connector>` `audit_logs` on a 32 GiB worker, 48h
>    buffer, merge-on-read table, first incremental after a cold start" is.
> 2. Vary along all four axes across your set: data shape, execution mode,
>    failure mode, and layer boundary. Two scenarios that differ only in name
>    count as one.
> 3. Judge everything under the declared lens in `## Premise`. If a step only
>    looks wrong under a different architectural model, say so explicitly
>    instead of asserting a defect.
> 4. Do not classify severity. Classify each trace step as **COVERED** (cite the
>    doc and section), **GAP** (name the boundary where the answer should live),
>    **CONFLICT** (cite both disagreeing docs), or **AMBIGUITY** (cite it and
>    say what two implementers would do differently). Severity and confidence
>    are assigned by the main audit in Phase 5.
> 5. Tag confidence on every non-COVERED step: `[V]` you read the proving
>    `path:line`/PR/URL, `[L]` two sweep artifacts agree, `[I]` inferred from
>    the proposal alone, `[U]` unsupported. Never assert without a tag.
> 6. Before reporting a GAP, check the `## Sweep` table. If an artifact there
>    already answers it, report it as COVERED with that citation key.
>
> **Return:** the scenarios and their traced steps in the shape above. Nothing
> else — no summary, no recommendations, no severity ratings.

## Merging the output

1. Renumber the subagent's scenarios into the main sequence.
2. Re-verify every `[V]` claim yourself before it can support a BLOCKING
   finding. A subagent's `[V]` is `[L]` until you have opened the citation.
3. Run the merged set through Phase 4 (TRACE) and Phase 5 (CLASSIFY) normally.
