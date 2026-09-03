#!/usr/bin/env python3
"""Structural gate for spec-audit reports.

Run before `manifest.py register`. Enforces the parts of the report that
downstream consumers (/plan-create, the manifest index, future sessions)
depend on, and the confidence rules that keep BLOCKING findings honest.

Usage:
    validate-report.py .context/spec-audit/<slug>_<date>.md

Exit codes:
    0  report is structurally valid (warnings may still be printed)
    1  report has errors
    2  bad invocation (missing or unreadable file)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REQUIRED_SECTIONS = (
    "## Sweep",
    "## Premise",
    "## Findings: BLOCKING",
    "## Findings: DEGRADED",
    "## Findings: COSMETIC",
    "## Dropped Findings",
)

REQUIRED_HEADER_FIELDS = ("**Ticket:**", "**Target:**")

# A BLOCKING row must point at something a reader can open.
EVIDENCE_PATTERNS = (
    re.compile(r"\S+\.\w+:\d+"),          # path:line
    re.compile(r"PR\s*#\d+", re.I),        # PR #2150
    re.compile(r"https?://\S+"),           # URL
)

BLOCKING_ID = re.compile(r"\bG-B\d+\b")
DROPPED_ID = re.compile(r"\bG-X\d+\b")


def section_body(text: str, heading: str) -> str:
    """Return the lines under `heading` up to the next same-or-higher heading."""
    lines = text.splitlines()
    depth = len(heading) - len(heading.lstrip("#"))
    out: list[str] = []
    collecting = False
    for line in lines:
        if line.strip() == heading:
            collecting = True
            continue
        if collecting:
            stripped = line.strip()
            if stripped.startswith("#"):
                this_depth = len(stripped) - len(stripped.lstrip("#"))
                if this_depth <= depth:
                    break
            out.append(line)
    return "\n".join(out)


def table_rows(body: str) -> list[str]:
    """Pipe rows of a markdown table, minus separator and blank rows.

    Header rows survive; callers filter by finding-ID pattern, which no
    header line matches.
    """
    rows = []
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if all(set(c) <= set("-: ") for c in cells):   # separator row
            continue
        if not any(cells):                              # empty row
            continue
        rows.append(stripped)
    return rows


def has_evidence(row: str) -> bool:
    return any(p.search(row) for p in EVIDENCE_PATTERNS)


def validate(path: Path) -> tuple[list[str], list[str]]:
    text = path.read_text(encoding="utf-8")
    errors: list[str] = []
    warnings: list[str] = []

    for field in REQUIRED_HEADER_FIELDS:
        if field not in text:
            errors.append(f"missing header field: {field}")

    for heading in REQUIRED_SECTIONS:
        if not re.search(rf"^{re.escape(heading)}\s*$", text, re.M):
            errors.append(f"missing required section: {heading}")

    # Addendum without a reconcile log means corrections were piled on
    # instead of reconciled in place.
    has_addendum = re.search(r"^## Addendum", text, re.M) is not None
    has_reconcile = re.search(r"^## Reconcile log", text, re.M) is not None
    if has_addendum and not has_reconcile:
        errors.append(
            "## Addendum present with no ## Reconcile log - corrections must be "
            "reconciled in place before an addendum is allowed"
        )
    if has_addendum:
        addenda = len(re.findall(r"^## Addendum", text, re.M))
        addenda += len(re.findall(r"^### Addendum", text, re.M))
        if addenda >= 3:
            warnings.append(
                f"{addenda} addenda - the report has been patched past the point "
                "of trust; rerun the audit instead"
            )

    # Every BLOCKING row needs [V] and an openable citation.
    blocking_body = section_body(text, "## Findings: BLOCKING")
    blocking_rows = [r for r in table_rows(blocking_body) if BLOCKING_ID.search(r)]
    for row in blocking_rows:
        finding_id = BLOCKING_ID.search(row).group(0)
        if "[V]" not in row:
            errors.append(f"{finding_id}: BLOCKING without [V] confidence tag")
        if not has_evidence(row):
            errors.append(
                f"{finding_id}: BLOCKING without evidence "
                "(needs a path:line, PR #N, or URL)"
            )

    # Findings tagged [U] should never have survived classification.
    for label in ("BLOCKING", "DEGRADED", "COSMETIC"):
        body = section_body(text, f"## Findings: {label}")
        for row in table_rows(body):
            if "[U]" in row:
                errors.append(f"[U] confidence in Findings: {label} - drop it instead")

    dropped_body = section_body(text, "## Dropped Findings")
    dropped_rows = [r for r in table_rows(dropped_body) if DROPPED_ID.search(r)]
    if not dropped_rows:
        warnings.append(
            "Dropped Findings table is empty - most honest runs drop at least "
            "one candidate; confirm nothing was silently promoted"
        )

    if not blocking_rows and not re.search(r"^### Prerequisites", text, re.M):
        warnings.append("no ### Prerequisites section for /plan-create")

    return errors, warnings


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__.strip(), file=sys.stderr)
        return 2

    path = Path(argv[1])
    if not path.is_file():
        print(f"error: no such report: {path}", file=sys.stderr)
        return 2

    errors, warnings = validate(path)

    for warning in warnings:
        print(f"warn:  {warning}")
    for error in errors:
        print(f"error: {error}", file=sys.stderr)

    if errors:
        print(f"\nFAIL {path} ({len(errors)} error(s))", file=sys.stderr)
        return 1

    print(f"\nOK {path}" + (f" ({len(warnings)} warning(s))" if warnings else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
