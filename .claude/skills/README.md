# Skills

Project skills, invoked as slash commands.

| Skill | Command | Purpose |
|-------|---------|---------|
| `spec-audit` | `/spec-audit <target>` | Pressure-test specification documents before implementation: trace system scenarios through the spec surface and publish a gap report |

## Layout convention

```
.claude/skills/
├── _shared/                  # Protocols shared across skills
│   └── ISSUE_TRACKER_GATE.md
└── <skill-name>/
    ├── SKILL.md              # Frontmatter (name, description) + workflow
    ├── references/           # Loaded on demand, not up front
    └── scripts/              # Executable gates and helpers
```

`SKILL.md` carries the workflow. Anything long, situational, or only needed
mid-run belongs in `references/` so it is read when relevant rather than
occupying context on every invocation.

## Placeholders to fill in per project

These skills ship generic. Replace before first real use:

| Placeholder | Where | What it should become |
|-------------|-------|-----------------------|
| `<TEAM-ID>` | `_shared/ISSUE_TRACKER_GATE.md`, `spec-audit/SKILL.md` | Your tracker's team identifier |
| `user-tracker` / `plugin-tracker` | same | The MCP server names for your issue tracker |
| `[a-z]+-[0-9]+` | same | Your branch ticket-ID pattern, if narrower |
| `~/.claude/scripts/manifest.py` | `spec-audit/SKILL.md` | Your artifact index script, or drop the register step |
| `docs/design-records/`, `docs/architecture/`, `.specs/` | `spec-audit/SKILL.md` | Wherever specs actually live here |
