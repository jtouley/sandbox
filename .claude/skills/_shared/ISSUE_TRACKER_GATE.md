# Issue Tracker Gate

Shared protocol. Any skill that produces a persisted artifact resolves a
tracker ticket **before** writing that artifact, so the artifact and the
ticket can be cross-referenced later.

## MCP server priority

1. `user-tracker` — the user's own tracker connection
2. `plugin-tracker` — plugin-provided connection; requires an `mcp_auth` call
   with `{}` first
3. Ask the user manually — never invent a ticket ID

## Resolution order

Stop at the first step that yields a ticket ID.

1. **Branch name.** Ticket IDs are conventionally embedded in the branch:

   ```bash
   BRANCH=$(git rev-parse --abbrev-ref HEAD)
   echo "$BRANCH" | grep -oiE '[a-z]+-[0-9]+' | head -1 | tr '[:lower:]' '[:upper:]'
   ```

2. **Explicit flag or active session.** A `--ticket <ID>` flag on the
   invocation, or an active session file under `.context/sessions/*.json`.

3. **Search the tracker** for an in-progress ticket that matches this work:

   ```
   CallMcpTool(server="user-tracker", toolName="list_issues", arguments={
     "query": "<target scope or spec name>",
     "team": "<TEAM-ID>",
     "state": "started",
     "assignee": "me",
     "limit": 10
   })
   ```

4. **Create one** if nothing matches:

   ```
   CallMcpTool(server="user-tracker", toolName="save_issue", arguments={
     "title": "<type>(<scope>): <short description>",
     "description": "<what and why>\n\n**Triggered by:** /<skill-name>\n**Target:** <path or document>",
     "team": "<TEAM-ID>",
     "priority": 3,
     "state": "In Progress",
     "assignee": "me",
     "labels": ["agent-created"]
   })
   ```

Replace `<TEAM-ID>` with the team identifier for this project. Keep it in
project config rather than hardcoding it into a skill.

## Recording the ID

The resolved ticket ID must appear in both places:

- The artifact's header — `**Ticket:** <TICKET-ID>`
- The manifest registration tags

## Closing the loop

On completion, update ticket state to reflect the artifact's own verdict:

| Outcome | Ticket state |
|---------|--------------|
| Clean result, nothing blocking | Done |
| Blocking items found that need human resolution | leave In Progress, or move to Todo |

Never mark a ticket Done while the artifact it produced still lists blocking
items.
