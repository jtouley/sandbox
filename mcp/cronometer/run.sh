#!/usr/bin/env bash
# Launches Pop101/cronometer-mcp (stdio MCP server) at a pinned, reviewed commit.
# Credentials come from CRONOMETER_USERNAME / CRONOMETER_PASSWORD in the environment
# (set them as environment variables in the cloud environment settings, never in git).
set -euo pipefail

REPO_URL="https://github.com/Pop101/cronometer-mcp"
COMMIT="34f7e4d846b4e70875dd58daea55a8cdfa30a108"
SRC_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/cronometer-mcp/$COMMIT"

if [ ! -f "$SRC_DIR/cronometer_mcp_server.py" ]; then
  rm -rf "$SRC_DIR"
  mkdir -p "$SRC_DIR"
  git -C "$SRC_DIR" init -q
  git -C "$SRC_DIR" fetch -q --depth 1 "$REPO_URL" "$COMMIT" >&2
  git -C "$SRC_DIR" checkout -q FETCH_HEAD
fi

# The server imports mcp.server.fastmcp (the official `mcp` SDK), not the `fastmcp` package
# its pyproject lists, and its console-script entry point is broken, so run the file directly.
exec uv run --quiet --no-project --python 3.12 \
  --with 'httpx>=0.27' --with 'mcp>=1.2,<2' \
  python "$SRC_DIR/cronometer_mcp_server.py"
