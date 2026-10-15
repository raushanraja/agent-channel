#!/usr/bin/env bash
# Two `opencode run` agents (different models) collaborate over chan using the
# MCP server (chan-mcp), i.e. native chan_* tools instead of the CLI.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/chan/target/debug"
[ -x "$BIN/chan-mcp" ] || { echo "build first: (cd $REPO/chan && cargo build --workspace)"; exit 1; }
command -v opencode >/dev/null || { echo "need the opencode CLI on PATH"; exit 1; }

DEMO=$(mktemp -d /tmp/opencode/mcp.XXXXXX)
SHARED="$DEMO/shared/.chan"
M1="${W1_MODEL:-opencode/deepseek-v4.1-flash-free}"
M2="${W2_MODEL:-opencode-go/deepseek-v4.1-flash}"

echo "demo dir : $DEMO"
echo "channel  : $SHARED   (one local log; each agent's chan-mcp attaches to it)"
echo "workers  : w1=$M1  w2=$M2"

"$BIN/chan" --dir "$SHARED" init

# Per-agent project dir with its own opencode.json exposing the chan MCP server.
mkproject() {
  local name="$1"
  mkdir -p "$DEMO/$name"
  cat > "$DEMO/$name/opencode.json" <<EOF
{
  "\$schema": "https://opencode.ai/config.json",
  "mcp": {
    "chan": {
      "type": "local",
      "command": ["$BIN/chan-mcp", "--dir", "$SHARED", "--as", "$name"],
      "enabled": true
    }
  }
}
EOF
  cat > "$DEMO/$name/brief.md" <<EOF
# chan collaboration — you are agent "$name"

You are one of several agents collaborating over a message bus called \`chan\`.
chan is available to you as **MCP tools** (names such as \`chan_subscribe\`,
\`chan_publish\`, \`chan_await\`, \`chan_send\`). Your identity is "$name" and the
tools already default to it. Use the MCP tools — do NOT shell out to a CLI.

Do this, in order:

1. Subscribe to the "team" room: call chan_subscribe with room "team".
2. Announce yourself: chan_publish with room "team", kind "event", payload {"join":"$name"}.
3. Wait for your task: chan_await with kind "task" and timeout_ms 400000.
   It returns JSON whose payload looks like {"n": <number>, "go": "square"}.
4. Compute value = n * n.
5. Send the result: chan_send with to "direct:orch", kind "result",
   payload {"name":"$name","value":<THE_SQUARE>}.
6. Announce completion: chan_publish with room "team", kind "event", payload {"done":"$name"}.

Then stop and report the exact result JSON you sent.
EOF
}
mkproject w1
mkproject w2

echo; echo "== launching two opencode run agents (MCP attach) =="
( cd "$DEMO/w1" && opencode run --model "$M1" --auto --file brief.md \
    "Follow the brief using the chan MCP tools. Report your result JSON when done." \
    > "$DEMO/w1.log" 2>&1; echo "W1_EXIT=$?" >> "$DEMO/w1.log" ) & P1=$!
( cd "$DEMO/w2" && opencode run --model "$M2" --auto --file brief.md \
    "Follow the brief using the chan MCP tools. Report your result JSON when done." \
    > "$DEMO/w2.log" 2>&1; echo "W2_EXIT=$?" >> "$DEMO/w2.log" ) & P2=$!

ORCH="$BIN/chan --dir $SHARED"
$ORCH subscribe --as orch --room team >/dev/null
sleep 5

echo; echo "== orchestrator dispatches tasks (via CLI, same channel log) =="
$ORCH send --from orch --to direct:w1 --kind task --payload '{"n":7,"go":"square"}'
$ORCH send --from orch --to direct:w2 --kind task --payload '{"n":9,"go":"square"}'

echo; echo "== orchestrator awaits results (blocking on the channel) =="
echo -n "result 1: "; $ORCH await --as orch --kind result --timeout 400s
echo -n "result 2: "; $ORCH await --as orch --kind result --timeout 400s

echo; echo "== shared room trace (pub/sub) =="
$ORCH room --as orch --room team

echo; echo "== agents used MCP tools? (grep logs) =="
for f in w1 w2; do echo "-- $f --"; grep -oE "chan_(subscribe|publish|await|send)[a-z_]*" "$DEMO/$f.log" | sort | uniq -c || echo "   (none)"; done

echo; echo "== waiting for agents =="
wait "$P1"; wait "$P2"
echo "== w1 log tail =="; tail -n 8 "$DEMO/w1.log"
echo "== w2 log tail =="; tail -n 8 "$DEMO/w2.log"
echo; echo "demo dir kept: $DEMO"
