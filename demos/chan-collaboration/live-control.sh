#!/usr/bin/env bash
# Live control — one MAIN agent controls two other sessions, and they post back.
#
# Unlike the other demos (fire-and-forget `opencode run` agents), this drives
# *running sessions* on a dedicated `opencode serve`, using chan's live tools
# (`chan_active` / `chan_inject`). The orchestrator then re-controls a finished
# worker (a "steer") to show the session can keep being driven.
#
# Two gotchas it handles for you:
#   1. The account's default model can be dead (e.g. an OpenRouter key with no
#      credits), which kills every session. The model is therefore pinned in the
#      server config. Override with MPROV/MMODEL.
#   2. opencode does NOT load a session's own directory `opencode.json` for
#      API-created sessions, so the chan MCP is injected into the *server* config
#      (OPENCODE_CONFIG_CONTENT) instead. All sessions share one chan identity
#      ("chan"); payloads carry a `name` field to tell agents apart.
#
# Your global opencode config is never touched, and nothing is left running.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/chan/target/debug"
CURL="${CURL:-/usr/bin/curl}"
PORT="${PORT:-4099}"
PW="live-$RANDOM$RANDOM"
URL="http://127.0.0.1:$PORT"
API="$URL/api"
MPROV="${MPROV:-opencode}"
MMODEL="${MMODEL:-deepseek-v4.1-flash-free}"

[ -x "$BIN/chan" ] && [ -x "$BIN/chan-mcp" ] || {
  echo "build first: (cd $REPO/chan && cargo build --workspace)"; exit 1; }
command -v opencode >/dev/null || { echo "need the opencode CLI on PATH"; exit 1; }

DEMO="$(mktemp -d /tmp/opencode/live-control.XXXXXX)"
SHARED="$DEMO/shared/.chan"
echo "demo dir : $DEMO"
echo "server   : $URL  (dedicated; your global opencode config is untouched)"
echo "model    : $MPROV/$MMODEL"

"$BIN/chan" --dir "$SHARED" init >/dev/null

# Dedicated server config: a working model + the chan MCP (shared identity "chan").
CFG=$(cat <<EOF
{"model":"$MPROV/$MMODEL",
 "mcp":{"chan":{"type":"local","enabled":true,
   "command":["$BIN/chan-mcp","--dir","$SHARED","--as","chan",
              "--opencode","$URL","--opencode-password","$PW"]}}}
EOF
)

( cd "$DEMO" && exec env OPENCODE_SERVER_PASSWORD="$PW" OPENCODE_CONFIG_CONTENT="$CFG" \
    opencode serve --port "$PORT" --print-logs ) >"$DEMO/serve.log" 2>&1 &
SPID=$!

cleanup() {
  for s in "${SID_MAIN:-}" "${SID_W1:-}" "${SID_W2:-}"; do
    [ -n "$s" ] && $CURL -s -m 5 -u "opencode:$PW" -X DELETE "$API/session/$s" >/dev/null 2>&1
  done
  kill "$SPID" 2>/dev/null
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  [ "$($CURL -s -o /dev/null -m 2 -w '%{http_code}' -u "opencode:$PW" "$API/info")" = "200" ] && break
  sleep 0.5
done

new_session() { # $1 = title
  $CURL -s -m 10 -u "opencode:$PW" -H 'content-type: application/json' -X POST \
    -d "{\"title\":\"$1\"}" "$API/session" \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["data"]["id"])'
}
inject() { # $1 = session id, $2 = text
  $CURL -s -m 15 -u "opencode:$PW" -o /dev/null -w '%{http_code}' \
    -H 'content-type: application/json' -X POST \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"text":sys.argv[1]}))' "$2")" \
    "$API/session/$1/prompt"
}
transcript() { # $1 = session id
  $CURL -s -m 5 -u "opencode:$PW" "$API/session/$1/message?limit=60" | python3 -c '
import sys,json
for m in json.load(sys.stdin).get("data",[]):
    if not isinstance(m,dict): continue
    if m.get("type")=="idle": print("   OUTCOME:", m.get("outcome"))
    for p in (m.get("content") or []):
        if not isinstance(p,dict): continue
        if p.get("type")=="tool":
            print("   TOOL", p.get("name"), "->", (p.get("state") or {}).get("status"))
        elif p.get("type")=="text" and (p.get("text") or "").strip():
            print("   text:", p["text"].replace("\n"," ")[:160])
'
}

SID_MAIN="$(new_session chan-main)"; SID_W1="$(new_session chan-w1)"; SID_W2="$(new_session chan-w2)"
echo "sessions : main=$SID_MAIN  w1=$SID_W1  w2=$SID_W2"

MAIN_BRIEF="You are the ORCHESTRATOR. Use ONLY the chan MCP tools (chan_active, chan_inject); do not shell out.
Two WORKER sessions are running: w1=$SID_W1 and w2=$SID_W2.

Do exactly this, in order:
1. Call chan_active and note the running session ids.
2. Call chan_inject with session \"$SID_W1\" and text: You are worker w1. Compute 7*7 and call chan_send with to=\"direct:orch\", kind=\"result\", payload {\"name\":\"w1\",\"value\":<the value>}. Then stop.
3. Call chan_inject with session \"$SID_W2\" and text: You are worker w2. Compute 9*9 and call chan_send with to=\"direct:orch\", kind=\"result\", payload {\"name\":\"w2\",\"value\":<the value>}. Then stop.
4. Call chan_active again.
5. Reply with JSON {\"before\":[<ids from step 1>],\"after\":[<ids from step 4>]}."

echo; echo "== MAIN: inject brief (asks it to control w1 + w2) =="
echo "brief http=$(inject "$SID_MAIN" "$MAIN_BRIEF")"

echo; echo "== orchestrator awaits the results posted back =="
"$BIN/chan" --dir "$SHARED" subscribe --as orch --room team >/dev/null 2>&1 || true
echo "result 1: $("$BIN/chan" --dir "$SHARED" await --as orch --kind result --timeout 150s 2>/dev/null | tail -1)"
echo "result 2: $("$BIN/chan" --dir "$SHARED" await --as orch --kind result --timeout 150s 2>/dev/null | tail -1)"

echo; echo "== STEER: re-inject w1 for a second task (re-control after completion) =="
echo "steer http=$(inject "$SID_W1" "You are worker w1 again. Square your previous result (49) and call chan_send with to=\"direct:orch\", kind=\"result\", payload {\"name\":\"w1b\",\"value\":<the value>}. Then stop.")"
echo "result 3: $("$BIN/chan" --dir "$SHARED" await --as orch --kind result --timeout 150s 2>/dev/null | tail -1)"

echo; echo "== MAIN transcript =="; transcript "$SID_MAIN"
echo; echo "== w1 transcript (did it post back?) =="; transcript "$SID_W1"

echo; echo "demo dir kept: $DEMO"
