#!/usr/bin/env bash
# Two `opencode run` agents (different models) collaborate over a chan daemon,
# driving the channel with the `chan` CLI.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/chan/target/debug"
[ -x "$BIN/chan" ] || { echo "build first: (cd $REPO/chan && cargo build --workspace)"; exit 1; }
command -v opencode >/dev/null || { echo "need the opencode CLI on PATH"; exit 1; }

DEMO=$(mktemp -d /tmp/opencode/collab.XXXXXX)
PORT=27511
BASE="http://127.0.0.1:$PORT"
TOKEN=$(cat /proc/sys/kernel/random/uuid)
CHANDIR="$DEMO/chan"
M1="${W1_MODEL:-opencode/deepseek-v4.1-flash-free}"
M2="${W2_MODEL:-opencode-go/deepseek-v4.1-flash}"

echo "demo dir : $DEMO"
echo "server   : $BASE  (token ${TOKEN:0:8}…)"
echo "workers  : w1=$M1  w2=$M2"

"$BIN/chan" --dir "$CHANDIR" init >/dev/null
"$BIN/chan-serve" --dir "$CHANDIR" --bind "127.0.0.1:$PORT" --token "$TOKEN" >"$DEMO/serve.log" 2>&1 & SRV=$!
for _ in $(seq 1 100); do curl -sf "$BASE/v1/health" >/dev/null 2>&1 && break; sleep 0.1; done

mkbrief() {
  local name="$1" n="$2"
  local flags="--server $BASE --token $TOKEN --state $DEMO/$name.cursors.json"
  cat > "$DEMO/brief-$name.md" <<EOF
# chan collaboration — you are agent "$name"

You are one of several agents collaborating over a message bus called \`chan\`.
Use the shell tool and run these steps EXACTLY, in order. If a command blocks,
that is expected — wait for it.

The chan CLI is: $BIN/chan
Common flags: $flags

Step 1 — subscribe to the team room:
  $BIN/chan $flags subscribe --as $name --room team

Step 2 — announce yourself:
  $BIN/chan $flags publish --room team --from $name --kind event --payload '{"join":"$name"}'

Step 3 — wait for your task (blocks until the orchestrator sends it):
  $BIN/chan $flags await --as $name --kind task --timeout 400s
The JSON it returns has payload like {"n": <number>, "go": "square"}.

Step 4 — compute value = n * n (the square of that number).

Step 5 — send the result back to the orchestrator as agent $name:
  $BIN/chan $flags send --from $name --to direct:orch --kind result --payload '{"name":"$name","value":<THE_SQUARE>}'

Step 6 — publish completion:
  $BIN/chan $flags publish --room team --from $name --kind event --payload '{"done":"$name"}'

Then stop and report the exact result JSON you sent.
EOF
}
mkbrief w1 7
mkbrief w2 9

echo; echo "== launching two opencode run agents =="
( cd "$DEMO" && opencode run --model "$M1" --auto --file "$DEMO/brief-w1.md" \
    "Follow the brief exactly using the shell tool. Report your result JSON when done." \
    > "$DEMO/w1.log" 2>&1; echo "W1_EXIT=$?" >> "$DEMO/w1.log" ) & P1=$!
( cd "$DEMO" && opencode run --model "$M2" --auto --file "$DEMO/brief-w2.md" \
    "Follow the brief exactly using the shell tool. Report your result JSON when done." \
    > "$DEMO/w2.log" 2>&1; echo "W2_EXIT=$?" >> "$DEMO/w2.log" ) & P2=$!

ORCH="$BIN/chan --server $BASE --token $TOKEN --state $DEMO/orch.cursors.json"
$ORCH subscribe --as orch --room team >/dev/null
sleep 4

echo; echo "== orchestrator dispatches tasks =="
$ORCH send --from orch --to direct:w1 --kind task --payload '{"n":7,"go":"square"}'
$ORCH send --from orch --to direct:w2 --kind task --payload '{"n":9,"go":"square"}'

echo; echo "== orchestrator awaits results (blocking on the channel) =="
echo -n "result 1: "; $ORCH await --as orch --kind result --timeout 400s
echo -n "result 2: "; $ORCH await --as orch --kind result --timeout 400s

echo; echo "== shared room trace (pub/sub) =="
$ORCH room --as orch --room team

echo; echo "== waiting for agents to exit =="
wait "$P1"; wait "$P2"
echo "== w1 log tail =="; tail -n 12 "$DEMO/w1.log"
echo "== w2 log tail =="; tail -n 12 "$DEMO/w2.log"
kill "$SRV" 2>/dev/null
echo; echo "demo dir kept: $DEMO"
