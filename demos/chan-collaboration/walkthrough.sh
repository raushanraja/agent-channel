#!/usr/bin/env bash
# End-to-end walkthrough of chan in a throwaway temp dir:
# local messaging -> pub/sub -> await -> daemon (HTTP+SSE, auth) -> MCP over stdio.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO/chan/target/debug"
[ -x "$BIN/chan" ] || { echo "build first: (cd $REPO/chan && cargo build --workspace)"; exit 1; }

DEMO=$(mktemp -d /tmp/opencode/chan-demo.XXXXXX)
CHAN="$DEMO/.chan"
STATE="$DEMO/cursors.json"
TOKEN=$(cat /proc/sys/kernel/random/uuid)
PORT=27499
BASE="http://127.0.0.1:$PORT"

say() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }
echo "temp dir: $DEMO"

say "1. local: init / send / recv"
"$BIN/chan" --dir "$CHAN" init
"$BIN/chan" --dir "$CHAN" send --from orch --to direct:w1 --kind task --payload '{"phase":1}'
echo -n "w1 inbox: "; "$BIN/chan" --dir "$CHAN" recv --as w1

say "2. pub/sub room"
"$BIN/chan" --dir "$CHAN" subscribe --as w1 --room build
"$BIN/chan" --dir "$CHAN" publish --room build --from orch --kind progress --payload '"halfway"'
echo -n "w1#build: "; "$BIN/chan" --dir "$CHAN" room --as w1 --room build
echo -n "rooms:    "; "$BIN/chan" --dir "$CHAN" rooms
echo -n "stats:    "; "$BIN/chan" --dir "$CHAN" list

say "3. await (blocks until a question lands)"
( sleep 0.4; "$BIN/chan" --dir "$CHAN" send --from orch --to direct:w2 --kind question --payload '"ok?"' ) &
"$BIN/chan" --dir "$CHAN" await --as w2 --kind question --timeout 5s
wait

say "4. daemon: chan serve + token + remote client"
"$BIN/chan-serve" --dir "$CHAN" --bind "127.0.0.1:$PORT" --token "$TOKEN" >"$DEMO/serve.log" 2>&1 & SRV=$!
for _ in $(seq 1 50); do curl -sf "$BASE/v1/health" >/dev/null 2>&1 && break; sleep 0.1; done
echo -n "health:   "; curl -s "$BASE/v1/health"; echo
"$BIN/chan" --server "$BASE" --token "$TOKEN" --state "$STATE" send --from orch --to direct:w3 --kind task --payload '{"net":true}'
echo -n "remote await w3: "
"$BIN/chan" --server "$BASE" --token "$TOKEN" --state "$STATE" await --as w3 --kind task --timeout 5s
echo -n "unauthenticated send: "
"$BIN/chan" --server "$BASE" --state "$STATE" send --from orch --to direct:w3 --kind task --payload '1' >/dev/null 2>&1 \
  && echo "UNEXPECTED SUCCESS" || echo "refused (401) as expected"
kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null

say "5. MCP over stdio: initialize -> tools/list -> tools/call chan_send"
{
  printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"demo","version":"0"}}}'
  printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}'
  printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
  printf '%s\n' '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"chan_send","arguments":{"to":"direct:w4","kind":"task","payload":{"via":"mcp"}}}}'
} | timeout 8 "$BIN/chan-mcp" --dir "$CHAN" --as worker-9 > "$DEMO/mcp.out" 2>/dev/null
echo -n "tools advertised: "
python3 - "$DEMO/mcp.out" <<'PY'
import sys, json
names = []
for line in open(sys.argv[1]):
    try: m = json.loads(line)
    except: continue
    res = m.get("result") or {}
    if isinstance(res, dict) and "tools" in res:
        names = [t["name"] for t in res["tools"]]
print(", ".join(names) if names else "(none)")
PY
echo -n "mcp wrote durable msg: "; "$BIN/chan" --dir "$CHAN" recv --as w4

say "6. on-disk layout"
find "$CHAN" -type f | sed "s#$CHAN/##" | sort
echo "cleanup: rm -rf $DEMO"
