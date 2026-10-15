#!/usr/bin/env bash
# gate-lock.sh <port>
#
# Serializes the DB/oracle-touching part of a verification gate so multiple
# parallel `opencode run` phases can compile/test concurrently but never run two
# oracle/DB jobs at once, and never fight over the gate server port.
#
# Usage:  bash gate-lock.sh 27420
# Output: rust_rc=<n> python_rc=<n>   (plus a log of the run)
#
# Edit the CONFIG block for your repo. Keep each parallel caller on its OWN port.
set -u
PORT="${1:?usage: gate-lock.sh <port>}"
LOCK="/tmp/opencode/gate.lock"
LOGDIR="/tmp/opencode"
mkdir -p "$LOGDIR"

# ---- CONFIG (edit per project) --------------------------------------------
REPO="${REPO:-$PWD}"
BIN="${BIN:-$REPO/target/debug/app}"           # server binary to smoke
BASE_URL="http://localhost:${PORT}"
ORACLE_CMD="${ORACLE_CMD:-pytest -q}"          # runs against $BASE_URL
# ---------------------------------------------------------------------------

# One exclusive lock around everything that touches the shared DB / server port.
exec 9>"$LOCK"
flock 9

log="$LOGDIR/gate-${PORT}.log"
: > "$log"

echo "== build (safe to overlap; cargo locks itself) ==" | tee -a "$log"
( cd "$REPO" && cargo build ) >>"$log" 2>&1 || { echo "rust_rc=1 python_rc=skip"; exit 0; }

echo "== start server on :$PORT ==" | tee -a "$log"
"$BIN" >"$LOGDIR/server-${PORT}.log" 2>&1 &
srv=$!
trap 'kill "$srv" 2>/dev/null' EXIT
sleep 3

echo "== oracle ==" | tee -a "$log"
if BASE_URL="$BASE_URL" bash -c "$ORACLE_CMD" >>"$log" 2>&1; then prc=0; else prc=1; fi
rrc=0
kill "$srv" 2>/dev/null

# Residue / cleanup hooks go here (e.g. purge derived rows, count leftovers).
echo "rust_rc=${rrc} python_rc=${prc}"
