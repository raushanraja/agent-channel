# Testing record — `orchestrating-with-opencode-run`

Method: skill-writing TDD (RED → GREEN → REFACTOR), plus a live end-to-end smoke.

## RED — baseline (no skill)

Two subagent runs on the same "multi-phase refactor, delegate to other models" scenario.

**Baseline 1 (neutral):** produced a mostly-reasonable procedure, but missed every
hard-won detail: no probe-before-dispatch, "default fully sequential" (no safe
cross-repo parallelism), no serialized-gate lock, no residue/flake protocol, and it
used `git stash` itself.

**Baseline 2 (under deadline + rate-limit pressure)** took the risky shortcuts:

| Shortcut observed | Why it's wrong |
|---|---|
| Told the child to `commit` | Child must never commit — orchestrator owns the commit |
| `git reset --hard && git clean -fdx` between phases | Destroys sibling/uncommitted work in a shared tree |
| `pkill -f "opencode run"` | Kills other in-flight runs |
| Reactive retry on rate-limit | Wastes a run; probe first |
| Parsed stdout only | A mid-run rate-limit loses all evidence |

## GREEN — with skill

Same pressured scenario, skill provided. The agent now:
- **probes** `PROBE_OK` before every dispatch;
- **forbids the child from committing** and commits itself (`git add -p` scoped);
- uses **non-destructive** cleanup, **no** broad `pkill`, no `checkout/stash/clean/reset`;
- **serializes** the oracle with `flock` on an owned port;
- **re-runs the gate itself**, checks residue = 0, demands ×5 parallel runs;
- **resumes** a dead run by attaching `git diff` (not restart);
- reads the child's **session DB** when the report is murky.

Every baseline shortcut is now explicitly countered.

## REFACTOR — loophole closed

The GREEN agent wrote `opencode run … & echo "EXIT=$?"`, which records the
*backgrounding* status, not the run's. Fixed in the skill by showing the correct
subshell form: `( opencode run … >log 2>&1; echo "EXIT=$?" >>log ) &`.

## Live smoke — full loop, real CLI

`opencode v2.0.29`, model `opencode/deepseek-v4.1-flash-free`.

1. **Probe:** `opencode run --model … "Reply with exactly: PROBE_OK"` → `PROBE_OK`,
   `EXIT=0`.
2. **Dispatch:** `opencode run --model … --auto --file /tmp/opencode/smoke-brief.md
   "Implement SMOKE-1 …; do NOT commit" > run.log 2>&1; echo EXIT=$? >> run.log`
3. **Up-message (file):** child wrote `hello.txt` = `hello from worker`.
4. **Up-message (report):** child returned the exact envelope:
   `STATUS: done / FILES / EVIDENCE / GIT`.
5. **Log + exit:** `EXIT=0` appended; evidence parseable from the log.
6. **Independent verify:** orchestrator `cat hello.txt` confirmed the content.

Result: the probe→dispatch→file/report-up→verify loop works as documented.
