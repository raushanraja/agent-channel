# <phase id> — <one-line goal>

Implement **<phase id>** from the committed plan. The orchestrator will review, fix,
and commit; you make the change, run the gates, and report. **Do NOT commit.**

Repo root: `<absolute path>`
Target: `<crate / package / dir>`

## Read first
- `<plan doc>` — §<the exact section this phase implements>.
- **`<sibling implementation to mirror>`** — the proven pattern to copy
  (e.g. `farmer-rs/src/event/consumers.rs`). Follow it; do not invent a new shape.

## Context (verified by the orchestrator)
- `<fact>` — `<file:line>`
- `<fact>` — `<command + result>`

## Change
1. `<file>` — `<exact function/behavior>`.
2. `<file>` — `<exact change>`.
3. …

## Tests
- `<new test name>` — `<what it asserts>`.
- Residue: `<derived rows/keys>` must be `0` after the suite.

## Gate (run these; paste raw output)
```
<build command>
<test command(s)>              # expected: <N> passed
<lint command>                 # expected: baseline <B>, no new
<extra: oracle / smoke / manual probe>
```
- Run the **standard** full suite at least **5 times, default parallelism** — report each
  result. Do not hide flakes behind `--test-threads=1` or concurrent suites.

## HARD RULES
- Do NOT commit. The orchestrator reviews and commits.
- Never run `git checkout/stash/clean/reset/restore`.
- Only touch: `<explicit allowlist of files>`.
- Do NOT touch `<tooling/build files>`, `<ports/servers you don't own>`.
- No new dependencies unless listed above. No emojis.

## Report (return EXACTLY this — it is my only channel besides your files)
```
STATUS: done | blocked
FILES:  <new/changed paths>
CHANGE: <2–4 lines: what changed and why>
EVIDENCE:
  <gate command> -> <result>
  ...
FLAKES: <the 5 standard-suite results>
RESIDUE: <counters>
BLOCKERS: <verbatim error, or none>
GIT: <output of `git status --short`>
```

---

### Resume / continuation block (append when re-dispatching a partial run)
```
RESUME <phase id>. It is partially done — the attached diff is the current state.
Do NOT restart and do NOT revert. Finish ONLY the remaining work listed here:
  - <remaining item 1>
  - <remaining item 2>
Then run the gate and report in the same format. Do NOT commit.
```
