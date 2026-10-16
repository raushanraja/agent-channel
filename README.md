# orchestrator

Tools for orchestrating coding agents.

This repo ships **`chan`** — durable, typed message channels (direct + pub/sub)
for coordinating many agents over a file-log source of truth, with a live control
surface for `opencode` sessions.

## Layout

- `chan/` — Rust workspace implementing `chan-core`, `chan-live`,
  `chan-opencode`, `chan-mcp`, `chan-cli`, `chan-server`, and `chan-client`.
- `orchestracon/` — the OpenCode skill for orchestrating work with `opencode run`.
- `config/` — ready-to-use snippets for wiring `chan` into coding agents
  (OpenCode, Claude Desktop, Codex). See [`config/README.md`](config/README.md).
- `docs/plans/` — design and implementation plans.
- `demos/chan-collaboration/` — runnable end-to-end examples.

## Quick install

```bash
make        # build + install binaries, skill, MCP entry, auto-pull plugin
make verify # sanity-check
make test   # run the full Rust gate
```

For manual installation details, see the Makefile and `config/`.