# chan collaboration demos

Runnable demos of agents coordinating over [`chan`](../../chan).

## Prereqs

```bash
cd ../../chan && cargo build --workspace
```

You also need the `opencode` CLI on `PATH` for the two collaboration demos.

## Run

| Script | What it shows |
|---|---|
| `./walkthrough.sh` | One process: local send/recv, pub/sub, `await`, the daemon (HTTP+SSE + auth), and MCP over stdio. |
| `./cli-collaboration.sh` | Two `opencode run` agents (different models) collaborating over a `chan serve` daemon, using the **CLI**. |
| `./mcp-collaboration.sh` | Two `opencode run` agents collaborating via the **MCP server** — native `chan_*` tools, no CLI. |
| `./live-control.sh` | One **main** agent drives two *running* sessions with chan's live tools (`chan_active` / `chan_inject`); the workers post results back to the orchestrator, then the main **steers** a finished worker. |

Each script uses a throwaway temp dir and prints the exchange. Override models with
`W1_MODEL` / `W2_MODEL` (free models come and go; probe one first with
`opencode run --model <m> "Reply with exactly: PROBE_OK"`).

## The pattern

1. Give each agent its own identity and the channel's coordinates.
2. Tell it to `subscribe` to a shared room, `await` its task, do the work, `send` the
   result back to `direct:orch`, and `publish` completion.
3. The orchestrator `await`s the results.

Two ways to attach an agent to the channel:

- **CLI** — the agent runs `chan --server <url> --token <t> …` from its shell.
- **MCP** — the agent's project `opencode.json` launches `chan-mcp --as <name>`, giving
  it `chan_send` / `chan_await` / … as native tools.

## Live control (`live-control.sh`)

The other demos use `opencode run` (fire-and-forget): a child gets a brief and never hears
from you again. `live-control.sh` instead drives **running sessions** on a dedicated
`opencode serve` via chan's live tools (`chan_active` / `chan_inject` / `chan_interrupt`) —
so you can inject more data into a session and re-control it after it finishes.

Two things it works around, both worth knowing:

- **Pin the model.** The account default may be dead (e.g. an OpenRouter key with no
  credits), which fails every session with `provider.quota`. The demo pins a working model
  in the server config; override with `MPROV`/`MMODEL`.
- **Session-directory config isn't loaded.** opencode does *not* read an API-created
  session's own `opencode.json`, so a per-agent chan MCP won't register. The demo injects
  the chan MCP into the *server* config (`OPENCODE_CONFIG_CONTENT`) instead — so all
  sessions share one chan identity (`chan`), and payloads carry `name` to disambiguate.
