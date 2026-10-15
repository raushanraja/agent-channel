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
