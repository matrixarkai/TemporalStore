# MatrixArk Memory — OpenAI Codex integration

Time-aware long-term memory for the [OpenAI Codex CLI](https://learn.chatgpt.com/docs), backed by
the Rust **TemporalStore** engine. Codex's extension model is MCP + config hooks, so this ships as:

- **`[mcp_servers.matrixark]`** — a stdio MCP server exposing explicit memory tools (recall a
  time-aware ContextPack, remember a fact, check status).
- **`notify`** — a per-turn handler that ingests each completed turn into TemporalStore memory,
  fire-and-forget.

Same engine benchmarked on LOCOMO & LongMemEval_s (<https://temporalstore.ai/benchmarks.html>).

## Prerequisite

A TemporalStore checkout on your machine:

```bash
git clone https://github.com/matrixarkai/TemporalStore
```

## Install

```bash
# from your TemporalStore checkout
bash integrations/codex-plugin/install-codex.sh --matrixark-home "$(pwd)"
```

This merges a managed block into `~/.codex/config.toml` (idempotent; re-run to update). Then start a
new Codex session and run `/mcp` to confirm the `matrixark` server is connected.

## What it writes to ~/.codex/config.toml

```toml
[mcp_servers.matrixark]
command = "/abs/path/TemporalStore/tools/run_matrixark_mcp_server.sh"
env = { MATRIXARK_MCP_SERVER = ".../tools/matrixark_mcp_server.py", MATRIXARK_MCP_BACKEND = "temporalstore-rust" }
startup_timeout_sec = 60
tool_timeout_sec = 120
enabled = true

notify = ["bash", "/abs/path/TemporalStore/integrations/codex-plugin/scripts/matrixark-codex-notify.sh"]
```

`notify` is only honored in the **user-level** `~/.codex/config.toml`, not project-local files.
The notify handler is fail-open — it never blocks or slows a turn if the engine is absent.

## Optional: lifecycle hooks

Codex also supports lifecycle hooks (`[features] hooks = true` + `hooks.json`). To route context
injection/ingest through the dedicated Rust codex hook, point a command hook at
`tools/run_rust_codex_context_hook.sh`. See the Codex hooks docs for the current event schema.

Apache-2.0 · part of [TemporalStore](https://github.com/matrixarkai/TemporalStore).
