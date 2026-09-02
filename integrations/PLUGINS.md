# MatrixArk memory plugins — Claude Code & OpenAI Codex

Ship the Rust **TemporalStore** memory engine to coding agents as first-class plugins. Both reuse
the same ingest / extract / retrieve pipeline benchmarked on LOCOMO & LongMemEval_s
(<https://temporalstore.ai/benchmarks.html>).

| Agent | Surface | What you get |
|-------|---------|--------------|
| **Claude Code** | Marketplace plugin `matrixark-memory@temporalstore` | Lifecycle hooks (auto ingest + ContextPack injection) + MCP recall/remember tools + slash commands |
| **OpenAI Codex** | `[mcp_servers.matrixark]` + `notify` in `~/.codex/config.toml` | MCP recall/remember tools + per-turn auto memory write |

The "plugin" is a **packaging + distribution** layer over the existing hook/MCP engine — the hooks
are the technical mechanism; the plugin just makes install one command and bundles the MCP tools and
slash commands. It drives a **TemporalStore checkout** on your machine (`matrixark_home`).

## Prerequisite (both)

```bash
git clone https://github.com/matrixarkai/TemporalStore
cd TemporalStore
```

## Quick install

```bash
# Codex (does the real config merge) + prints the Claude Code /plugin commands
bash integrations/install-matrixark-plugins.sh --agent both --matrixark-home "$(pwd)"
```

### Claude Code

```
/plugin marketplace add matrixarkai/TemporalStore
/plugin install matrixark-memory@temporalstore
```
Set `matrixark_home` to your checkout path when enabling (or `export MATRIXARK_HOME=...`).
Slash commands: `/matrixark-memory:memory-recall`, `:memory-remember`, `:memory-status`.
Details: [`claude-plugin/README.md`](claude-plugin/README.md).

### OpenAI Codex

```bash
bash integrations/codex-plugin/install-codex.sh --matrixark-home "$(pwd)"
```
Merges a managed block into `~/.codex/config.toml` (idempotent). Start a new session and run `/mcp`
to confirm the `matrixark` server is connected. Details:
[`codex-plugin/README.md`](codex-plugin/README.md).

## Architecture

```
Claude Code hooks ─┐                        ┌─ tools/matrixark_claude_hook.sh   (ingest + inject)
                   ├─ matrixark_home ──────>├─ tools/run_matrixark_mcp_server.sh (recall/remember, MCP)
Codex notify/MCP ──┘   (TemporalStore repo) └─ tools/run_rust_codex_context_hook.sh (codex ingest)
                                             └─ Rust TemporalStore engine (temporalstore-rust)
```

Both hook wrappers are **fail-open**: if `matrixark_home` is unset or the engine is unavailable,
they emit a benign result and never block or slow a turn.

## Troubleshooting

- **Claude: plugin enabled but no memory** — confirm `matrixark_home` points at the checkout
  (must contain `tools/matrixark_claude_hook.sh`); `export MATRIXARK_HOME=` overrides it.
- **Codex: `/mcp` shows no matrixark** — re-run `install-codex.sh`; `notify` and `[mcp_servers]`
  only take effect in the user-level `~/.codex/config.toml`.
- **First run is slow** — the MCP server builds the Rust proxy on first launch; subsequent runs reuse it.

## Relation to `agent-hooks/`

`integrations/agent-hooks/` is the earlier, settings.json-based hook installer (still supported).
These plugin packages are the modern, marketplace/`config.toml` distribution of the same engine.

Apache-2.0 · part of [TemporalStore](https://github.com/matrixarkai/TemporalStore).
