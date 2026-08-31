# MatrixArk Memory — Claude Code plugin

Time-aware long-term memory for [Claude Code](https://code.claude.com), backed by the Rust
**TemporalStore** engine. The plugin:

- **Automatically ingests** each session and **injects a ranked ContextPack** on every prompt,
  via Claude Code lifecycle hooks (`SessionStart`, `UserPromptSubmit`, `PostToolUse`, `Stop`,
  `SubagentStop`, `PreCompact`, `SessionEnd`).
- Exposes **explicit memory tools over MCP** — recall a time-aware ContextPack, remember a fact,
  check backend status — plus slash commands `/matrixark-memory:memory-recall`,
  `/matrixark-memory:memory-remember`, `/matrixark-memory:memory-status`.

It reuses the same engine benchmarked on LOCOMO & LongMemEval_s
(see <https://temporalstore.ai/benchmarks.html>).

## Prerequisite

The plugin is the packaging layer; it drives a **TemporalStore checkout** on your machine.
Clone and build it once:

```bash
git clone https://github.com/matrixarkai/TemporalStore
# note the absolute path — you'll set it as matrixark_home below
```

## Install (marketplace)

```
/plugin marketplace add bjmeetsfo/TemporalStore
/plugin install matrixark-memory@temporalstore
```

When enabling, set the **`matrixark_home`** config to the absolute path of your TemporalStore
checkout (the directory containing `tools/matrixark_claude_hook.sh`). You can also override it
per-shell with `export MATRIXARK_HOME=/abs/path/TemporalStore`.

Or run the unified installer, which also prints these commands and sets `matrixark_home`:

```bash
bash integrations/install-matrixark-plugins.sh --agent claude
```

## How it works

| Piece | Wired to |
|-------|----------|
| Lifecycle hooks (`hooks/hooks.json`) | `scripts/matrixark-claude-hook.sh` → `$matrixark_home/tools/matrixark_claude_hook.sh --event <E>` |
| MCP server (`.mcp.json`) | `$matrixark_home/tools/run_matrixark_mcp_server.sh` (backend `temporalstore-rust`) |
| Slash commands (`commands/`) | call the `matrixark` MCP tools |

The hook wrapper is **fail-open**: if `matrixark_home` is unset or the engine is absent, it emits a
benign result and never blocks a turn.

## Files

```
integrations/claude-plugin/
├── .claude-plugin/plugin.json   # manifest + userConfig (matrixark_home)
├── hooks/hooks.json             # lifecycle hooks
├── .mcp.json                    # matrixark MCP server
├── commands/                    # /memory-recall, /memory-remember, /memory-status
├── scripts/matrixark-claude-hook.sh
└── README.md
```

Apache-2.0 · part of [TemporalStore](https://github.com/matrixarkai/TemporalStore).
