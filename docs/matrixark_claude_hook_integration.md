# MatrixArk Claude Code Hook Integration

The Claude Code hook gives Claude the **same MatrixArk context management as the
Codex hook** — ingestion, extraction, and retrieval — scoped to its own agent
identity. It is the Claude Code counterpart to the Codex hook family
(`tools/matrixark_codex_hook.sh`, `tools/matrixark_codex_rust_hook.sh`).

Entry point: [`tools/matrixark_claude_hook.sh`](../tools/matrixark_claude_hook.sh).

## Feature parity with the Codex hook

The Claude hook and the Codex hook run the **same engine**, parametrized by agent.
They differ only in agent identity and session scope (`claude:<id>` vs
`codex:<id>`, and a distinct tenant).

| Stage | Trigger events | Codex hook | Claude hook |
|---|---|---|---|
| **Ingestion** | `UserPromptSubmit`, `PostToolUse`, `PreToolUse`, `Stop`, `SubagentStop` | `matrixark_ingest` | same (`--agent claude`) |
| **Extraction** | `Stop`, `SubagentStop`, `PreCompact`, `SessionEnd`, idle/threshold | `matrixark_session_commit` batch extract (segments, entities, index, summary, embeddings) | same |
| **Retrieval** | `UserPromptSubmit` (before LLM) | `matrixark_retrieve` → `hookSpecificOutput.additionalContext` | same |

Parity is enforced by
`tools/test_matrixark_popular_agent_hooks.py::test_codex_and_claude_hooks_have_ingestion_extraction_retrieval_parity`,
which drives both agents through all three stages and asserts equivalent
behavior and identical result shapes.

## Backends

Selected with `MATRIXARK_CLAUDE_HOOK_BACKEND`:

- **`auto`** (default) — the python parity pipeline when the rust proxy binary
  (`target/release/matrixark_rust_proxy`) is present, otherwise the offline rust
  engine.
- **`python`** — `tools/matrixark_agent_hook.py --agent claude`. Byte-for-byte the
  same ingest/extract/retrieve code as the Codex hook (`matrixark_codex_hook.py`),
  running the full MatrixArk pipeline over the local rust proxy (no metaserver
  required).
- **`rust`** — the self-contained crate binary
  [`codex_context_hook`](../crates/temporalstore-rust/src/bin/codex_context_hook.rs)
  (`--agent-name claude`). Fully offline (`TemporalEngine` local dirs), per-agent
  session index at `<root>/claude-session-index.json`. Lighter than the full
  pipeline; good for offline/no-infra use.

Both backends emit the Claude Code hook contract: on `UserPromptSubmit` /
`SessionStart` they print
`{"hookSpecificOutput": {"hookEventName": "<event>", "additionalContext": "<retrieved context>"}}`;
all other events print `{}`. Any internal failure fails open (`{}`, exit 0) so a
hook problem never blocks a Claude Code turn.

## Install

Automatic (writes `~/.claude/settings.json`):

```bash
bash integrations/agent-hooks/install/install.sh --agent claude \
  --repo /opt/github-services/TemporalStore
```

This registers the full Claude Code lifecycle (`SessionStart`, `UserPromptSubmit`,
`PostToolUse`, `Stop`, `SubagentStop`, `PreCompact`, `SessionEnd`) pointing at
`tools/matrixark_claude_hook.sh`. `UserPromptSubmit` is registered with a 30s
timeout to fit Claude Code's budget for that event.

Manual: copy
[`integrations/agent-hooks/claude/settings.example.json`](../integrations/agent-hooks/claude/settings.example.json)
into `~/.claude/settings.json` (user-global) or `.claude/settings.json` (project),
replacing `${CLAUDE_PROJECT_DIR}` with the absolute repo path if your Claude Code
project root is not the TemporalStore repo.

Configuration:
[`integrations/agent-hooks/config/temporalstore-claude.example.env`](../integrations/agent-hooks/config/temporalstore-claude.example.env).

### Warm-up

The rust backend needs its binary built. The `SessionStart` hook builds it under
the long session-start budget; `UserPromptSubmit` never triggers a cold build (it
uses the existing binary, or fails open if none exists yet). To pre-build:

```bash
bash tools/run_rust_agent_context_hook.sh --help >/dev/null 2>&1 || true
CARGO_TARGET_DIR=/tmp/temporalstore-context-workflow-target \
  cargo build -p temporalstore-rust --bin codex_context_hook
```

## Quick check

```bash
echo '{"conversation_id":"demo","prompt":"Remember: Claude owns the release checklist."}' \
  | tools/matrixark_claude_hook.sh --event UserPromptSubmit
echo '{"conversation_id":"demo","prompt":"Who owns the release checklist?"}' \
  | tools/matrixark_claude_hook.sh --event UserPromptSubmit
```

The second call returns a `hookSpecificOutput.additionalContext` block containing
the remembered fact.
