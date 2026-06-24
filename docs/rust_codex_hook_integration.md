# Rust TemporalStore Agent Hook Integration

Last validated: 2026-06-24

This mirrors the C++ MatrixArk agent hook flow, but uses the Rust TemporalStore
context pipeline directly. Codex, Claude, Cursor, and generic agent lifecycle
events are converted into
`ContextNode`, `ContextEvent`, `ContextIndexRef`, `ContextSummaryDirtyMarker`,
retrieval blocks, prompt injection, and `ContextPackAudit` records through
`TemporalEngine`.

For MCP-based Codex integration, use the shared MatrixArk MCP server with either
the C++ `temporalstore-direct` backend or the Rust `temporalstore-rust` backend.
That parity path is documented in
`docs/rust_cpp_codex_mcp_integration.md`.

## Hook Shape

Codex sends JSON to stdin for hook events such as `UserPromptSubmit` and `Stop`.
Claude/Cursor style integrations can send equivalent JSON payloads. The Rust
hook accepts the same tolerant payload fields used by the C++ hook plus common
agent aliases:

```text
prompt
user_prompt
userPrompt
input
text
message
query
instruction
params.prompt
params.input
params.text
turn.input
request.prompt
request.input
request.text
conversation.last_message
cursor.prompt
claude.prompt
messages[].content
items[].content
transcript[].content
conversation[].content
raw text payloads
```

The Rust path is:

```text
agent hook event
-> codex_context_hook
-> extract_context
-> TemporalEngine durable ContextNode/Event/IndexRef/DirtyMarker writes
-> retrieve_context for UserPromptSubmit or explicit --query
-> inject_context
-> ContextPackAudit
-> JSON hook report + JSONL event log
```

The binary keeps the historical name `codex_context_hook` for compatibility, but
it accepts `--agent-name` or `MATRIXARK_AGENT_NAME` / `TEMPORALSTORE_AGENT_NAME`.
Supported built-in profiles are `codex`, `claude`, `cursor`, and `generic`.

## Project Hook Config

This open-source repository does not track `.codex/hooks.json`, because Codex
hook files normally contain local checkout paths, WSL distribution names, user
ids, and session ids. Create a local `.codex/hooks.json` from this template when
you want to enable hooks for your checkout:

```bash
wsl -d <your-ubuntu-distro> -- bash -lc \
  'cd <repo-root> &&
   tools/run_rust_codex_context_hook.sh --event UserPromptSubmit \
     --root /tmp/temporalstore-rust-codex-hook \
     --event-log /tmp/temporalstore-rust-codex-hook/events.jsonl \
     --agent-name codex \
     --account-id acct_codex \
     --tenant-id tenant_codex \
     --user-id <user-id> \
     --session-id codex-thread-local'
```

After changing hooks, open `/hooks` in Codex and trust the project hook
definition. Codex records trust by hook hash, so edited hook commands must be
trusted again.

## Claude And Cursor

Claude or Cursor can call the same binary with a different agent profile. Each
agent gets its own session index under `--root/<agent>-session-index.json`, while
all events still use the same Rust TemporalStore context models.

Claude-style prompt ingestion:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-context-workflow-target \
  cargo run -p temporalstore-rust --bin codex_context_hook -- \
  --agent-name claude \
  --root /tmp/temporalstore-rust-agent-hook-test \
  --event-log /tmp/temporalstore-rust-agent-hook-test/events.jsonl \
  --event UserPromptSubmit \
  --session-id claude-thread-local <<'JSON'
{"claude":{"prompt":"Remember that Maya owns the Claude release checklist."}}
JSON
```

Cursor-style tool or prompt event:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-context-workflow-target \
  cargo run -p temporalstore-rust --bin codex_context_hook -- \
  --agent-name cursor \
  --root /tmp/temporalstore-rust-agent-hook-test \
  --event-log /tmp/temporalstore-rust-agent-hook-test/events.jsonl \
  --event cursor.tool \
  --session-id cursor-workspace-local <<'JSON'
{"cursor":{"prompt":"The edited file adds context retrieval tests."}}
JSON
```

## Manual Validation

Build or run the hook:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-context-workflow-target \
  cargo run -p temporalstore-rust --bin codex_context_hook -- \
  --agent-name codex \
  --root /tmp/temporalstore-rust-codex-hook-test \
  --event-log /tmp/temporalstore-rust-codex-hook-test/events.jsonl \
  --event UserPromptSubmit \
  --session-id rust-codex-test <<'JSON'
{"prompt":"Remember that Alice approved the GPU purchase for Project Orion."}
JSON
```

Then query the same session:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-context-workflow-target \
  cargo run -p temporalstore-rust --bin codex_context_hook -- \
  --agent-name codex \
  --root /tmp/temporalstore-rust-codex-hook-test \
  --event-log /tmp/temporalstore-rust-codex-hook-test/events.jsonl \
  --event UserPromptSubmit \
  --session-id rust-codex-test \
  --query "What did Alice approve for Project Orion?" <<'JSON'
{"prompt":"What did Alice approve for Project Orion?"}
JSON
```

Observed result from this validation:

| Field | Value |
| --- | --- |
| `backend` | `rust-temporalstore` |
| first prompt `ingest.status` | `accepted` |
| first prompt `retrieve.selected_ref_count` | `3` |
| first prompt `retrieve.injected_prompt_contains_context` | `true` |
| second prompt `node_index.session_node_count` | `2` |
| second prompt `retrieve.selected_ref_count` | `6` |
| second prompt `retrieve.injected_prompt_contains_context` | `true` |

## Report Fields

The hook prints one JSON object per event:

```json
{
  "status": "ok",
  "backend": "rust-temporalstore",
  "agent_name": "codex",
  "agent_profile": "codex",
  "event": "UserPromptSubmit",
  "lifecycle_stage": {
    "before_llm_retrieve": true,
    "after_llm_ingest_only": false,
    "hook_boundary_commit": false,
    "idle_timeout_commit": false
  },
  "ingest": {
    "status": "accepted",
    "node_hash": 16115782117882045832,
    "event_id_hash": 16043062544917456546
  },
  "retrieve": {
    "context_pack_id": "rust-codex-pack-c65717cdd61cd67c",
    "selected_ref_count": 3,
    "used_context_tokens": 13,
    "injected_prompt_contains_context": true
  }
}
```

The JSONL event log is stored at the configured `--event-log` path. A small
session node index is stored under `--root/<agent>-session-index.json` so
separate hook processes can retrieve prior Rust TemporalStore nodes for the same
agent session.

## Difference From C++ Hook

- C++ hook default: `matrixark_codex_cpp_hook.sh` plus MatrixArk direct adapter
  backed by the C++ SDK.
- Rust hook default: `codex_context_hook` plus Rust `TemporalEngine`.
- Rust keeps the same high-level lifecycle contract but does not use brpc/thrift
  or the C++ SDK.
- Stop events are ingested as assistant/user-event context. Full C++-style
  session batch commit is still represented by the regular Rust context
  workflow and benchmark harnesses, not by this minimal hook binary.
