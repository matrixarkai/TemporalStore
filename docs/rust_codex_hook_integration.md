# Rust TemporalStore Codex Hook Integration

Last validated: 2026-06-23

This mirrors the C++ MatrixArk Codex hook flow, but uses the Rust TemporalStore
context pipeline directly. Codex lifecycle events are converted into
`ContextNode`, `ContextEvent`, `ContextIndexRef`, `ContextSummaryDirtyMarker`,
retrieval blocks, prompt injection, and `ContextPackAudit` records through
`TemporalEngine`.

## Hook Shape

Codex sends JSON to stdin for hook events such as `UserPromptSubmit` and `Stop`.
The Rust hook accepts the same tolerant payload fields used by the C++ hook:

```text
prompt
user_prompt
input
text
message
params.prompt
params.input
params.text
turn.input
messages[].content
items[].content
raw text payloads
```

The Rust path is:

```text
Codex hook event
-> codex_context_hook
-> extract_context
-> TemporalEngine durable ContextNode/Event/IndexRef/DirtyMarker writes
-> retrieve_context for UserPromptSubmit or explicit --query
-> inject_context
-> ContextPackAudit
-> JSON hook report + JSONL event log
```

## Project Hook Config

This repo now includes `.codex/hooks.json` with `UserPromptSubmit` and `Stop`
hooks. On Windows, the hook uses WSL:

```bash
wsl -d Ubuntu2204Deeproute -u root -- bash -lc \
  'cd /mnt/c/Users/Deeproute/Documents/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore &&
   tools/run_rust_codex_context_hook.sh --event UserPromptSubmit \
     --root /tmp/temporalstore-rust-codex-hook \
     --event-log /tmp/temporalstore-rust-codex-hook/events.jsonl \
     --account-id acct_codex \
     --tenant-id tenant_codex \
     --user-id deeproute \
     --session-id codex-thread-local'
```

After changing hooks, open `/hooks` in Codex and trust the project hook
definition. Codex records trust by hook hash, so edited hook commands must be
trusted again.

## Manual Validation

Build or run the hook:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-context-workflow-target \
  cargo run -p temporalstore-rust --bin codex_context_hook -- \
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
session node index is stored under `--root/codex-session-index.json` so separate
hook processes can retrieve prior Rust TemporalStore nodes for the same Codex
session.

## Difference From C++ Hook

- C++ hook default: `matrixark_codex_cpp_hook.sh` plus MatrixArk direct adapter
  backed by the C++ SDK.
- Rust hook default: `codex_context_hook` plus Rust `TemporalEngine`.
- Rust keeps the same high-level lifecycle contract but does not use brpc/thrift
  or the C++ SDK.
- Stop events are ingested as assistant/user-event context. Full C++-style
  session batch commit is still represented by the regular Rust context
  workflow and benchmark harnesses, not by this minimal hook binary.
