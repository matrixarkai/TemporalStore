# TemporalStore Agent Hooks Design

## Goals

- Provide a baseline-style installable integration for Codex first, then
  Claude and other agents.
- Keep agent-specific hook configuration thin.
- Normalize all agent events into one MatrixArk event schema.
- Preserve same-session context while still enabling weighted cross-session,
  same-repo, and same-topic retrieval.
- Support Windows, Linux, macOS, local Docker, native, WSL, and hosted
  TemporalStore deployments.

## Architecture

```text
Agent lifecycle hook
  -> temporalstore_hook_launcher.mjs
  -> payload_normalizer.mjs
  -> session_resolver.mjs
  -> temporalstore_client.mjs
  -> TemporalStore / MatrixArk backend
```

## Event Model

The launcher converts Codex, Claude, and custom payloads into this common shape:

```json
{
  "agent": "codex",
  "event": "UserPromptSubmit",
  "conversation_id": "019f66b3-...",
  "session_id": "019f66b3-...",
  "session_id_source": "payload.session_id",
  "workspace_root": "/opt/github-services/TemporalStore",
  "repo_remote": "https://github.com/matrixarkai/TemporalStore.git",
  "project": "TemporalStore",
  "role": "user",
  "text": "user prompt text",
  "timestamp_ms": 1784130000000
}
```

## Session Semantics

Same session means same agent conversation/task/thread. For Codex, prefer the
Codex session id or thread/conversation id. For Claude, prefer the transcript or
conversation id when present. If no id is present, derive a stable fallback from
workspace plus process metadata.

## Retrieval Weighting

Default retrieval should not be same-session-only. Use same-session-first:

```text
same session:        1.00
same repo/workspace: 0.75
same product/topic:  0.45
global durable:      0.25
unrelated session:   0.05
```

Recommended query plan:

```text
1. Retrieve top K from same session.
2. Retrieve from same repo/workspace.
3. Retrieve from same product/topic.
4. Retrieve durable global facts.
5. Deduplicate, rerank, and fit the agent token budget.
```

## Platform Modes

### WSL Mode

Windows agent host calls `wsl.exe`, changes directory to the TemporalStore repo,
then invokes `tools/matrixark_codex_hook.py`.

### Native Mode

Linux/macOS runs Python directly from the TemporalStore repo.

### Remote Mode

The launcher posts normalized events to a hosted endpoint. This is the preferred
team/deployment mode because no local TemporalStore binaries are required.

### Docker Mode

Docker should expose a local HTTP endpoint and use the same remote-mode client.
Docker composition is intentionally left as a follow-up after the HTTP contract
is stable.

## Split Plan

Keep this under `integrations/agent-hooks/` until:

- Codex plugin install is repeatable.
- Claude settings install is repeatable.
- Smoke tests pass on Windows/WSL and Linux.
- Remote endpoint contract is stable.

Then split into a standalone repository with this directory as the initial root.
