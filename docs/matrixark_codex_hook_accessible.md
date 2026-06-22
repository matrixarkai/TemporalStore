# MatrixArk Codex Hook Integration

This accessible doc explains how MatrixArk hooks into Codex, what Codex sends us, what MatrixArk stores, and whether we get batch messages.

Source basis: the current Codex manual describes hooks as command scripts injected into the Codex agent loop. Hooks can send conversation data to logging or memory systems, summarize conversations into persistent memory, run validation at turn stop, and customize prompting by directory.

## 1. Goal

MatrixArk should not require Codex or a Cursor-like agent to understand MatrixArk internals. Codex can emit lifecycle events; MatrixArk converts them into context records.

```text
Codex lifecycle event
-> command hook
-> tools/matrixark_codex_hook.py
-> matrixark_ingest
-> raw ContextEvent + SessionBuffer
-> optional matrixark_retrieve for UserPromptSubmit
-> matrixark_session_commit on Stop/session boundary
-> derived ContextEntity / ContextSegment / ContextSummary / ContextEmbedding / ContextIndex
-> ContextPackAudit / commit audit
```

## 2. Useful Codex Hooks

| Hook | MatrixArk use | Recommended MVP behavior |
|---|---|---|
| SessionStart | mark thread/session scope | optional session marker |
| UserPromptSubmit | capture raw user prompt before LLM | ingest + retrieve ContextPack |
| PreToolUse | audit intended tool action | optional governance/audit |
| PermissionRequest | capture permission/governance state | optional governance/audit |
| PostToolUse | capture tool result, file path, command output summary | ingest useful tool result |
| PreCompact | capture compaction trigger | optional memory checkpoint |
| PostCompact | capture compacted summary | ingest session/node summary |
| SubagentStart | track delegated work | optional subtask marker |
| SubagentStop | capture subagent result | ingest subagent result |
| Stop | capture final assistant turn signal and session boundary | ingest final signal + `matrixark_session_commit` |

Minimum useful MVP:

```text
UserPromptSubmit -> ingest + retrieve
PostToolUse      -> ingest useful tool output summary
Stop             -> ingest final assistant signal + session_commit
PostCompact      -> ingest compacted session summary + session_commit
```

## 3. What Codex Sends Us

Codex command hooks send JSON on stdin. Payload fields can vary, so `tools/matrixark_codex_hook.py` is tolerant. It looks for text in:

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
raw_text
```

If no known text field exists, MatrixArk stores a compact JSON string from the hook payload.

Example `UserPromptSubmit` payload:

```json
{"prompt":"Remember that Alice approved the GPU purchase for Project Orion.","id":"turn-001","cwd":"/root/src/github-services/TemporalStore"}
```

The hook converts it into a MatrixArk ingest call:

```json
{
  "messages": [{"role":"user","content":"Remember that Alice approved the GPU purchase for Project Orion."}],
  "scope": {
    "account_id":"acct_codex",
    "tenant_id":"tenant_codex",
    "user_id":"deeproute",
    "session_id":"codex-thread-1",
    "team":"codex",
    "project":"local"
  },
  "metadata": {
    "source":"codex_hook",
    "codex_event":"UserPromptSubmit",
    "node_path":[
      "account:acct_codex",
      "tenant:tenant_codex",
      "principal:user:deeproute",
      "collection:sessions",
      "session:codex-thread-1",
      "event:UserPromptSubmit"
    ],
    "raw_hook_payload":{"prompt":"..."}
  },
  "agent_hook": {
    "source":"codex",
    "hook_type":"before_llm",
    "trigger":"UserPromptSubmit",
    "auto_captured":true
  }
}
```

For `UserPromptSubmit`, the hook also calls `matrixark_retrieve` so MatrixArk can return a ContextPack for the new prompt.

## 4. Event To Role Mapping

| Codex event | MatrixArk role | Hook type |
|---|---|---|
| UserPromptSubmit | user | before_llm |
| PreToolUse | tool | tool_result |
| PermissionRequest | tool | tool_result |
| PostToolUse | tool | tool_result |
| Stop | assistant | after_llm |
| PostCompact | assistant | after_llm |
| SubagentStop | assistant | after_llm |

## 5. What MatrixArk Stores

For a user prompt, MatrixArk stores:

```text
ContextSummary(node_l0) for each node-path prefix
ContextEmbedding(node_l0) for each node-path prefix
ContextEmbedding(event_text)
ContextEvent(raw prompt as replayable evidence)
ContextSummary(session_l0)
ContextEmbedding(session_l0)
ContextPackAudit if retrieval ran
```

For a useful tool result, MatrixArk should store:

```text
ContextEvent(tool result summary)
ContextIndex(tool name, file path, command type, success/error state)
ContextEntity(optional evolving state: build_status, test_status, current_plan)
ContextSummary/ContextEmbedding for the affected node
```

For `Stop`, MatrixArk should store:

```text
ContextEvent(final assistant answer or turn-stop signal)
ContextEntity(optional commitments, decisions, status updates)
ContextSummary(session_l0 update)
ContextEmbedding(summary/event)
```

Short feedback such as `yes`, `correct`, or `approved` should become `CONFIRMATION` only if MatrixArk has prior same-session context or an explicit `context_pack_id`. Without prior context, it should remain ambiguous/noise.

## 6. Do We Get Batch Messages?

Not by default from simple Codex command hooks.

Codex hooks are lifecycle events. A `UserPromptSubmit` hook is usually one submitted user prompt. A `PostToolUse` hook is usually one tool event. A `Stop` hook is one turn-stop signal. The current MVP hook therefore sends one MatrixArk message per hook event.

MatrixArk should still support batching in three modes:

### Option A: Online Per-Event Capture

```text
UserPromptSubmit -> matrixark_ingest(one user message)
PostToolUse      -> matrixark_ingest(one tool message)
Stop             -> matrixark_ingest(one assistant/final message)
```

This gives freshest serving context.

### Option B: Session Batch Extraction

A small daemon or background worker can buffer hook events by `account_id + tenant_id + user_id + session_id`, then call:

```text
matrixark_batch_extract(messages=[...], threshold_messages=20, force=false)
```

Recommended flush policy:

```text
flush when messages >= 20
flush on Stop if enough useful events accumulated
flush on PostCompact with the compacted summary
flush every 2-5 minutes for active sessions
```

### Option C: Hybrid Production Path

```text
per-event ingest for freshness
+ async batch extraction for quality
+ async entity update / summary refresh / compression
```

This is the best production direction: online events are immediately retrievable, while batch extraction creates better entities, segments, summaries, sparse terms, and compression records.

## 7. Hook Config Example

Project-local `.codex/hooks.json` can call the hook script. Use the real path and session IDs for your machine.

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event UserPromptSubmit --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local",
            "timeout": 30,
            "statusMessage": "Ingesting prompt into MatrixArk"
          }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Bash|apply_patch|Edit|Write|mcp__.*",
        "hooks": [
          {
            "type": "command",
            "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event PostToolUse --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local",
            "timeout": 30,
            "statusMessage": "Ingesting tool result into MatrixArk"
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event Stop --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local",
            "timeout": 30,
            "statusMessage": "Ingesting final Codex turn signal into MatrixArk"
          }
        ]
      }
    ]
  }
}
```

After adding or changing hooks, open `/hooks` in Codex and trust the exact hook definition. Codex records trust by hook hash, so changed hooks must be reviewed again.

## 8. Windows / WSL Command Example

If Codex runs on Windows but MatrixArk runs in WSL, use `commandWindows`:

```json
{
  "type": "command",
  "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event UserPromptSubmit",
  "commandWindows": "wsl -d Ubuntu -- bash -lc 'cd /root/src/github-services/TemporalStore && python3 tools/matrixark_codex_hook.py --event UserPromptSubmit --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local'",
  "timeout": 30
}
```

Find the real distro name with:

```powershell
wsl -l -v
```

## 9. Local Debug Commands

Ingest one prompt:

```bash
cd /root/src/github-services/TemporalStore
python3 tools/matrixark_codex_hook.py \
  --event UserPromptSubmit \
  --event-log /tmp/matrixark-codex-hook.jsonl \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute \
  --session-id codex-thread-1 <<'JSON'
{"prompt":"Remember that Alice approved the GPU purchase for Project Orion."}
JSON
```

Ask a follow-up query:

```bash
python3 tools/matrixark_codex_hook.py \
  --event UserPromptSubmit \
  --event-log /tmp/matrixark-codex-hook.jsonl \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute \
  --session-id codex-thread-1 <<'JSON'
{"prompt":"What was approved for Project Orion?"}
JSON
```

Inspect records:

```bash
tail -n 20 /tmp/matrixark-codex-hook.jsonl
```

Run the test:

```bash
PYTHONPATH=. python3 tools/test_matrixark_codex_hook.py
```

## 10. C++ TemporalStore Mode

For production-like storage, switch the hook to C++ TemporalStore direct mode:

```bash
export MATRIXARK_MCP_BACKEND=temporalstore-direct
export MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:18000
export MATRIXARK_TEMPORALSTORE_NAMESPACE=deploy_ns
export MATRIXARK_TEMPORALSTORE_TABLE=deploy_table
export MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:codex-hook
export TEMPORALSTORE_LIB=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so
```

Then run the same hook command. The hook will persist MatrixArk records through the C++ TemporalStore direct adapter.

## 11. Backlog

```text
Batch buffer daemon per user/session
Batch flush into matrixark_batch_extract at >=20 messages
Better PostToolUse payload summarization
ContextPack injection back into Codex prompt flow
PostCompact summary ingestion as first-class session summary
Hook idempotency and dedupe by turn_id/tool_call_id
Access-managed API-key mode for team deployments
C++ TemporalStore gateway/proxy mode for hosted MatrixArk
UI page showing Codex sessions, hook events, batches, ContextPacks, and replay
```

## 12. Recommendation

Start with per-event hooks for freshness:

```text
UserPromptSubmit + Stop + PostToolUse
```

Then add async batching:

```text
group by account_id / tenant_id / user_id / session_id
flush to matrixark_batch_extract at >=20 messages or on Stop/PostCompact
```

This gives MatrixArk both online freshness and VikingMem-style batch memory quality without requiring Codex itself to understand MatrixArk internal data models.
