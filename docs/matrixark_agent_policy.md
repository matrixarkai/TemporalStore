# MatrixArk Agent Policy

Use this policy in Codex, Claude, Cursor, or a vertical AI harness when the
agent can call MatrixArk MCP tools.

## Principle

Use MatrixArk as durable remote context. Keep using native local context for
currently open files, active buffers, terminal output, selected code, tool
results, and the immediate conversation window.

MatrixArk should provide:

```text
cross-session memory
time-aware current state
shared resources and runbooks
skills and operating procedures
accepted/rejected evidence
team or enterprise governed context
```

## Before Answering

Call `matrixark_retrieve` when the request may depend on prior or shared
context.

Use it for:

```text
previous decisions
approvals or budget state
prior debugging attempts
customer/user preferences
current-state facts
shared runbooks/resources
team skills
cross-device context
```

Request shape:

```json
{
  "query": "What did we decide about the GPU budget?",
  "scope": {
    "account_id": "acct_acme",
    "tenant_id": "tenant_prod",
    "user_id": "user_123",
    "session_id": "cursor_thread_456"
  },
  "max_context_tokens": 2048,
  "local_context": [
    {"ref": "open-buffer:src/billing.py", "text": "short local summary"}
  ]
}
```

## After Answering

Call `matrixark_ingest` or `matrixark_feedback` when the turn produced durable
information.

Store:

```text
final answer summaries
accepted refs
rejected refs
corrections
confirmations
decisions
tool outcomes
new facts
new resource or skill refs
```

Feedback shape:

```json
{
  "context_pack_id": "pack_123",
  "messages": [
    {"role": "assistant", "content": "The GPU request is approved under capex."},
    {"role": "user", "content": "Yes, that is correct."}
  ],
  "accepted_refs": ["event_gpu_approval"],
  "rejected_refs": [],
  "scope": {
    "account_id": "acct_acme",
    "tenant_id": "tenant_prod",
    "user_id": "user_123",
    "session_id": "cursor_thread_456"
  }
}
```

## Session Commit

Call `matrixark_session_commit` when:

```text
task is finished
conversation is compacted
agent is about to stop
pending same-session messages >= 20
session has been idle for 10-30 minutes
```

This lets MatrixArk run one-pass batch extraction and produce stronger segments,
entities, summaries, and indexes without making every single-message ingestion
heavy.

## Local Plus Remote Context

Do not blindly append every MatrixArk result to every prompt.

Recommended budget policy:

```text
code-local question:
  local code first, MatrixArk for prior decisions/resources/skills

cross-session question:
  MatrixArk first, local code only if current state matters

current-state question:
  ContextEntity + fresh ContextEvent first, stale blockers as warnings

resource/runbook question:
  selected ResourceChunk with citation, not full raw file
```

## Minimum Scope

For production, pass at least one durable user/session identifier:

```text
account_id or tenant_id
user_id and preferably session_id
```

`session_id` is strongly recommended for confirmation detection, same-session
batch extraction, replay, and audit.

