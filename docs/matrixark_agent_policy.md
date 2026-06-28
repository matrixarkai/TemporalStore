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
  "local_context": [
    {"ref": "open-buffer:src/billing.py", "text": "short visible local summary"},
    {"ref": "tool-output:test", "text": "relevant test output summary"}
  ],
  "local_context_tokens": 700,
  "max_context_tokens": 2048,
  "ranking": {
    "weights": {"time": 0.2, "business": 0.1}
  }
}
```

Send only visible local context: open files, selected text, tool outputs,
terminal summaries, current page refs, and explicit local summaries. Do not send
hidden/internal prompt context. MatrixArk dedupes local refs and fills only the
remaining budget with remote context.

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

Post-answer ingest shape:

```json
{
  "messages": [
    {"role": "user", "content": "What did we decide about the GPU budget?"},
    {"role": "assistant", "content": "The GPU request is approved under capex."}
  ],
  "metadata": {
    "source": "agent_post_answer",
    "reply_to_context_pack_id": "pack_123",
    "local_context_refs": ["open-buffer:src/billing.py", "tool-output:test"]
  },
  "scope": {
    "account_id": "acct_acme",
    "tenant_id": "tenant_prod",
    "user_id": "user_123",
    "session_id": "cursor_thread_456"
  }
}
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
  same-user cross-session is considered by default in session_scope=prefer
  default budget is 12% normal, 15% broad/evidence, 20% current/latest/multi-hop/date
  cross-session candidates still need to pass the score threshold, session fanout, candidate cap, and token budget

current-state question:
  ContextEntity + fresh ContextEvent first, stale blockers as warnings

resource/runbook question:
  selected ResourceChunk with citation, not full raw file
```

MatrixArk should decide what to do from the payload:

```text
before_llm + query/messages:
  lightweight ingest user message, retrieve ContextPack

after_llm/tool_result:
  ingest assistant/tool evidence and link to prior ContextPack

ResourceAdded/SkillAdded + raw_uri:
  run import task, chunk, embed, extract resource facts or skill sections

Stop/PostCompact/idle:
  commit pending session buffer and run one-pass extraction
```

## Minimum Scope

For production, pass at least one durable user/session identifier:

```text
account_id or tenant_id
user_id and preferably session_id
```

`session_id` is strongly recommended for confirmation detection, same-session
batch extraction, replay, and audit.
