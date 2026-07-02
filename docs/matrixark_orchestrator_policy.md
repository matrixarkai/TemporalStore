# MatrixArk Orchestrator Policy

MatrixArk agents should treat MCP as a durable context service, not as a place
to expose hidden prompts or internal chain-of-thought. The host agent sends only
visible workspace/user context and MatrixArk decides how to ingest, extract,
retrieve, and pack it.

## Standard Turn Flow

1. Before the model answers, call `matrixark_retrieve` when prior memory,
   shared resources, skills, project state, or cross-session context may help.
2. Pass the raw user query, visible local context, local token estimate, max
   context budget, and known scope fields.
3. After the model answers or a tool completes, call `matrixark_ingest` for
   durable outcomes: decisions, approvals, corrections, incidents, tool results,
   and accepted final answers.
4. On resource or skill file events, call `matrixark_ingest` with
   `kind=resource` or `kind=skill` plus `raw_uri`.
5. On explicit user feedback, call `matrixark_feedback` with accepted/rejected
   refs.
6. At task/session boundaries, call `matrixark_session_commit` so MatrixArk can
   batch extract and refresh summaries.

## Agent Envelope

All agents should use the same shape where possible:

```json
{
  "query": "raw user request",
  "scope": {
    "account_id": "acct_local",
    "tenant_id": "tenant_codex",
    "user_id": "local_user",
    "session_id": "thread_or_run_id",
    "agent_name": "codex"
  },
  "local_context": [
    {"ref_type": "file", "ref": "src/app.py", "text": "visible snippet"},
    {"ref_type": "tool", "ref": "pytest", "text": "visible tool output"}
  ],
  "local_context_tokens": 700,
  "max_context_tokens": 12000,
  "agent_hook": {
    "source": "codex",
    "hook_type": "before_llm",
    "hook_id": "stable-event-id",
    "observed_at_ms": 1780000000000,
    "auto_captured": true
  }
}
```

## Integration Rules

- Send visible local context only: open files, selected text, terminal/tool
  output, browser/page refs, and short local summaries.
- Do not send hidden/internal prompt context.
- Send `user_id` and `session_id` when known; MatrixArk fills local defaults
  otherwise.
- Let MatrixArk dedupe local refs and fill only the remaining remote budget.
- Do not require agents to know `ContextEvent`, `ContextEntity`,
  `ContextSummary`, or `ContextEmbedding` internals.

## Production Defaults

- Python MCP performs protocol, auth, request validation, parser/model
  orchestration, and backend dispatch.
- C++/Rust TemporalStore owns hot-path batch append, placement/index fetch,
  retrieval scoring, and ContextPack assembly when native APIs are available.
- Broad scan is debug/fallback only.
- Audit/debug is async or sampled by default.
- Cloud HTTP mode requires API key or trusted gateway authentication.
