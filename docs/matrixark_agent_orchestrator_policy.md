# MatrixArk Agent Orchestrator Policy

MatrixArk treats Codex, Claude, Cursor, OpenClaw, OpenCode, Aider, Continue,
Cline/Roo, and generic agents as clients of one envelope. Agents send visible
local context, query text, scope hints, token budget estimates, lifecycle event
type, and optional file/resource references. `file_refs` and `resource_refs` are
optional; use them only for visible files or resources the agent is allowed to
send, such as local Markdown/PDF paths or S3 `raw_uri` values. Agents do not need
to understand ContextEvent, ContextEntity, ContextSummary, ContextEmbedding,
ContextIndex, ResourceChunk, or SkillSection internals.

## Lifecycle

- Before LLM: call `matrixark_retrieve`.
- After answer or tool result: call `matrixark_ingest` with durable outcome.
- Resource or skill added: call `matrixark_ingest` with `raw_uri` or file ref.
- Feedback: call `matrixark_feedback` with accepted/rejected refs.
- Session boundary: call `matrixark_session_commit` for batch extraction.

## Envelope

The generated source of truth is `tools/matrixark_agent_config.py`.

```bash
python3 tools/matrixark_agent_config.py --client policy
python3 tools/matrixark_agent_config.py --client all
```

The envelope must include only visible local context. Do not send hidden prompts,
system prompts, or private model reasoning. MatrixArk resolves scope, enforces
access, dedupes local context against remote refs, and returns a compact
ContextPack.

Minimum useful lifecycle payloads:

- `before_llm`: `query`, optional `scope`, optional `local_context`, optional `max_context_tokens`.
- `after_answer` or `after_tool`: `messages` containing user/assistant/tool outcome and useful refs.
- `resource_added`: `file_refs`, `resource_refs`, or `raw_uri`.
- `feedback`: `accepted_refs` or `rejected_refs`.
- `session_boundary`: `scope` plus optional commit reason.
