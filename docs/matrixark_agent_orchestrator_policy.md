# MatrixArk Agent Orchestrator Policy

MatrixArk treats Codex as the only production-supported hook client today.
Claude Code, Claude Desktop, Cursor, OpenClaw, OpenCode, Aider, Continue,
Cline/Roo, and generic agents remain TODO/planned integrations until their hook
payloads and registration flows are validated. The shared envelope is still the
target design: agents send visible local context, query text, scope hints, token
budget estimates, lifecycle event type, and optional file/resource references.
`file_refs` and `resource_refs` are optional; use them only for visible files or
resources the agent is allowed to send, such as local Markdown/PDF paths or S3
`raw_uri` values. Agents do not need to understand ContextEvent, ContextEntity,
ContextSummary, ContextEmbedding, ContextIndex, ResourceChunk, or SkillSection
internals.

## Lifecycle

- Before LLM: retrieve with `matrixark_retrieve`.
- After answer/tool: ingest durable outcome with `matrixark_ingest`.
- Resource added: import resource/skill with `matrixark_ingest`.
- Feedback: record accepted/rejected refs with `matrixark_feedback`.
- Session boundary: commit/batch extract with `matrixark_session_commit`.

Agents must not construct or depend on ContextEvent, ContextEntity,
ContextSummary, ContextEmbedding, ContextIndex, ResourceChunk, or SkillSection
records. MatrixArk owns those internals behind the MCP envelope.

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
- `skill_added`: `file_refs`, `resource_refs`, or `raw_uri`.
- `feedback`: `accepted_refs` or `rejected_refs`.
- `session_boundary`: `scope` plus optional commit reason.
