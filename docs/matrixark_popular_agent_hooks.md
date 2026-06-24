# MatrixArk Hooks For Popular AI Agents

Date: 2026-06-24

## Summary

MatrixArk supports two integration styles for popular agents:

```text
MCP first:
  Codex, Claude, Cursor, Windsurf, Cline, Continue, vertical agents

Hook adapter:
  agents that expose lifecycle hooks or external command runners
```

This follows the OpenViking-style integration lesson: the context system should
be agent-facing, not app-specific. Agents get a stable interface for memory,
resources, and skills; products can add automatic recall/capture through hooks
when their runtime exposes lifecycle events.

Use `tools/matrixark_agent_hook.py` as the universal hook adapter. It reads the
agent's JSON payload from stdin and normalizes it into MatrixArk MCP calls:

```text
User prompt / before LLM -> matrixark_ingest + matrixark_retrieve
Tool result / after LLM -> matrixark_ingest
Stop / compact / idle -> matrixark_session_commit
```

## Universal Hook Command

```bash
cd /root/src/github-services/TemporalStore
echo '{"prompt":"Alice approved the GPU request.","session_id":"demo-thread"}' |
  python3 tools/matrixark_agent_hook.py \
    --agent generic \
    --event UserPromptSubmit \
    --backend local
```

For C++ TemporalStore-backed operation:

```bash
python3 tools/matrixark_agent_hook.py \
  --agent claude \
  --event UserPromptSubmit \
  --backend temporalstore-direct \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --storage-prefix matrixark:agent-hook
```

## Supported Agent Labels

```text
codex
claude
cursor
windsurf
cline
roo
continue
copilot
opencode
openclaw
aider
gemini
qwen-code
autogen
langgraph
crewai
llamaindex
semantic-kernel
dify
n8n
generic
```

These are presets, not a closed list. `--agent` accepts any string. The label is
used in `metadata.source`, `agent_hook.source`, session id fallbacks, and audit
logs.

## Event Mapping

```text
Before LLM / retrieve:
  UserPromptSubmit
  user_prompt_submit
  before_llm
  prompt
  chat_prompt
  session_start

After LLM / ingest:
  Stop
  after_llm
  assistant_message
  response

Tool events / ingest:
  PreToolUse
  PostToolUse
  tool_call
  tool_result
  PermissionRequest

Commit events / one-pass batch extraction:
  Stop
  SubagentStop
  PostCompact
  PreCompact
  session_end
  task_complete
  idle_timeout
```

## Agent Matrix

| Agent | Best MatrixArk integration | Notes |
|---|---|---|
| Codex Desktop / CLI | MCP plus hook adapter | MCP tools are explicit. Hooks can auto-capture prompt/tool/stop events when configured. |
| Claude Desktop | MCP | Add MatrixArk as an MCP stdio server. Claude keeps local context and calls MatrixArk for durable context. |
| Claude Code-style agents | MCP plus native hook command where available | Use hook adapter for prompt/tool/stop/compact lifecycle events. |
| Cursor | MCP first | Cursor should keep local code context and call MatrixArk for team memory, runbooks, prior decisions, and skills. |
| Windsurf | MCP first, external runner if available | Use the same MatrixArk MCP server; hook adapter works if the client can run external commands. |
| Cline / Roo-style agents | MCP first | These agent loops are tool-heavy; MatrixArk should be a durable memory/tool provider. |
| Continue | MCP or HTTP | Good fit for local IDE context plus MatrixArk durable memory. |
| OpenCode / OpenClaw-style agents | MCP plus hook adapter | Same model as OpenViking integrations: explicit context tools plus automatic lifecycle capture where available. |
| Aider | CLI wrapper or MCP bridge | Wrap the CLI invocation or use a supervising script to call MatrixArk before/after prompts. |
| Gemini CLI / Qwen Code | MCP or CLI wrapper | Use MatrixArk as a durable context tool; hook adapter works when wrapping command execution. |
| AutoGen / LangGraph / CrewAI | HTTP or Python-side hook calls | Integrate at graph node boundaries: before model call, after model call, after tool call, and workflow end. |
| LlamaIndex / Semantic Kernel | HTTP or tool plugin | Register MatrixArk as memory/context tool and call retrieve before synthesis. |
| Dify / n8n / workflow agents | HTTP nodes | Use retrieve/ingest/session_commit nodes in the workflow. |
| GitHub Copilot-style agents | HTTP/MCP gateway | Native lifecycle hooks are not a stable public boundary; integrate through extension/server-side workflow where available. |
| Enterprise vertical agents | MCP, HTTP, and hooks | Best target. They can send before-LLM, after-LLM, feedback, tool-result, idle, and session-commit events. |

## Claude Hook Example

Use these commands in the hook entries exposed by the Claude installation:

```bash
python3 /root/src/github-services/TemporalStore/tools/matrixark_agent_hook.py \
  --agent claude \
  --event UserPromptSubmit \
  --backend temporalstore-direct

python3 /root/src/github-services/TemporalStore/tools/matrixark_agent_hook.py \
  --agent claude \
  --event PostToolUse \
  --backend temporalstore-direct

python3 /root/src/github-services/TemporalStore/tools/matrixark_agent_hook.py \
  --agent claude \
  --event Stop \
  --backend temporalstore-direct
```

Expected payload fields can be any of:

```text
prompt
input
text
message
messages[].content
session_id
thread_id
conversation_id
workspace_root
cwd
open_files[]
tool_outputs[]
```

## Cursor Hook/Fallback Example

Cursor should primarily use MCP. If a project or enterprise wrapper can call an
external command before/after the agent loop, use:

```bash
python3 /root/src/github-services/TemporalStore/tools/matrixark_agent_hook.py \
  --agent cursor \
  --event UserPromptSubmit \
  --backend temporalstore-direct
```

Payload:

```json
{
  "prompt": "What did we decide about the GPU approval?",
  "session_id": "cursor-thread-123",
  "workspace_root": "/repo/acme",
  "open_files": [
    {"path": "infra/budget.md", "summary": "Current capex budget notes"}
  ]
}
```

## Cline / Roo / Continue Example

Use MCP as the default. If the agent can run a command after tool calls:

```bash
python3 /root/src/github-services/TemporalStore/tools/matrixark_agent_hook.py \
  --agent cline \
  --event PostToolUse \
  --backend temporalstore-direct
```

This captures durable tool outcomes and keeps raw tool output out of future
prompts unless it becomes relevant.

## Multi-Agent Framework Example

For AutoGen, LangGraph, CrewAI, LlamaIndex, Semantic Kernel, Dify, or n8n,
place MatrixArk at workflow boundaries:

```text
Before model node:
  call matrixark_retrieve(query, scope, local_context, max_context_tokens)

After model node:
  call matrixark_ingest(messages=[assistant answer], metadata.source=<framework>)

After tool node:
  call matrixark_ingest(messages=[tool result], metadata.event=tool_result)

At graph/workflow end:
  call matrixark_feedback and matrixark_session_commit
```

CLI wrapper pattern:

```bash
cat payload.json | python3 /root/src/github-services/TemporalStore/tools/matrixark_agent_hook.py \
  --agent langgraph \
  --event UserPromptSubmit \
  --backend temporalstore-direct
```

HTTP workflow node pattern:

```http
POST /v1/context/retrieve
Authorization: Bearer mk_live_...
Content-Type: application/json

{
  "query": "raw user or workflow task",
  "scope": {
    "account_id": "acct_prod",
    "tenant_id": "tenant_prod",
    "user_id": "user_123",
    "session_id": "workflow_run_456"
  },
  "max_context_tokens": 2048
}
```

## What The Hook Sends To MatrixArk

For a prompt event:

```json
{
  "messages": [
    {"role": "user", "content": "Alice approved the GPU request."}
  ],
  "scope": {
    "account_id": "acct_agent",
    "tenant_id": "tenant_agent",
    "user_id": "agent_user",
    "session_id": "claude:demo-thread"
  },
  "metadata": {
    "source": "claude_hook",
    "agent": "claude",
    "agent_event": "UserPromptSubmit"
  },
  "agent_hook": {
    "source": "claude",
    "hook_type": "before_llm",
    "auto_captured": true
  }
}
```

Then it calls `matrixark_retrieve` and returns a summary including:

```text
context_pack_id
selected_ref_count
used_context_tokens
```

## Why MCP Still Matters

Not every popular agent exposes reliable lifecycle hooks. MCP gives a stable
portable tool boundary. Hooks are best for automatic capture; MCP is best for
agent-controlled retrieval and feedback.

Recommended enterprise setup:

```text
MCP:
  retrieval, replay, resource lookup, skill lookup

hooks:
  automatic event ingestion, tool-result capture, session commit

HTTP:
  server-side vertical agents and SaaS backends
```
