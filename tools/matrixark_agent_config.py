#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Generate MatrixArk agent integration snippets.

The MatrixArk MCP server is the common boundary for Codex today.
Claude Code, Cursor, OpenClaw, OpenCode, Aider, Continue, Cline/Roo, and
generic agents remain TODO/planned integrations until their hook payloads and
registration flows are validated.
"""

from __future__ import annotations

import argparse
import json


DEFAULT_REPO_ROOT = "."
DEFAULT_LAUNCHER = "tools/matrixark_mcp_rust_server.sh"
SUPPORTED_AGENT_CLIENTS = ["codex", "claude"]
SUPPORTED_HOOK_CLIENTS = ["codex", "claude"]
TODO_AGENT_CLIENTS = [
    "cursor",
    "openclaw",
    "opencode",
    "aider",
    "continue",
    "cline",
    "roo",
    "generic",
]
LIFECYCLE_TOOLS = {
    "before_llm": "matrixark_retrieve",
    "after_answer": "matrixark_ingest",
    "after_tool": "matrixark_ingest",
    "resource_added": "matrixark_ingest",
    "skill_added": "matrixark_ingest",
    "feedback": "matrixark_feedback",
    "session_boundary": "matrixark_session_commit",
}
LIFECYCLE_ACTIONS = {
    "before_llm": "retrieve",
    "after_answer": "ingest_durable_outcome",
    "after_tool": "ingest_durable_outcome",
    "resource_added": "import_resource_or_skill",
    "skill_added": "import_resource_or_skill",
    "feedback": "record_accepted_rejected_refs",
    "session_boundary": "commit_batch_extract",
}
MEMORY_EXTRACTION_POLICY = {
    "live_ingest": {
        "events": ["UserPromptSubmit", "AssistantResponse", "PostToolUse"],
        "action": "append_visible_message_or_tool_evidence",
        "phase": "raw",
    },
    "threshold_checkpoint": {
        "trigger": "message_count_or_token_threshold",
        "tool": "matrixark_session_commit",
        "extraction_phase": "provisional",
        "final_session_boundary": False,
    },
    "idle_checkpoint": {
        "trigger": "idle_timeout",
        "tool": "matrixark_session_commit",
        "extraction_phase": "provisional",
        "final_session_boundary": False,
    },
    "final_boundary": {
        "events": ["Stop", "SubagentStop", "PostCompact"],
        "tool": "matrixark_session_commit",
        "extraction_phase": "final",
        "final_session_boundary": True,
    },
}
RETRIEVAL_BUDGET_POLICY = {
    "local_context_first": True,
    "remote_fills_remaining_budget": True,
    "provisional_memory_confidence": "lower_than_final",
    "debug_fields": ["include_retrieval_metrics", "include_retrieval_debug", "debug_context_pack"],
    "debug_default": "off",
}


def agent_envelope_schema() -> dict[str, object]:
    return {
        "schema": "matrixark_agent_envelope_v1",
        "visible_local_context_only": True,
        "fields": [
            "query",
            "scope",
            "local_context",
            "local_context_tokens",
            "max_context_tokens",
            "lifecycle_event_type",
            "file_refs",
            "resource_refs",
        ],
        "required_fields_by_lifecycle": {
            "before_llm": ["query"],
            "after_answer": ["messages"],
            "after_tool": ["messages"],
            "resource_added": ["file_refs or resource_refs or raw_uri"],
            "skill_added": ["file_refs or resource_refs or raw_uri"],
            "feedback": ["accepted_refs or rejected_refs"],
            "session_boundary": ["scope"],
        },
        "optional_fields": ["scope", "local_context", "local_context_tokens", "max_context_tokens", "file_refs", "resource_refs"],
        "scope_fields": ["account_id", "tenant_id", "user_id", "session_id", "team", "project"],
        "local_context_examples": [
            "open files",
            "selected text",
            "visible tool outputs",
            "terminal summaries",
            "browser/page refs",
        ],
        "file_ref_examples": [
            {"path": "docs/runbook.md", "kind": "md", "visibility": "session"},
            {"path": "approval_packet.pdf", "kind": "pdf", "visibility": "tenant_shared"},
        ],
        "resource_ref_examples": [
            {"raw_uri": "file:///workspace/docs/runbook.md", "resource_type": "md"},
            {"raw_uri": "s3://matrixark-resources/tenant/runbook.pdf", "resource_type": "pdf"},
        ],
        "do_not_send": ["hidden prompt", "system prompt", "private model chain-of-thought"],
        "lifecycle_tools": dict(LIFECYCLE_TOOLS),
        "lifecycle_actions": dict(LIFECYCLE_ACTIONS),
        "memory_extraction_policy": dict(MEMORY_EXTRACTION_POLICY),
        "retrieval_budget_policy": dict(RETRIEVAL_BUDGET_POLICY),
        "agent_internal_model_hidden": [
            "ContextEvent",
            "ContextEntity",
            "ContextSummary",
            "ContextEmbedding",
            "ContextIndex",
            "ResourceChunk",
            "SkillSection",
        ],
    }


def wsl_args(repo_root: str, launcher: str) -> list[str]:
    return [
        "--cd",
        repo_root,
        "-e",
        "bash",
        "-lc",
        f"exec {launcher}",
    ]


def stdio_server(repo_root: str, launcher: str) -> dict[str, object]:
    return {
        "command": "wsl.exe",
        "args": wsl_args(repo_root, launcher),
        "env": {
            "MATRIXARK_LOCAL_MODE": "no-metaserver",
            "MATRIXARK_MCP_BACKEND": "temporalstore-rust",
            # No MATRIXARK_RETRIEVAL_TIMEOUT_MS here. It used to write "5000", which is the
            # exact ceiling matrixark_mcp_server.py raised itself off: a cold-start proxy
            # scans the whole serving-record set before scoring, routinely passed 5000 ms,
            # and the server then discarded the real ContextPack for an empty fallback --
            # refs computed and dropped. Writing it back here put that ceiling into every
            # config this generates. The launcher supplies the budget for this path.
            "MATRIXARK_RUST_PROXY_ASYNC_STORAGE": "true",
        },
    }


def codex_toml(repo_root: str, launcher: str) -> str:
    args = ", ".join(f"'{item}'" for item in wsl_args(repo_root, launcher))
    return "\n".join(
        [
            "[mcp_servers.matrixark]",
            "command = 'wsl.exe'",
            f"args = [ {args} ]",
            "startup_timeout_sec = 120",
            "",
        ]
    )


def claude_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {"mcpServers": {"matrixark": stdio_server(repo_root, launcher)}},
        indent=2,
        sort_keys=True,
    )


def claude_code_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {
            "matrixark": stdio_server(repo_root, launcher),
            "status": "supported",
            "hook_status": "supported",
            "recommended_hook_command": f"{repo_root}/tools/matrixark_claude_hook.sh --event UserPromptSubmit",
            "usage": "Register the full Claude Code lifecycle via integrations/agent-hooks/install/install.sh --agent claude (WSL) or install.ps1 -Agent claude (native Windows). See docs/matrixark_claude_hook_integration.md.",
        },
        indent=2,
        sort_keys=True,
    )


def cursor_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {"mcpServers": {"matrixark": stdio_server(repo_root, launcher)}},
        indent=2,
        sort_keys=True,
    )


def generic_json(repo_root: str, launcher: str, *, agent: str = "generic") -> str:
    return json.dumps(
        {
            "name": "matrixark",
            "agent": agent,
            "transport": "stdio",
            "server": stdio_server(repo_root, launcher),
            "envelope": agent_envelope_schema(),
            "required_tools": [
                "matrixark_retrieve",
                "matrixark_ingest",
                "matrixark_feedback",
                "matrixark_session_commit",
            ],
        },
        indent=2,
        sort_keys=True,
    )


def openclaw_json(repo_root: str, launcher: str) -> str:
    return json.dumps(
        {
            "name": "matrixark",
            "agent": "openclaw",
            "transport": "stdio",
            "server": stdio_server(repo_root, launcher),
            "envelope": agent_envelope_schema(),
            "hook_status": "todo_planned",
            "todo": "Validate OpenClaw/OpenCode hook payloads and registration before enabling production hook commands.",
            "required_tools": [
                "matrixark_retrieve",
                "matrixark_ingest",
                "matrixark_feedback",
                "matrixark_session_commit",
            ],
        },
        indent=2,
        sort_keys=True,
    )


def named_agent_json(agent: str, repo_root: str, launcher: str) -> str:
    payload = json.loads(generic_json(repo_root, launcher, agent=agent))
    payload["hook_status"] = "todo_planned"
    payload["todo"] = f"Validate {agent} hook payloads and registration before enabling production hook commands."
    return json.dumps(payload, indent=2, sort_keys=True)


def agent_policy_text() -> str:
    envelope = json.dumps(agent_envelope_schema(), indent=2, sort_keys=True)
    return f"""# MatrixArk Agent Policy

Use MatrixArk as durable remote context. Keep using your native local context
for currently open files, active buffers, terminal output, and immediate tool
results.

Before answering, call matrixark_retrieve when the request may depend on:
- prior decisions, approvals, incidents, debugging attempts, or user memory
- shared resources, runbooks, skills, team/project history, or cross-device context
- current-state facts that may have superseded older facts

Pass:
- query: the raw user request
- scope: account_id/tenant_id plus user_id and preferably session_id when known
- local_context: visible open files, selected text, tool output, terminal summaries, browser/page refs
- local_context_tokens: estimated visible local-context tokens when known
- max_context_tokens: remaining prompt budget for local plus remote context when known
- file_refs/resource_refs: optional visible files or raw_uri resources to import,
  such as a local PDF/Markdown file or an S3 resource URI

Do not send hidden/internal prompt context. Send only visible user/workspace
context. MatrixArk dedupes local refs and fills the remaining budget with remote
events, entities, resources, skills, and summaries.

After answering or using tools, call matrixark_ingest when the turn produced
durable information:
- accepted or rejected refs
- final answer summary
- correction, confirmation, decision, approval, commitment, or tool outcome

Use matrixark_feedback for accepted/rejected refs when the host agent has
explicit feedback signals.

At task/session boundaries, call matrixark_session_commit so MatrixArk can run
one-pass batch extraction over the same-session buffer.

For live Codex memory, do not wait only for Stop. PromptSubmit, assistant
responses, and selected tool evidence are ingested immediately. Message/token
thresholds and idle timeouts call matrixark_session_commit as provisional
checkpoints with extraction_phase=provisional and final_session_boundary=false.
Stop, SubagentStop, and PostCompact call matrixark_session_commit as the final
session boundary with extraction_phase=final and final_session_boundary=true.
Retrieval may use provisional memories during long sessions, but agents should
treat final memories and summaries as higher-confidence durable memory.

MatrixArk decides the route from the payload:
- before_llm/query -> matrixark_retrieve
- after_llm/tool_result -> matrixark_ingest durable answer/tool evidence
- ResourceAdded/SkillAdded/raw_uri -> resource or skill import task through matrixark_ingest
- Feedback/accepted_refs/rejected_refs -> matrixark_feedback
- threshold/idle -> matrixark_session_commit provisional batch extraction
- Stop/SubagentStop/PostCompact -> matrixark_session_commit final batch extraction

Lifecycle actions:
- before_llm: retrieve
- after_answer/tool: ingest durable outcome
- resource_added/skill_added: import resource or skill
- feedback: record accepted/rejected refs
- session_boundary: commit/batch extract

Agents do not need to understand ContextEvent, ContextEntity, ContextSummary,
ContextEmbedding, ContextIndex, ResourceChunk, or SkillSection internals. They
send the envelope below; MatrixArk resolves scope, routing, retrieval budget,
resource import, feedback, and session commit policy. Retrieval keeps visible
local context plus a safety margin first, then fills only the remaining remote
budget. Retrieval metrics and debug ContextPacks are opt-in audit fields, not
default hot-path payload.

```json
{envelope}
```
"""


def hook_examples_text(repo_root: str) -> str:
    hook = f"{repo_root}/tools/matrixark_agent_hook.py"
    return f"""# MatrixArk Agent Hook Examples

# Supported today: Codex and Claude Code hooks.
python3 {hook} --agent codex --event UserPromptSubmit
python3 {hook} --agent codex --event PostToolUse
python3 {hook} --agent codex --event Stop
python3 {hook} --agent codex --event ResourceAdded
python3 {hook} --agent codex --event Feedback

# Claude Code (supported): drive the same ingest/extract/retrieve pipeline as codex.
python3 {hook} --agent claude --event UserPromptSubmit
python3 {hook} --agent claude --event PostToolUse
python3 {hook} --agent claude --event Stop
# Or install the full Claude Code lifecycle wrapper:
#   integrations/agent-hooks/install/install.sh --agent claude   (WSL)
#   integrations/agent-hooks/install/install.ps1 -Agent claude   (native Windows)

# TODO/planned, not production-supported yet:
# - Cursor / Windsurf / Cline / Roo / Continue hook registration
# - OpenCode / OpenClaw hook registration
# - Aider, Gemini/Qwen Code, AutoGen, LangGraph, CrewAI, LlamaIndex,
#   Semantic Kernel, Dify, n8n, and generic agent hook payload mapping
#
# Keep these agents on MCP/manual integration until their lifecycle payloads,
# session identity, and hook reload behavior are validated end to end.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--client",
        choices=[
            "codex",
            "claude",
            "claude-code",
            "cursor",
            "openclaw",
            "opencode",
            "aider",
            "continue",
            "cline",
            "roo",
            "generic",
            "policy",
            "hooks",
            "all",
        ],
        default="all",
        help="Config snippet to print.",
    )
    parser.add_argument("--repo-root", default=DEFAULT_REPO_ROOT)
    parser.add_argument("--launcher", default=DEFAULT_LAUNCHER)
    args = parser.parse_args()

    generators = {
        "codex": ("# Codex config.toml", codex_toml),
        "claude": ("# Claude Desktop claude_desktop_config.json", claude_json),
        "claude-code": ("# Claude Code MCP server entry", claude_code_json),
        "cursor": ("# Cursor MCP config", cursor_json),
        "openclaw": ("# OpenClaw / OpenCode-style MCP plus hook config", openclaw_json),
        "generic": ("# Generic MCP stdio config", generic_json),
    }
    for agent in ("opencode", "aider", "continue", "cline", "roo"):
        generators[agent] = (f"# {agent} MatrixArk MCP plus hook config", lambda repo_root, launcher, agent=agent: named_agent_json(agent, repo_root, launcher))
    if args.client == "policy":
        print(agent_policy_text())
        return 0
    if args.client == "hooks":
        print(hook_examples_text(args.repo_root))
        return 0
    selected = generators if args.client == "all" else {args.client: generators[args.client]}
    blocks: list[str] = []
    for _, (title, generator) in selected.items():
        blocks.append(title)
        blocks.append(generator(args.repo_root, args.launcher))
    print("\n".join(blocks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
