#!/usr/bin/env python3
"""Universal MatrixArk hook adapter for popular AI agents.

The adapter accepts JSON hook payloads on stdin and normalizes them into
MatrixArk MCP tool calls.  It is intentionally permissive: different agents use
different lifecycle event names and payload shapes, so this script extracts the
best available text, scope, session id, local refs, and lifecycle stage.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_codex_hook import (
        build_server,
        call_tool,
        default_hook_backend,
        first_string_at,
        generated_session_id,
        hook_idempotency_key,
        payload_session_candidate,
        payload_text,
        read_stdin_payload,
        validate_hook_backend_policy,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_codex_hook import (  # type: ignore
        build_server,
        call_tool,
        default_hook_backend,
        first_string_at,
        generated_session_id,
        hook_idempotency_key,
        payload_session_candidate,
        payload_text,
        read_stdin_payload,
        validate_hook_backend_policy,
    )


Json = dict[str, Any]


BEFORE_LLM_EVENTS = {
    "userpromptsubmit",
    "user_prompt_submit",
    "beforellm",
    "before_llm",
    "prompt",
    "chatprompt",
    "chat_prompt",
    "sessionstart",
    "session_start",
}
AFTER_LLM_EVENTS = {
    "stop",
    "subagentstop",
    "subagent_stop",
    "afterllm",
    "after_llm",
    "assistantmessage",
    "assistant_message",
    "response",
}
TOOL_EVENTS = {
    "pretooluse",
    "pre_tool_use",
    "posttooluse",
    "post_tool_use",
    "toolcall",
    "tool_call",
    "toolresult",
    "tool_result",
    "permissionrequest",
    "permission_request",
}
COMMIT_EVENTS = {
    "stop",
    "subagentstop",
    "subagent_stop",
    "postcompact",
    "post_compact",
    "precompact",
    "pre_compact",
    "compact",
    "sessionend",
    "session_end",
    "taskcomplete",
    "task_complete",
    "idletimeout",
    "idle_timeout",
    "sessionidle",
    "session_idle",
}
RESOURCE_EVENTS = {
    "resourceadded",
    "resource_added",
    "addresource",
    "add_resource",
    "resource",
    "fileadded",
    "file_added",
    "documentadded",
    "document_added",
    "resourceimport",
    "resource_import",
    "skilladded",
    "skill_added",
}
FEEDBACK_EVENTS = {
    "feedback",
    "userfeedback",
    "user_feedback",
    "acceptrefs",
    "accept_refs",
    "acceptedrefs",
    "accepted_refs",
    "rejectrefs",
    "reject_refs",
    "rejectedrefs",
    "rejected_refs",
    "confirmation",
    "correction",
}

RESOURCE_TYPE_BY_SUFFIX = {
    ".md": "md",
    ".markdown": "md",
    ".txt": "txt",
    ".log": "log",
    ".html": "html",
    ".htm": "html",
    ".pdf": "pdf",
    ".docx": "docx",
    ".pptx": "pptx",
    ".xlsx": "xlsx",
    ".csv": "csv",
    ".tsv": "tsv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".webp": "image",
}


def norm(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum() or ch == "_")


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--agent",
        default=os.environ.get("MATRIXARK_AGENT", "generic"),
        help=(
            "Agent label for metadata/audit/session prefixes. Known labels include "
            "codex, claude, cursor, windsurf, cline, roo, continue, copilot, "
            "opencode, openclaw, aider, gemini, qwen-code, autogen, langgraph, "
            "crewai, llamaindex, semantic-kernel, dify, n8n, and generic."
        ),
    )
    parser.add_argument(
        "--event",
        default=os.environ.get("MATRIXARK_AGENT_EVENT")
        or os.environ.get("CODEX_HOOK_EVENT")
        or os.environ.get("CLAUDE_CODE_HOOK_EVENT")
        or "UserPromptSubmit",
    )
    parser.add_argument("--event-log", type=Path, default=Path(os.environ.get("MATRIXARK_AGENT_EVENT_LOG", "/tmp/matrixark-agent-hook.jsonl")))
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default=default_hook_backend() if default_hook_backend() != "temporalstore-rust" else "temporalstore-direct")
    parser.add_argument("--api-key", default=os.environ.get("MATRIXARK_API_KEY", ""))
    parser.add_argument("--account-id", default=os.environ.get("MATRIXARK_ACCOUNT_ID", "acct_agent"))
    parser.add_argument("--tenant-id", default=os.environ.get("MATRIXARK_TENANT_ID", "tenant_agent"))
    parser.add_argument("--user-id", default=os.environ.get("MATRIXARK_USER_ID", os.environ.get("USERNAME", "agent_user")))
    parser.add_argument("--session-id", default=os.environ.get("MATRIXARK_SESSION_ID"))
    parser.add_argument("--session-state-dir", type=Path, default=Path(os.environ.get("MATRIXARK_AGENT_SESSION_STATE_DIR", "/tmp/matrixark-agent-sessions")))
    parser.add_argument("--team", default=os.environ.get("MATRIXARK_TEAM", "agent"))
    parser.add_argument("--project", default=os.environ.get("MATRIXARK_PROJECT", "local"))
    parser.add_argument("--query", default="")
    parser.add_argument("--max-context-tokens", type=int, default=int(os.environ.get("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", "1024")))
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--temporalstore-lib", default=os.environ.get("TEMPORALSTORE_LIB", ""))
    parser.add_argument("--storage-prefix", default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:agent-hook"))
    parser.add_argument("--request-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "60000")))
    parser.add_argument("--io-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "60000")))
    parser.add_argument("--session-commit-threshold", type=int, default=int(os.environ.get("MATRIXARK_SESSION_COMMIT_THRESHOLD", "20")))
    parser.add_argument("--idle-commit-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", "0")))
    parser.add_argument("--understanding-provider", default=os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", "rules"))
    parser.add_argument("--segment-provider", default=os.environ.get("MATRIXARK_SEGMENT_PROVIDER", "deterministic"))
    parser.add_argument("--repo-root", type=Path, default=root)
    return parser.parse_args()


def role_for_agent_event(event: str) -> str:
    e = norm(event)
    if e in TOOL_EVENTS:
        return "tool"
    if e in AFTER_LLM_EVENTS or "assistant" in e or "response" in e:
        return "assistant"
    return "user"


def hook_type_for_agent_event(event: str) -> str:
    e = norm(event)
    if e in RESOURCE_EVENTS:
        return "resource_added"
    if e in FEEDBACK_EVENTS:
        return "feedback"
    if e in TOOL_EVENTS:
        return "tool_result"
    if e in COMMIT_EVENTS:
        return "session_commit" if "idle" in e or "compact" in e else "after_llm"
    if e in AFTER_LLM_EVENTS:
        return "after_llm"
    return "before_llm"


def should_retrieve(event: str) -> bool:
    e = norm(event)
    return e in BEFORE_LLM_EVENTS or "prompt" in e or "before" in e


def should_commit(event: str) -> bool:
    return norm(event) in COMMIT_EVENTS


def is_resource_event(event: str) -> bool:
    return norm(event) in RESOURCE_EVENTS


def is_feedback_event(event: str) -> bool:
    return norm(event) in FEEDBACK_EVENTS


def commit_reason(event: str) -> str:
    e = norm(event)
    if "idle" in e:
        return "idle_timeout"
    if "compact" in e or "stop" in e or "complete" in e or "end" in e:
        return "hook_boundary"
    return "manual_api"


def resolve_session_id(payload: Json, args: argparse.Namespace) -> tuple[str, str]:
    if args.session_id:
        return args.session_id, "explicit"

    candidate = first_string_at(
        payload,
        [
            ["session_id"],
            ["sessionId"],
            ["thread_id"],
            ["threadId"],
            ["conversation_id"],
            ["conversationId"],
            ["chat_id"],
            ["chatId"],
            ["workspace_id"],
            ["workspaceId"],
            ["transcript_id"],
            ["transcriptId"],
            ["cwd"],
            ["workspace_root"],
            ["workspaceRoot"],
            ["project_root"],
            ["projectRoot"],
            ["params", "session_id"],
            ["params", "thread_id"],
            ["params", "conversation_id"],
            ["metadata", "session_id"],
            ["metadata", "thread_id"],
        ],
    )
    if candidate:
        return f"{args.agent}:{candidate}", "payload_field"

    codex_candidate, source = payload_session_candidate(payload)
    if codex_candidate:
        return codex_candidate.replace("codex:", f"{args.agent}:", 1), source

    generated, source = generated_session_id(payload, args)
    return generated.replace("codex:", f"{args.agent}:", 1), source


def local_context_from_payload(payload: Json) -> list[Json]:
    refs: list[Json] = []
    containers = [payload]
    for nested_key in ("params", "turn", "metadata", "context"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            containers.append(nested)
    for container in containers:
        for key in ("local_context", "context", "open_files", "active_files", "files", "buffers", "selected_text", "selection", "tool_outputs", "terminal_output"):
            value = container.get(key)
            items = value if isinstance(value, list) else [value] if value else []
            for item in items[:24]:
                if isinstance(item, str):
                    refs.append({"ref": key, "text": item[:1200]})
                elif isinstance(item, dict):
                    text = first_string_at(item, [["text"], ["content"], ["summary"], ["snippet"], ["selected_text"], ["output"], ["path"], ["uri"]])
                    ref = first_string_at(item, [["ref"], ["path"], ["uri"], ["url"], ["name"], ["file"], ["relative_path"]])
                    kind = first_string_at(item, [["kind"], ["type"], ["mime_type"], ["language"]])
                    if text or ref:
                        compact = {"ref": ref or key, "text": text[:1200]}
                        if kind:
                            compact["kind"] = kind
                        refs.append(compact)
                if len(refs) >= 24:
                    return refs[:24]
    return refs[:24]


def agent_context_from_payload(payload: Json, *, agent: str, event: str, session_id_source: str, args: argparse.Namespace) -> Json:
    workspace = first_string_at(
        payload,
        [
            ["workspace_root"],
            ["workspaceRoot"],
            ["project_root"],
            ["projectRoot"],
            ["cwd"],
            ["params", "cwd"],
            ["metadata", "cwd"],
        ],
    )
    current_url = first_string_at(payload, [["url"], ["current_url"], ["browser_url"], ["metadata", "url"]])
    tool_name = first_string_at(payload, [["tool_name"], ["toolName"], ["tool", "name"], ["params", "tool_name"]])
    tool_status = first_string_at(payload, [["tool_status"], ["status"], ["tool", "status"], ["params", "status"]])
    return {
        "agent": agent,
        "event": event,
        "session_id_source": session_id_source,
        "workspace_root": workspace or str(args.repo_root),
        "current_url": current_url,
        "tool_name": tool_name,
        "tool_status": tool_status,
        "local_context": local_context_from_payload(payload),
        "payload_keys": sorted(str(key) for key in payload.keys())[:80],
    }


def payload_resource_uri(payload: Json) -> str:
    return first_string_at(
        payload,
        [
            ["raw_uri"],
            ["rawUri"],
            ["uri"],
            ["url"],
            ["path"],
            ["file_path"],
            ["filePath"],
            ["resource_path"],
            ["resourcePath"],
            ["document_path"],
            ["documentPath"],
            ["params", "raw_uri"],
            ["params", "uri"],
            ["params", "path"],
            ["metadata", "raw_uri"],
            ["metadata", "uri"],
            ["metadata", "path"],
        ],
    )


def payload_resource_type(payload: Json, raw_uri: str) -> str:
    direct = first_string_at(
        payload,
        [
            ["resource_type"],
            ["resourceType"],
            ["type"],
            ["mime_type"],
            ["mimeType"],
            ["params", "resource_type"],
            ["params", "resourceType"],
            ["metadata", "resource_type"],
            ["metadata", "resourceType"],
        ],
    )
    if direct:
        value = direct.strip().lower()
        if "/" in value:
            value = value.split("/")[-1]
        return value
    suffix = Path(raw_uri).suffix.lower()
    return RESOURCE_TYPE_BY_SUFFIX.get(suffix, "skill" if Path(raw_uri).name.lower() == "skill.md" else "")


def payload_list(payload: Json, keys: list[str]) -> list[str]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [str(item) for item in value if str(item)]
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        for key in keys:
            value = metadata.get(key)
            if isinstance(value, list):
                return [str(item) for item in value if str(item)]
    return []


def scope_from_args(args: argparse.Namespace) -> Json:
    return {
        "account_id": args.account_id,
        "tenant_id": args.tenant_id,
        "user_id": args.user_id,
        "session_id": args.session_id,
        "team": args.team,
        "project": args.project,
    }


def main() -> int:
    args = parse_args()
    validate_hook_backend_policy(args.backend)
    payload = read_stdin_payload()
    args.session_id, session_id_source = resolve_session_id(payload, args)
    text = payload_text(payload) or args.query
    raw_uri = payload_resource_uri(payload)
    resource_type = payload_resource_type(payload, raw_uri) if raw_uri else ""
    event_hook_type = hook_type_for_agent_event(args.event)
    if not text and not raw_uri and not should_commit(args.event):
        print(json.dumps({"status": "skipped", "reason": "empty hook payload", "agent": args.agent}))
        return 0
    agent_context = agent_context_from_payload(payload, agent=args.agent, event=args.event, session_id_source=session_id_source, args=args)

    server = build_server(args)
    scope = scope_from_args(args)
    common: Json = {"scope": scope}
    if args.api_key:
        common["api_key"] = args.api_key

    hook_meta: Json = {
        "source": args.agent,
        "hook_type": event_hook_type,
        "hook_id": f"{args.agent}:{args.event}:{int(time.time() * 1000)}",
        "observed_at_ms": int(time.time() * 1000),
        "idempotency_key": hook_idempotency_key(payload, event=args.event, session_id=args.session_id, fallback=raw_uri),
        "trigger": args.event,
        "auto_captured": True,
        "session_id_source": session_id_source,
    }
    base_metadata: Json = {
        "source": f"{args.agent}_hook",
        "agent": args.agent,
        "agent_event": args.event,
        "raw_hook_payload": payload,
        "agent_context": agent_context,
        "session_id_source": session_id_source,
    }

    ingest: Json = {}
    feedback: Json = {}
    if is_feedback_event(args.event):
        feedback_args: Json = {
            **common,
            "messages": [{"role": role_for_agent_event(args.event), "content": text or json.dumps(payload, sort_keys=True)[:1000]}],
            "metadata": base_metadata,
            "context_pack_id": first_string_at(payload, [["context_pack_id"], ["contextPackId"], ["metadata", "context_pack_id"]]),
            "accepted_refs": payload_list(payload, ["accepted_refs", "acceptedRefs"]),
            "rejected_refs": payload_list(payload, ["rejected_refs", "rejectedRefs"]),
            "understanding_provider": args.understanding_provider,
            "segment_provider": args.segment_provider,
            "agent_hook": hook_meta,
        }
        feedback = call_tool(server, "matrixark_feedback", feedback_args)
    elif is_resource_event(args.event) or raw_uri:
        kind = "skill" if resource_type == "skill" or Path(raw_uri).name.lower() == "skill.md" or norm(args.event).startswith("skill") else "resource"
        ingest_args = {
            **common,
            "kind": kind,
            "messages": [{"role": "user", "content": text or f"{kind} added: {raw_uri}"}],
            "raw_uri": raw_uri or "inline-resource",
            "resource_type": resource_type or kind,
            "metadata": {**base_metadata, "raw_uri": raw_uri, "resource_type": resource_type or kind},
            "understanding_provider": args.understanding_provider,
            "segment_provider": args.segment_provider,
            "agent_hook": hook_meta,
            "wait": bool(payload.get("wait", True)),
        }
        ingest = call_tool(server, "matrixark_ingest", ingest_args)
    elif text:
        ingest_args = {
            **common,
            "messages": [{"role": role_for_agent_event(args.event), "content": text}],
            "understanding_provider": args.understanding_provider,
            "segment_provider": args.segment_provider,
            "metadata": base_metadata,
            "agent_hook": hook_meta,
        }
        if should_retrieve(args.event):
            ingest_args["auto_batch_extract"] = True
            ingest_args["session_buffer_threshold"] = args.session_commit_threshold
            if args.idle_commit_timeout_ms > 0:
                ingest_args["idle_commit_timeout_ms"] = args.idle_commit_timeout_ms
        ingest = call_tool(server, "matrixark_ingest", ingest_args)

    retrieve: Json = {}
    if should_retrieve(args.event) or args.query:
        retrieve_args: Json = {
            **common,
            "query": args.query or text[:500],
            "max_context_tokens": args.max_context_tokens,
        }
        local_context = agent_context.get("local_context", [])
        if local_context:
            retrieve_args["local_context"] = local_context
        retrieve = call_tool(server, "matrixark_retrieve", retrieve_args)

    commit: Json = {}
    if should_commit(args.event):
        reason = commit_reason(args.event)
        commit = call_tool(
            server,
            "matrixark_session_commit",
            {
                **common,
                "threshold_messages": args.session_commit_threshold,
                "force": reason != "idle_timeout",
                "commit_reason": reason,
                "understanding_provider": args.understanding_provider,
                "segment_provider": args.segment_provider,
                **({"idle_timeout_ms": args.idle_commit_timeout_ms} if reason == "idle_timeout" else {}),
                "agent_hook": {
                    "source": args.agent,
                    "hook_type": "session_commit",
                    "hook_id": f"{args.agent}:session_commit:{args.event}:{int(time.time() * 1000)}",
                    "observed_at_ms": int(time.time() * 1000),
                    "idempotency_key": hook_idempotency_key(payload, event=f"session_commit:{args.event}", session_id=args.session_id),
                    "trigger": args.event,
                    "auto_captured": True,
                    "session_id_source": session_id_source,
                },
            },
        )

    print(
        json.dumps(
            {
                "status": "ok",
                "agent": args.agent,
                "event": args.event,
                "session_id": args.session_id,
                "session_id_source": session_id_source,
                "agent_context_refs": len(agent_context.get("local_context", [])),
                "workspace_root": agent_context.get("workspace_root", ""),
                "ingested": bool(ingest),
                "feedbacked": bool(feedback),
                "resource_uri": raw_uri,
                "resource_type": resource_type,
                "retrieved": {
                    "context_pack_id": retrieve.get("context_pack_id"),
                    "selected_ref_count": len(retrieve.get("selected_refs", [])) if retrieve else 0,
                    "used_context_tokens": retrieve.get("used_context_tokens", 0) if retrieve else 0,
                },
                "committed": {
                    "status": commit.get("status"),
                    "commit_reason": commit.get("commit_reason"),
                    "segments_written": commit.get("segments_written", 0),
                    "entities_written": commit.get("entities_written", 0),
                } if commit else {},
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
