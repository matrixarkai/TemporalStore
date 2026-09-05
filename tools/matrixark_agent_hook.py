#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
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
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:
    from tools.matrixark_codex_hook import (
        build_server,
        call_tool,
        close_server_best_effort,
        auto_batch_decision_summary,
        codex_retrieve_cross_session_options,
        codex_retrieve_question_type,
        codex_retrieve_ranking_options,
        default_hook_backend,
        first_string_at,
        generated_session_id,
        hook_idempotency_key,
        is_synthetic_hook_text,
        payload_session_candidate,
        payload_text,
        read_stdin_payload,
        retrieval_budget_pressure_from_retrieve,
        retrieval_budget_summary_from_retrieve,
        retrieval_layer_summary_from_retrieve,
        retrieval_memory_hierarchy_contract_from_retrieve,
        codex_memory_selection_metadata,
        selected_assistant_memory_text,
        selected_tool_evidence_text,
        selected_user_prompt_memory_text,
        session_commit_summary,
        selected_ref_count_from_retrieve,
        used_context_tokens_from_retrieve,
        validate_hook_backend_policy,
    )
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_codex_hook import (  # type: ignore
        build_server,
        call_tool,
        close_server_best_effort,
        auto_batch_decision_summary,
        codex_retrieve_cross_session_options,
        codex_retrieve_question_type,
        codex_retrieve_ranking_options,
        default_hook_backend,
        first_string_at,
        is_synthetic_hook_text,
        generated_session_id,
        hook_idempotency_key,
        payload_session_candidate,
        payload_text,
        read_stdin_payload,
        retrieval_budget_pressure_from_retrieve,
        retrieval_budget_summary_from_retrieve,
        retrieval_layer_summary_from_retrieve,
        retrieval_memory_hierarchy_contract_from_retrieve,
        codex_memory_selection_metadata,
        selected_assistant_memory_text,
        selected_tool_evidence_text,
        selected_user_prompt_memory_text,
        session_commit_summary,
        selected_ref_count_from_retrieve,
        used_context_tokens_from_retrieve,
        validate_hook_backend_policy,
    )


Json = dict[str, Any]
DEFAULT_IDLE_COMMIT_TIMEOUT_MS = 120_000


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
    "idle",
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
    parser.add_argument("--backend", choices=["temporalstore-direct", "temporalstore-rust", "temporalstore-rust-direct"], default=default_hook_backend())
    parser.add_argument("--api-key", default=os.environ.get("MATRIXARK_API_KEY", ""))
    parser.add_argument("--account-id", default=os.environ.get("MATRIXARK_ACCOUNT_ID", "acct_agent"))
    parser.add_argument("--tenant-id", default=os.environ.get("MATRIXARK_TENANT_ID", "tenant_agent"))
    parser.add_argument("--user-id", default=os.environ.get("MATRIXARK_USER_ID", os.environ.get("USERNAME", "agent_user")))
    parser.add_argument("--session-id", default=os.environ.get("MATRIXARK_SESSION_ID"))
    parser.add_argument("--session-state-dir", type=Path, default=Path(os.environ.get("MATRIXARK_AGENT_SESSION_STATE_DIR", "/tmp/matrixark-agent-sessions")))
    parser.add_argument("--team", default=os.environ.get("MATRIXARK_TEAM", "agent"))
    parser.add_argument("--project", default=os.environ.get("MATRIXARK_PROJECT", "local"))
    parser.add_argument("--query", default="")
    parser.add_argument("--max-context-tokens", type=int, default=int(os.environ.get("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", os.environ.get("MATRIXARK_DEFAULT_MAX_CONTEXT_TOKENS", "128000"))))
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--temporalstore-lib", default=os.environ.get("TEMPORALSTORE_LIB", ""))
    parser.add_argument("--rust-proxy", default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_PROXY", os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", "")))
    parser.add_argument("--rust-direct-sdk", default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK", ""))
    parser.add_argument("--rust-cli", default=os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_CLI", ""))
    parser.add_argument("--storage-prefix", default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:agent-hook"))
    parser.add_argument("--request-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "60000")))
    parser.add_argument("--io-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "60000")))
    parser.add_argument("--session-commit-threshold", type=int, default=int(os.environ.get("MATRIXARK_SESSION_COMMIT_THRESHOLD", "20")))
    parser.add_argument("--idle-commit-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", str(DEFAULT_IDLE_COMMIT_TIMEOUT_MS))))
    parser.add_argument("--idle-commit-cutoff-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_CUTOFF_MS", "0") or 0))
    parser.add_argument("--idle-commit-worker-only", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--understanding-provider", default=os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", "rules"))
    parser.add_argument("--segment-provider", default=os.environ.get("MATRIXARK_SEGMENT_PROVIDER", "deterministic"))
    parser.add_argument("--repo-root", type=Path, default=root)
    parser.add_argument(
        "--require-retrieval-memory-coverage",
        action="store_true",
        help="Exit nonzero when retrieval runs but no session/profile memory layer is selected.",
    )
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


def should_auto_batch_extract_on_ingest(event: str) -> bool:
    return not should_commit(event)


def agent_retrieve_args(
    common: Json,
    args: argparse.Namespace,
    *,
    query: str,
    retrieve_metadata: Json,
    local_context: list[Any],
) -> Json:
    question_type = codex_retrieve_question_type(query)
    retrieve_args: Json = {
        **common,
        "query": query,
        "question_type": question_type,
        "max_context_tokens": args.max_context_tokens,
        "session_buffer_threshold": args.session_commit_threshold,
        "idle_commit_timeout_ms": args.idle_commit_timeout_ms,
        "storage_options": hook_storage_options(),
        "ranking": codex_retrieve_ranking_options(),
        "cross_session": codex_retrieve_cross_session_options(query),
        "understanding_provider": getattr(args, "understanding_provider", None),
        "extraction_provider": getattr(args, "extraction_provider", None),
        "segment_provider": getattr(args, "segment_provider", None),
        "segment_model": getattr(args, "segment_model", None),
        "segment_model_path": getattr(args, "segment_model_path", None),
        "segment_max_new_tokens": getattr(args, "segment_max_new_tokens", None),
        "segment_provider_fallback": getattr(args, "segment_provider_fallback", None),
        "skip_prior_context": bool(getattr(args, "skip_prior_context", False)),
        "metadata": {
            **retrieve_metadata,
            "question_type": question_type,
        },
        "retrieval_request_metadata": {
            **retrieve_metadata,
            "question_type": question_type,
        },
    }
    if local_context:
        retrieve_args["local_context"] = local_context
    # Flag synthetic/debug probes so the retrieve budget policy serves them from the remote
    # TemporalStore pack only (no local reservation); real prompts keep local + remote.
    retrieve_args["synthetic"] = is_synthetic_hook_text(query)
    return retrieve_args


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


def retrieval_session_identity_from_retrieve(pack: Json | None, *, session_id_source: str = "") -> Json:
    if not isinstance(pack, dict):
        source = str(session_id_source or "")
        return {
            "session_id_source": source,
            "strong_session_identity": source in {"explicit", "payload_field", "payload_path_hash"} or source.startswith(("payload.", "env.")),
            "fallback_session_identity": source in {"state_file", "state_file_created", "workspace_hash"},
            "risk": "workspace_fallback_may_merge_multiple_codex_tasks" if source in {"state_file", "state_file_created", "workspace_hash"} else "",
        } if source else {}
    recall_policy = pack.get("recall_policy") if isinstance(pack.get("recall_policy"), dict) else {}
    session_identity = recall_policy.get("session_identity") if isinstance(recall_policy.get("session_identity"), dict) else {}
    if isinstance(session_identity, dict) and session_identity.get("session_id_source"):
        return session_identity
    source = str(session_id_source or "")
    return {
        "session_id_source": source,
        "strong_session_identity": source in {"explicit", "payload_field", "payload_path_hash"} or source.startswith(("payload.", "env.")),
        "fallback_session_identity": source in {"state_file", "state_file_created", "workspace_hash"},
        "risk": "workspace_fallback_may_merge_multiple_codex_tasks" if source in {"state_file", "state_file_created", "workspace_hash"} else "",
        "source": "hook_metadata_fallback",
    } if source else {}


def retrieval_quality_warnings_from_retrieve(pack: Json | None) -> list[Any]:
    if not isinstance(pack, dict):
        return []
    warnings = pack.get("quality_warnings")
    return warnings if isinstance(warnings, list) else []


HOOK_MESSAGE_ROLE_ALIASES = {
    "assistant_response": "assistant",
    "agent": "assistant",
    "ai": "assistant",
    "bot": "assistant",
    "llm": "assistant",
    "model": "assistant",
    "tool_result": "tool",
    "tool-output": "tool",
    "tooloutput": "tool",
    "tool_output": "tool",
    "function": "tool",
    "function_call_output": "tool",
    "custom_tool_call_output": "tool",
    "tool_call_output": "tool",
    "human": "user",
    "prompt": "user",
}


def normalized_hook_message_role(role: str, *, event: str) -> str:
    role_name = role_for_agent_event(event) if not str(role or "").strip() else str(role or "").strip()
    return HOOK_MESSAGE_ROLE_ALIASES.get(role_name.lower(), role_name.lower())


def hook_messages_from_payload(payload: Json, *, event: str, text: str) -> list[Json]:
    def message_for_role(role: str, content: str, *, original_content: str | None = None) -> Json:
        normalized_role = normalized_hook_message_role(role, event=event)
        selected_content = compact_for_role(normalized_role, content)
        message: Json = {"role": normalized_role, "content": selected_content}
        selection = codex_memory_selection_metadata(
            role=normalized_role,
            event=event,
            text=selected_content,
            original_text=content if original_content is None else original_content,
        )
        if selection:
            message["metadata"] = {"codex_memory_selection": selection}
        return message

    def compact_for_role(role: str, content: str) -> str:
        normalized_role = normalized_hook_message_role(role, event=event)
        if normalized_role == "assistant":
            return selected_assistant_memory_text(content)
        if normalized_role == "tool":
            return selected_tool_evidence_text(content)
        if normalized_role == "user":
            return selected_user_prompt_memory_text(content)
        return str(content or "").strip()

    raw_messages = payload.get("messages") if isinstance(payload, dict) else None
    messages: list[Json] = []
    if isinstance(raw_messages, list):
        for item in raw_messages:
            if isinstance(item, dict):
                content = str(item.get("content") or item.get("text") or item.get("message") or "").strip()
                if content:
                    role = normalized_hook_message_role(str(item.get("role") or role_for_agent_event(event)).strip(), event=event)
                    messages.append(message_for_role(role, content))
            elif isinstance(item, str) and item.strip():
                role = normalized_hook_message_role(role_for_agent_event(event), event=event)
                messages.append(message_for_role(role, item.strip()))
    if messages:
        return messages
    if norm(event) in TOOL_EVENTS:
        tool_texts: list[str] = []
        containers = [payload]
        for nested_key in ("params", "turn", "metadata", "tool", "result"):
            nested = payload.get(nested_key) if isinstance(payload, dict) else None
            if isinstance(nested, dict):
                containers.append(nested)
        for container in containers:
            for key in ("tool_outputs", "terminal_output", "stdout", "stderr", "result", "output", "text", "content"):
                value = container.get(key)
                values = value if isinstance(value, list) else [value] if value else []
                for item in values[:8]:
                    if isinstance(item, str) and item.strip():
                        tool_texts.append(item)
                    elif isinstance(item, dict):
                        item_text = first_string_at(item, [["text"], ["content"], ["stdout"], ["stderr"], ["output"], ["result"], ["summary"]])
                        if item_text:
                            tool_texts.append(item_text)
        evidence = selected_tool_evidence_text("\n".join(tool_texts) or text)
        if evidence:
            return [message_for_role("tool", evidence, original_content="\n".join(tool_texts) or text)]
    role = role_for_agent_event(event)
    return [message_for_role(role, text)]


def agent_memory_selection_metadata(payload: Json, *, event: str, text: str, messages: list[Json]) -> Json:
    if not messages:
        return {}
    raw_messages = payload.get("messages") if isinstance(payload, dict) else None
    original_by_role: dict[str, list[str]] = {}
    if isinstance(raw_messages, list):
        for item in raw_messages:
            if isinstance(item, dict):
                role = str(item.get("role") or role_for_agent_event(event)).strip() or role_for_agent_event(event)
                role = normalized_hook_message_role(role, event=event)
                content = str(item.get("content") or item.get("text") or item.get("message") or "").strip()
            elif isinstance(item, str):
                role = normalized_hook_message_role(role_for_agent_event(event), event=event)
                content = item.strip()
            else:
                continue
            if content:
                original_by_role.setdefault(role, []).append(content)
    if not original_by_role and str(text or "").strip():
        original_by_role.setdefault(role_for_agent_event(event), []).append(str(text or "").strip())

    selected_by_role: dict[str, list[str]] = {}
    for message in messages:
        role = str(message.get("role") or role_for_agent_event(event)).strip() or role_for_agent_event(event)
        role = normalized_hook_message_role(role, event=event)
        content = str(message.get("content") or "").strip()
        if content:
            selected_by_role.setdefault(role, []).append(content)

    selections: list[Json] = []
    policy_counts: Json = {}
    policies: list[str] = []
    source_profile_memory_classes: list[str] = []
    source_profile_memory_kinds: list[str] = []
    for role in sorted(selected_by_role):
        selected_text = "\n".join(selected_by_role.get(role, []))
        original_text = "\n".join(original_by_role.get(role, [])) or selected_text
        role_messages = [
            message
            for message in messages
            if normalized_hook_message_role(str(message.get("role") or role_for_agent_event(event)).strip(), event=event)
            == role
        ]
        if len(role_messages) == 1:
            metadata = role_messages[0].get("metadata") if isinstance(role_messages[0].get("metadata"), dict) else {}
            selection = metadata.get("codex_memory_selection") if isinstance(metadata.get("codex_memory_selection"), dict) else {}
        else:
            selection = {}
        if not selection:
            selection = codex_memory_selection_metadata(
                role=role,
                event=event,
                text=selected_text,
                original_text=original_text,
            )
        if not selection:
            continue
        selection_policies = selection.get("policies")
        if not isinstance(selection_policies, list):
            selection_policies = [selection.get("policy")]
        role_message_count = len(selected_by_role.get(role, []))
        for raw_policy in selection_policies:
            policy = str(raw_policy or "").strip()
            if policy:
                policies.append(policy)
                policy_counts[policy] = int(policy_counts.get(policy, 0)) + role_message_count
        for field, target in [
            ("source_profile_memory_classes", source_profile_memory_classes),
            ("profile_memory_class", source_profile_memory_classes),
            ("source_profile_memory_kinds", source_profile_memory_kinds),
            ("profile_memory_kind", source_profile_memory_kinds),
        ]:
            raw_values = selection.get(field)
            values = raw_values if isinstance(raw_values, list) else [raw_values]
            for raw_value in values:
                value = str(raw_value or "").strip()
                if value and value not in target:
                    target.append(value)
        selections.append(selection)

    if not selections:
        return {}
    aggregate: Json = {
        "source_memory_selection_policies": sorted(set(policies)),
        "source_memory_selection_policy_counts": policy_counts,
    }
    if source_profile_memory_classes:
        aggregate["source_profile_memory_classes"] = sorted(set(source_profile_memory_classes))
        aggregate["profile_memory_class"] = (
            "memory_feature"
            if "memory_feature" in source_profile_memory_classes
            else aggregate["source_profile_memory_classes"][0]
        )
    if source_profile_memory_kinds:
        aggregate["source_profile_memory_kinds"] = sorted(set(source_profile_memory_kinds))
        aggregate["profile_memory_kind"] = (
            "memory_feature"
            if "memory_feature" in source_profile_memory_kinds
            else aggregate["source_profile_memory_kinds"][0]
        )
    if len(selections) == 1:
        aggregate["codex_memory_selection"] = selections[0]
    return aggregate


def normalized_session_buffer_from_ingest(ingest: Json | None) -> Json:
    if not isinstance(ingest, dict):
        return {}
    raw = ingest.get("session_buffer") if isinstance(ingest.get("session_buffer"), dict) else {}
    summary = dict(raw)
    try:
        pending_event_count = int(summary.get("pending_event_count") or 0)
    except (TypeError, ValueError):
        pending_event_count = 0
    try:
        threshold_messages = int(summary.get("threshold_messages") or ingest.get("session_buffer_threshold") or 0)
    except (TypeError, ValueError):
        threshold_messages = 0
    if "pending_event_count" not in summary:
        summary["pending_event_count"] = pending_event_count
    if threshold_messages > 0 and "threshold_messages" not in summary:
        summary["threshold_messages"] = threshold_messages
    if "threshold_ready" not in summary:
        summary["threshold_ready"] = bool(threshold_messages > 0 and pending_event_count >= threshold_messages)
    if "idle_ready" not in summary:
        idle_result = ingest.get("idle_commit_result") if isinstance(ingest.get("idle_commit_result"), dict) else {}
        summary["idle_ready"] = bool(idle_result.get("trigger_policy") == "idle_timeout")
    if "auto_batch_extract" not in summary:
        summary["auto_batch_extract"] = True
    return summary


def hook_storage_options() -> Json:
    return {"route": os.environ.get("MATRIXARK_HOOK_STORAGE_ROUTE", "shared_store_async")}


def require_retrieval_memory_coverage(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "required", "require"}


def retrieval_memory_coverage_summary(layer_summary: Json, *, selected_ref_count: int) -> Json:
    gaps: list[str] = []
    memory_layer_budget = layer_summary.get("memory_layer_budget")
    memory_layer_budget = memory_layer_budget if isinstance(memory_layer_budget, dict) else {}
    try:
        budget_selected_refs = int(memory_layer_budget.get("total_selected_refs") or 0)
    except (TypeError, ValueError):
        budget_selected_refs = 0

    if selected_ref_count <= 0:
        gaps.append("retrieval:no_remote_refs_selected")
    if int(layer_summary.get("session_memory_refs") or 0) <= 0 and int(layer_summary.get("profile_memory_refs") or 0) <= 0:
        gaps.append("retrieval:no_session_or_profile_memory_selected")
    if int(layer_summary.get("same_session_refs") or 0) <= 0 and int(layer_summary.get("cross_session_refs") or 0) <= 0:
        gaps.append("retrieval:no_session_continuity_refs_selected")
    if not memory_layer_budget or budget_selected_refs <= 0:
        gaps.append("retrieval:memory_layer_budget_missing_selected_refs")

    return {
        "status": "gap" if gaps else "ok",
        "gaps": gaps,
        "selected_ref_count": selected_ref_count,
        "session_memory_refs": int(layer_summary.get("session_memory_refs") or 0),
        "profile_memory_refs": int(layer_summary.get("profile_memory_refs") or 0),
        "same_session_refs": int(layer_summary.get("same_session_refs") or 0),
        "cross_session_refs": int(layer_summary.get("cross_session_refs") or 0),
        "memory_layer_budget_selected_refs": budget_selected_refs,
    }


def agent_retrieval_summary(retrieve: Json | None, *, session_id_source: str) -> Json:
    retrieve = retrieve if isinstance(retrieve, dict) else {}
    layer_summary = retrieval_layer_summary_from_retrieve(retrieve, include_budget_lineage=True)
    selected_ref_count = selected_ref_count_from_retrieve(retrieve)
    return {
        "context_pack_id": retrieve.get("context_pack_id"),
        "selected_ref_count": selected_ref_count,
        "used_context_tokens": used_context_tokens_from_retrieve(retrieve),
        "budget": retrieval_budget_summary_from_retrieve(retrieve),
        "budget_pressure": retrieval_budget_pressure_from_retrieve(retrieve),
        "layer_summary": layer_summary,
        "memory_layer_budget": layer_summary.get("memory_layer_budget", {}),
        "memory_layer_pressure": layer_summary.get("memory_layer_pressure", {}),
        "memory_layer_coverage": retrieval_memory_coverage_summary(
            layer_summary,
            selected_ref_count=selected_ref_count,
        ),
        "session_identity": retrieval_session_identity_from_retrieve(
            retrieve,
            session_id_source=session_id_source,
        ),
        "quality_warnings": retrieval_quality_warnings_from_retrieve(retrieve),
        "memory_hierarchy_contract": retrieval_memory_hierarchy_contract_from_retrieve(retrieve),
    }


def append_cli_value(cmd: list[str], flag: str, value: Any) -> None:
    if value in (None, ""):
        return
    cmd.extend([flag, str(value)])


def spawn_idle_commit_worker_child(args: argparse.Namespace, *, ingest: Json, session_id_source: str) -> Json:
    if getattr(args, "idle_commit_worker_only", False):
        return {"status": "skipped", "reason": "already_idle_commit_worker"}
    if os.environ.get("MATRIXARK_DISABLE_IDLE_COMMIT_WORKER", "").strip().lower() in {"1", "true", "yes", "on"}:
        return {"status": "disabled", "reason": "MATRIXARK_DISABLE_IDLE_COMMIT_WORKER"}
    session_buffer = ingest.get("session_buffer") if isinstance(ingest, dict) else {}
    if not isinstance(session_buffer, dict) or not session_buffer.get("idle_commit_scheduled"):
        return {"status": "skipped", "reason": "idle_commit_not_scheduled"}
    timeout_ms = int(getattr(args, "idle_commit_timeout_ms", 0) or 0)
    if timeout_ms <= 0:
        return {"status": "skipped", "reason": "idle_commit_timeout_disabled"}
    auto_batch = ingest.get("auto_batch_extract_result") if isinstance(ingest.get("auto_batch_extract_result"), dict) else {}
    deadline_ms = int(session_buffer.get("idle_commit_deadline_ms") or auto_batch.get("idle_commit_deadline_ms") or 0)
    now_ms_value = int(time.time() * 1000)
    cutoff_ms = int(session_buffer.get("idle_commit_cutoff_ms") or auto_batch.get("idle_commit_cutoff_ms") or now_ms_value)
    delay_ms = max(0, deadline_ms - now_ms_value) if deadline_ms > 0 else timeout_ms
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--idle-commit-worker-only",
        "--agent",
        str(getattr(args, "agent", "generic")),
        "--event",
        "IdleTimeout",
        "--backend",
        str(getattr(args, "backend", default_hook_backend())),
        "--account-id",
        str(getattr(args, "account_id", "")),
        "--tenant-id",
        str(getattr(args, "tenant_id", "")),
        "--user-id",
        str(getattr(args, "user_id", "")),
        "--session-id",
        str(getattr(args, "session_id", "")),
        "--team",
        str(getattr(args, "team", "agent")),
        "--project",
        str(getattr(args, "project", "local")),
        "--session-commit-threshold",
        str(getattr(args, "session_commit_threshold", 20)),
        "--idle-commit-timeout-ms",
        str(timeout_ms),
        "--idle-commit-cutoff-ms",
        str(cutoff_ms),
        "--understanding-provider",
        str(getattr(args, "understanding_provider", "rules")),
        "--segment-provider",
        str(getattr(args, "segment_provider", "deterministic")),
        "--request-timeout-ms",
        str(getattr(args, "request_timeout_ms", 60000)),
        "--io-timeout-ms",
        str(getattr(args, "io_timeout_ms", 60000)),
        "--repo-root",
        str(getattr(args, "repo_root", Path(__file__).resolve().parents[1])),
    ]
    for flag, attr in [
        ("--api-key", "api_key"),
        ("--metaserver", "metaserver"),
        ("--namespace", "namespace"),
        ("--table", "table"),
        ("--temporalstore-lib", "temporalstore_lib"),
        ("--rust-proxy", "rust_proxy"),
        ("--rust-direct-sdk", "rust_direct_sdk"),
        ("--rust-cli", "rust_cli"),
        ("--storage-prefix", "storage_prefix"),
        ("--session-state-dir", "session_state_dir"),
    ]:
        append_cli_value(cmd, flag, getattr(args, attr, ""))
    env = os.environ.copy()
    env["MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS"] = str(delay_ms)
    env["MATRIXARK_IDLE_COMMIT_CUTOFF_MS"] = str(cutoff_ms)
    env["MATRIXARK_IDLE_COMMIT_WORKER_PARENT_EVENT"] = str(getattr(args, "event", ""))
    try:
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=str(getattr(args, "repo_root", Path(__file__).resolve().parents[1])),
            env=env,
        )
    except OSError as exc:
        return {
            "status": "error",
            "reason": "idle_commit_worker_spawn_failed",
            "error": str(exc)[:300],
            "delay_ms": delay_ms,
        }
    return {
        "status": "spawned",
        "reason": "session_buffer_idle_deadline_worker",
        "delay_ms": delay_ms,
        "idle_commit_timeout_ms": timeout_ms,
        "idle_commit_deadline_ms": deadline_ms,
        "idle_commit_cutoff_ms": cutoff_ms,
    }


def run_idle_commit_worker_only(args: argparse.Namespace, session_id_source: str) -> int:
    delay_ms = int(os.environ.get("MATRIXARK_IDLE_COMMIT_WORKER_DELAY_MS", "0") or 0)
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)
    server = build_server(args)
    try:
        common: Json = {"scope": scope_from_args(args)}
        if args.api_key:
            common["api_key"] = args.api_key
        result = call_tool(
            server,
            "matrixark_session_commit",
            {
                **common,
                "threshold_messages": args.session_commit_threshold,
                "force": False,
                "commit_reason": "idle_timeout",
                "idle_timeout_ms": args.idle_commit_timeout_ms,
                "commit_before_ms": args.idle_commit_cutoff_ms or None,
                "understanding_provider": args.understanding_provider,
                "segment_provider": args.segment_provider,
                "storage_options": hook_storage_options(),
                "agent_hook": {
                    "source": args.agent,
                    "hook_type": "session_commit",
                    "hook_id": f"{args.agent}:idle_commit_worker:{args.session_id}:{int(time.time() * 1000)}",
                    "observed_at_ms": int(time.time() * 1000),
                    "idempotency_key": f"agent-idle-commit-worker:{args.agent}:{args.session_id}",
                    "trigger": "IdleTimeout:worker",
                    "auto_captured": True,
                    "session_id_source": session_id_source,
                },
            },
        )
        print(json.dumps({"status": "ok", "worker": "idle_commit", "result": session_commit_summary(result)}, sort_keys=True))
    finally:
        close_server_best_effort(server)
    return 0


def _text_from_transcript_content(content: Any) -> str:
    """Flatten a transcript message's `content` (string or block list) to text."""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if block.get("type") in (None, "text") and isinstance(block.get("text"), str):
                    parts.append(block["text"])
        return "\n".join(part for part in parts if part).strip()
    if isinstance(content, dict) and isinstance(content.get("text"), str):
        return content["text"].strip()
    return ""


def _last_assistant_text_from_transcript(path: str, *, max_chars: int = 8000) -> str:
    """Return the last assistant message text from a Claude Code transcript JSONL."""
    try:
        transcript = Path(path)
        if not transcript.is_file():
            return ""
        last = ""
        with transcript.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                if not isinstance(obj, dict):
                    continue
                message = obj.get("message") if isinstance(obj.get("message"), dict) else None
                if message is not None:
                    role = str(message.get("role") or obj.get("role") or obj.get("type") or "")
                    content = message.get("content")
                else:
                    role = str(obj.get("role") or obj.get("type") or "")
                    content = obj.get("content") if obj.get("content") is not None else obj.get("text")
                if role != "assistant":
                    continue
                text = _text_from_transcript_content(content)
                if text:
                    last = text
        return last[:max_chars]
    except OSError:
        return ""


def augment_payload_with_transcript(payload: Json, *, event: str) -> None:
    """Surface the model's reply from `transcript_path` for after-LLM/commit events.

    Claude Code's Stop / SubagentStop / PostCompact payloads carry only a
    ``transcript_path`` (the JSONL conversation log) and no inline assistant text,
    so the hook previously ingested only the payload envelope (or nothing) and the
    model's actual answer was never captured -- transcript_path was used solely to
    hash a session id. Read the transcript and expose the last assistant turn under
    ``last_assistant_message`` (an assistant_paths key that payload_text/ingest
    already understand). Only fires for after-LLM/commit events and never clobbers
    assistant text the payload already supplies. Best effort: any failure leaves
    the payload unchanged so the hook stays fail-open.
    """
    if not isinstance(payload, dict):
        return
    normalized = norm(event)
    is_after_llm = (
        should_commit(event)
        or normalized in AFTER_LLM_EVENTS
        or "stop" in normalized
        or "compact" in normalized
    )
    if not is_after_llm:
        return
    if first_string_at(
        payload,
        [["last_assistant_message"], ["last_agent_message"], ["assistant_message"], ["response"], ["output"]],
    ):
        return
    path = first_string_at(
        payload,
        [["transcript_path"], ["transcriptPath"], ["conversation_path"], ["conversationPath"], ["log_path"], ["logPath"]],
    )
    if not path:
        return
    text = _last_assistant_text_from_transcript(path)
    if text:
        payload["last_assistant_message"] = text


def additional_context_from_retrieve(retrieve: Json | None, *, max_chars: int = 8000) -> str:
    """Flatten a retrieve context pack into the Claude Code additionalContext string.

    The native context pack returns retrieved memory as ``groups[].items[].text``.
    agent_hook previously never surfaced this as ``hookSpecificOutput`` /
    ``additionalContext``, so the Claude wrapper's pass-through always emitted
    ``{}`` and no retrieved context ever reached the model even when refs were
    selected. Join the unique item texts (bounded) into a compact block the hook
    can inject. Returns "" when nothing was retrieved so the fail-open ``{}``
    contract is preserved.
    """
    if not isinstance(retrieve, dict):
        return ""
    lines: list[str] = []
    seen: set[str] = set()
    groups = retrieve.get("groups")
    if isinstance(groups, list):
        for group in groups:
            if not isinstance(group, dict):
                continue
            items = group.get("items")
            if not isinstance(items, list):
                continue
            for item in items:
                if isinstance(item, dict):
                    text = str(item.get("text") or item.get("entity") or "").strip()
                elif isinstance(item, str):
                    text = item.strip()
                else:
                    text = ""
                if not text or text in seen:
                    continue
                seen.add(text)
                lines.append(text)
    if not lines:
        return ""
    header = "Relevant context from earlier turns (MatrixArk memory):"
    body = "\n".join(f"- {line}" for line in lines)
    return (header + "\n" + body)[:max_chars]


def call_write_tool(
    server: Any,
    tool: str,
    tool_args: Json,
    *,
    failures: list[Json],
) -> Json:
    """Call a write-side tool without denying the later read path."""
    try:
        return call_tool(server, tool, tool_args)
    except Exception as write_error:  # noqa: BLE001 - hook writes must fail into JSON.
        failures.append(
            {
                "tool": tool,
                "error": f"{write_error.__class__.__name__}: {write_error}"[:500],
            }
        )
        return {}


def main() -> int:
    args = parse_args()
    validate_hook_backend_policy(args.backend)
    payload = read_stdin_payload()
    args.session_id, session_id_source = resolve_session_id(payload, args)
    if args.idle_commit_worker_only:
        return run_idle_commit_worker_only(args, session_id_source)
    augment_payload_with_transcript(payload, event=args.event)
    text = payload_text(payload, event=args.event) or args.query
    raw_uri = payload_resource_uri(payload)
    resource_type = payload_resource_type(payload, raw_uri) if raw_uri else ""
    event_hook_type = hook_type_for_agent_event(args.event)
    if not text and not raw_uri and not should_commit(args.event):
        print(json.dumps({"status": "skipped", "reason": "empty hook payload", "agent": args.agent}))
        return 0
    agent_context = agent_context_from_payload(payload, agent=args.agent, event=args.event, session_id_source=session_id_source, args=args)
    if args.backend in {"temporalstore-rust", "temporalstore-rust-direct"}:
        os.environ.setdefault("MATRIXARK_RUST_PROXY_PUBLISH_VISIBILITY_AFTER_FLUSH", "1")

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
    retrieve: Json = {}
    if should_retrieve(args.event) or args.query:
        retrieve_metadata = {
            "retrieval_source": f"{args.agent}_hook",
            "agent": args.agent,
            "agent_event": args.event,
            "hook_type": event_hook_type,
            "session_id_source": session_id_source,
            "codex_session_id_source": session_id_source if args.agent == "codex" else "",
        }
        local_context = agent_context.get("local_context", [])
        retrieve_args = agent_retrieve_args(
            common,
            args,
            query=args.query or text[:500],
            retrieve_metadata=retrieve_metadata,
            local_context=local_context if isinstance(local_context, list) else [],
        )
        retrieve = call_tool(server, "matrixark_retrieve", retrieve_args)
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
    write_failures: list[Json] = []
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
            "storage_options": hook_storage_options(),
        }
        feedback = call_write_tool(server, "matrixark_feedback", feedback_args, failures=write_failures)
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
            "storage_options": hook_storage_options(),
            "wait": bool(payload.get("wait", False)),
        }
        ingest = call_write_tool(server, "matrixark_ingest", ingest_args, failures=write_failures)
    elif text:
        hook_messages = hook_messages_from_payload(payload, event=args.event, text=text)
        selection_metadata = agent_memory_selection_metadata(
            payload,
            event=args.event,
            text=text,
            messages=hook_messages,
        )
        ingest_args = {
            **common,
            "messages": hook_messages,
            "understanding_provider": args.understanding_provider,
            "segment_provider": args.segment_provider,
            "metadata": {**base_metadata, **selection_metadata},
            "agent_hook": hook_meta,
            "storage_options": hook_storage_options(),
        }
        auto_batch_extract = should_auto_batch_extract_on_ingest(args.event)
        ingest_args["auto_batch_extract"] = auto_batch_extract
        ingest_args["session_buffer_threshold"] = args.session_commit_threshold
        if auto_batch_extract and args.idle_commit_timeout_ms > 0:
            ingest_args["idle_commit_timeout_ms"] = args.idle_commit_timeout_ms
        ingest = call_write_tool(server, "matrixark_ingest", ingest_args, failures=write_failures)

    if not retrieve and (should_retrieve(args.event) or args.query):
        retrieve_metadata = {
            "retrieval_source": f"{args.agent}_hook",
            "agent": args.agent,
            "agent_event": args.event,
            "hook_type": event_hook_type,
            "session_id_source": session_id_source,
            "codex_session_id_source": session_id_source if args.agent == "codex" else "",
        }
        local_context = agent_context.get("local_context", [])
        retrieve_args = agent_retrieve_args(
            common,
            args,
            query=args.query or text[:500],
            retrieve_metadata=retrieve_metadata,
            local_context=local_context if isinstance(local_context, list) else [],
        )
        retrieve = call_tool(server, "matrixark_retrieve", retrieve_args)

    commit: Json = {}
    if should_commit(args.event):
        reason = commit_reason(args.event)
        commit = call_write_tool(
            server,
            "matrixark_session_commit",
            {
                **common,
                "threshold_messages": args.session_commit_threshold,
                "force": reason != "idle_timeout",
                "commit_reason": reason,
                "understanding_provider": args.understanding_provider,
                "segment_provider": args.segment_provider,
                "storage_options": hook_storage_options(),
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
            failures=write_failures,
        )

    ingest_session_buffer = normalized_session_buffer_from_ingest(ingest)
    idle_commit_worker = spawn_idle_commit_worker_child(
        args,
        ingest=ingest,
        session_id_source=session_id_source,
    ) if ingest else {}
    close_server_best_effort(server)
    retrieved_summary = agent_retrieval_summary(retrieve, session_id_source=session_id_source)
    output = {
        "status": "ok",
        "agent": args.agent,
        "event": args.event,
        "session_id": args.session_id,
        "session_id_source": session_id_source,
        "agent_context_refs": len(agent_context.get("local_context", [])),
        "workspace_root": agent_context.get("workspace_root", ""),
        "ingested": bool(ingest),
        "ingest": {
            "status": ingest.get("status"),
            "event_id_hash": ingest.get("event_id_hash"),
            "extraction_mode": ingest.get("extraction_mode"),
            "session_buffer": ingest_session_buffer,
            "summary_refresh": ingest.get("summary_refresh", {}),
            "quality_warnings": ingest.get("quality_warnings", []),
        } if ingest else {},
        "auto_batch_extract": session_commit_summary(
            ingest.get("auto_batch_extract_result")
            if isinstance(ingest.get("auto_batch_extract_result"), dict)
            else {}
        ) if ingest else {},
        "auto_batch_extract_decision": auto_batch_decision_summary(ingest) if ingest else {},
        "idle_commit": session_commit_summary(
            ingest.get("idle_commit_result")
            if isinstance(ingest.get("idle_commit_result"), dict)
            else {}
        ) if ingest else {},
        "idle_commit_worker": idle_commit_worker,
        "feedbacked": bool(feedback),
        "resource_uri": raw_uri,
        "resource_type": resource_type,
        "retrieved": retrieved_summary,
        "committed": session_commit_summary(commit),
        "write_failures": write_failures,
    }
    # Emit the Claude Code hook contract so retrieved memory actually reaches the
    # model. The wrapper (matrixark_claude_hook.sh) passes through
    # hookSpecificOutput.additionalContext on before-LLM events; without this the
    # pack is retrieved but never injected.
    if should_retrieve(args.event) or args.query:
        additional_context = additional_context_from_retrieve(retrieve)
        if additional_context.strip():
            output["hookSpecificOutput"] = {
                "hookEventName": args.event,
                "additionalContext": additional_context,
            }
    require_retrieval_coverage = args.require_retrieval_memory_coverage or require_retrieval_memory_coverage(
        os.environ.get("MATRIXARK_REQUIRE_RETRIEVAL_MEMORY_COVERAGE")
    )
    if require_retrieval_coverage and (should_retrieve(args.event) or args.query):
        coverage = retrieved_summary.get("memory_layer_coverage")
        if isinstance(coverage, dict) and coverage.get("status") == "gap":
            output["status"] = "retrieval_memory_coverage_gap"
            output["retrieval_memory_coverage_required"] = True
            print(json.dumps(output, sort_keys=True))
            return 3
    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
