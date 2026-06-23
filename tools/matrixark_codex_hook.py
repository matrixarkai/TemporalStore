#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any


Json = dict[str, Any]


def load_matrixark(root: Path):
    sys.path.insert(0, str(root))
    from tools.matrixark_mcp_server import (  # type: ignore
        MatrixArkLocalAdapter,
        MatrixArkMcpServer,
        MatrixArkTemporalStoreDirectAdapter,
    )

    return MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkTemporalStoreDirectAdapter


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Ingest Codex hook payloads into MatrixArk.")
    parser.add_argument("--event", default=os.environ.get("CODEX_HOOK_EVENT", "UserPromptSubmit"))
    parser.add_argument("--event-log", type=Path, default=Path(os.environ.get("MATRIXARK_CODEX_EVENT_LOG", "/tmp/matrixark-codex-hook.jsonl")))
    parser.add_argument("--backend", choices=["local", "temporalstore-direct"], default=os.environ.get("MATRIXARK_MCP_BACKEND", "local"))
    parser.add_argument("--api-key", default=os.environ.get("MATRIXARK_API_KEY", ""))
    parser.add_argument("--account-id", default=os.environ.get("MATRIXARK_ACCOUNT_ID", "acct_codex"))
    parser.add_argument("--tenant-id", default=os.environ.get("MATRIXARK_TENANT_ID", "tenant_codex"))
    parser.add_argument("--user-id", default=os.environ.get("MATRIXARK_USER_ID", os.environ.get("USERNAME", "codex_user")))
    parser.add_argument("--session-id", default=os.environ.get("MATRIXARK_SESSION_ID"))
    parser.add_argument(
        "--session-state-dir",
        type=Path,
        default=Path(os.environ.get("MATRIXARK_CODEX_SESSION_STATE_DIR", "/tmp/matrixark-codex-sessions")),
        help="Directory used for the fallback generated Codex hook session id.",
    )
    parser.add_argument("--team", default=os.environ.get("MATRIXARK_TEAM", "codex"))
    parser.add_argument("--project", default=os.environ.get("MATRIXARK_PROJECT", "local"))
    parser.add_argument("--query", default="")
    parser.add_argument("--max-context-tokens", type=int, default=int(os.environ.get("MATRIXARK_HOOK_MAX_CONTEXT_TOKENS", "1024")))
    parser.add_argument("--metaserver", default=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", "127.0.0.1:18000"))
    parser.add_argument("--namespace", default=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "deploy_ns"))
    parser.add_argument("--table", default=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "deploy_table"))
    parser.add_argument("--temporalstore-lib", default=os.environ.get("TEMPORALSTORE_LIB", ""))
    parser.add_argument("--storage-prefix", default=os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX", "matrixark:codex-hook"))
    parser.add_argument("--request-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS", "60000")))
    parser.add_argument("--io-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS", "60000")))
    parser.add_argument("--session-commit-threshold", type=int, default=int(os.environ.get("MATRIXARK_SESSION_COMMIT_THRESHOLD", "20")))
    parser.add_argument("--idle-commit-timeout-ms", type=int, default=int(os.environ.get("MATRIXARK_IDLE_COMMIT_TIMEOUT_MS", "0")))
    parser.add_argument("--understanding-provider", default=os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER", "rules"))
    parser.add_argument("--segment-provider", default=os.environ.get("MATRIXARK_SEGMENT_PROVIDER", "deterministic"))
    parser.add_argument("--repo-root", type=Path, default=root)
    return parser.parse_args()


def read_stdin_payload() -> Json:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {"raw_text": raw}
    return value if isinstance(value, dict) else {"payload": value}


def first_string_at(payload: Json, paths: list[list[str]]) -> str:
    for path in paths:
        value: Any = payload
        for part in path:
            if not isinstance(value, dict) or part not in value:
                value = None
                break
            value = value[part]
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def stable_short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def payload_session_candidate(payload: Json) -> tuple[str, str]:
    direct = first_string_at(
        payload,
        [
            ["session_id"],
            ["sessionId"],
            ["codex_session_id"],
            ["thread_id"],
            ["threadId"],
            ["conversation_id"],
            ["conversationId"],
            ["transcript_id"],
            ["transcriptId"],
            ["run", "session_id"],
            ["run", "thread_id"],
            ["params", "session_id"],
            ["params", "thread_id"],
            ["turn", "session_id"],
            ["turn", "thread_id"],
            ["metadata", "session_id"],
            ["metadata", "thread_id"],
        ],
    )
    if direct:
        return f"codex:{direct}", "payload_field"

    path_value = first_string_at(
        payload,
        [
            ["transcript_path"],
            ["transcriptPath"],
            ["conversation_path"],
            ["conversationPath"],
            ["thread_path"],
            ["threadPath"],
            ["log_path"],
            ["logPath"],
        ],
    )
    if path_value:
        return f"codex:path:{stable_short_hash(path_value)}", "payload_path_hash"

    return "", ""


def workspace_fingerprint(payload: Json, args: argparse.Namespace) -> str:
    workspace = first_string_at(
        payload,
        [
            ["workspace_root"],
            ["workspaceRoot"],
            ["cwd"],
            ["params", "cwd"],
            ["metadata", "cwd"],
        ],
    )
    if not workspace:
        workspace = str(args.repo_root)
    seed = "|".join([args.account_id, args.tenant_id, args.user_id, workspace])
    return stable_short_hash(seed)


def generated_session_id(payload: Json, args: argparse.Namespace) -> tuple[str, str]:
    args.session_state_dir.mkdir(parents=True, exist_ok=True)
    state_file = args.session_state_dir / f"{workspace_fingerprint(payload, args)}.session"
    if state_file.exists():
        existing = state_file.read_text(encoding="utf-8").strip()
        if existing:
            return existing, "state_file"
    value = f"codex:local:{uuid.uuid4().hex[:16]}"
    state_file.write_text(value + "\n", encoding="utf-8")
    return value, "state_file_created"


def resolve_session_id(payload: Json, args: argparse.Namespace) -> tuple[str, str]:
    if args.session_id:
        return args.session_id, "explicit"
    candidate, source = payload_session_candidate(payload)
    if candidate:
        return candidate, source
    return generated_session_id(payload, args)


def payload_text(payload: Json) -> str:
    direct = first_string_at(
        payload,
        [
            ["prompt"],
            ["user_prompt"],
            ["input"],
            ["text"],
            ["message"],
            ["params", "prompt"],
            ["params", "input"],
            ["params", "text"],
            ["turn", "input"],
            ["raw_text"],
        ],
    )
    if direct:
        return direct
    for key in ["messages", "items", "input"]:
        value = payload.get(key)
        if isinstance(value, list):
            parts = []
            for item in value:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    text = first_string_at(item, [["content"], ["text"], ["message"]])
                    if text:
                        parts.append(text)
            if parts:
                return "\n".join(parts)
    return json.dumps(payload, sort_keys=True)[:4000] if payload else ""


def role_for_event(event: str) -> str:
    if event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        return "tool"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        return "assistant"
    return "user"


def hook_type_for_event(event: str) -> str:
    if event == "UserPromptSubmit":
        return "before_llm"
    if event in {"PostToolUse", "PreToolUse", "PermissionRequest"}:
        return "tool_result"
    if event in {"IdleTimeout", "SessionIdle"}:
        return "session_commit"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        return "after_llm"
    return "before_llm"


def should_commit_session(event: str) -> bool:
    return event in {"Stop", "PostCompact", "SubagentStop", "IdleTimeout", "SessionIdle"}


def commit_reason_for_event(event: str) -> str:
    if event in {"IdleTimeout", "SessionIdle"}:
        return "idle_timeout"
    if event in {"Stop", "PostCompact", "SubagentStop"}:
        return "hook_boundary"
    return "manual_api"


def call_tool(server: Any, name: str, arguments: Json) -> Json:
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000) % 1_000_000,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }
    )
    if "error" in response:
        raise RuntimeError(response["error"]["message"])
    return json.loads(response["result"]["content"][0]["text"])


def build_server(args: argparse.Namespace):
    MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkTemporalStoreDirectAdapter = load_matrixark(args.repo_root)
    if args.backend == "temporalstore-direct":
        adapter = MatrixArkTemporalStoreDirectAdapter(
            metaserver=args.metaserver,
            namespace=args.namespace,
            table=args.table,
            library_path=args.temporalstore_lib or None,
            storage_prefix=args.storage_prefix,
            request_timeout_ms=args.request_timeout_ms,
            io_timeout_ms=args.io_timeout_ms,
        )
    else:
        adapter = MatrixArkLocalAdapter(args.event_log)
    return MatrixArkMcpServer(adapter)


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
    payload = read_stdin_payload()
    resolved_session_id, session_id_source = resolve_session_id(payload, args)
    args.session_id = resolved_session_id
    text = payload_text(payload) or args.query
    if not text and args.event not in {"IdleTimeout", "SessionIdle"}:
        print(json.dumps({"status": "skipped", "reason": "empty hook payload"}))
        return 0

    server = build_server(args)
    scope = scope_from_args(args)
    common: Json = {"scope": scope}
    if args.api_key:
        common["api_key"] = args.api_key

    ingest = {}
    if text:
        ingest_args: Json = {
            **common,
            "messages": [{"role": role_for_event(args.event), "content": text}],
            "understanding_provider": args.understanding_provider,
            "segment_provider": args.segment_provider,
            "metadata": {
                "source": "codex_hook",
                "codex_event": args.event,
                "raw_hook_payload": payload,
                "compacted_session_summary": args.event == "PostCompact",
                "codex_session_id_source": session_id_source,
            },
            "agent_hook": {
                "source": "codex",
                "hook_type": hook_type_for_event(args.event),
                "hook_id": f"{args.event}:{int(time.time() * 1000)}",
                "observed_at_ms": int(time.time() * 1000),
                "idempotency_key": str(payload.get("id") or payload.get("turn_id") or ""),
                "trigger": args.event,
                "auto_captured": True,
                "session_id_source": session_id_source,
            },
        }
        if args.event == "UserPromptSubmit":
            ingest_args["auto_batch_extract"] = True
            ingest_args["session_buffer_threshold"] = args.session_commit_threshold
            if args.idle_commit_timeout_ms > 0:
                ingest_args["idle_commit_timeout_ms"] = args.idle_commit_timeout_ms
        ingest = call_tool(server, "matrixark_ingest", ingest_args)

    commit = {}
    if should_commit_session(args.event):
        commit_reason = commit_reason_for_event(args.event)
        commit = call_tool(
            server,
            "matrixark_session_commit",
            {
                **common,
                "threshold_messages": args.session_commit_threshold,
                "force": commit_reason != "idle_timeout",
                "commit_reason": commit_reason,
                "understanding_provider": args.understanding_provider,
                "segment_provider": args.segment_provider,
                **({"idle_timeout_ms": args.idle_commit_timeout_ms} if commit_reason == "idle_timeout" else {}),
                "agent_hook": {
                    "source": "codex",
                    "hook_type": "session_commit",
                    "hook_id": f"session_commit:{args.event}:{int(time.time() * 1000)}",
                    "observed_at_ms": int(time.time() * 1000),
                    "idempotency_key": str(payload.get("id") or payload.get("turn_id") or ""),
                    "trigger": args.event,
                    "auto_captured": True,
                    "session_id_source": session_id_source,
                },
            },
        )

    retrieve = {}
    query = args.query or text[:500]
    if args.event == "UserPromptSubmit" or args.query:
        retrieve = call_tool(
            server,
            "matrixark_retrieve",
            {
                **common,
                "query": query,
                "max_context_tokens": args.max_context_tokens,
            },
        )

    print(
        json.dumps(
            {
                "status": "ok",
                "event": args.event,
                "session_id": args.session_id,
                "session_id_source": session_id_source,
                "lifecycle_stage": {
                    "before_llm_retrieve": args.event == "UserPromptSubmit",
                    "after_llm_ingest_only": args.event in {"PostToolUse", "PreToolUse", "PermissionRequest"},
                    "hook_boundary_commit": args.event in {"Stop", "PostCompact", "SubagentStop"},
                    "idle_timeout_commit": args.event in {"IdleTimeout", "SessionIdle"},
                    "auto_threshold_commit": bool(ingest.get("auto_batch_extract_result")) if ingest else False,
                },
                "ingest": ingest,
                "retrieve": {
                    "context_pack_id": retrieve.get("context_pack_id"),
                    "selected_ref_count": len(retrieve.get("selected_refs", [])) if retrieve else 0,
                    "used_context_tokens": retrieve.get("used_context_tokens", 0) if retrieve else 0,
                },
                "session_commit": {
                    "status": commit.get("status"),
                    "commit_id_hash": commit.get("commit_id_hash"),
                    "commit_reason": commit.get("commit_reason"),
                    "trigger_policy": commit.get("trigger_policy"),
                    "source_event_count": commit.get("committed_event_count", len(commit.get("source_event_ids", []))),
                    "segments_written": commit.get("segments_written", 0),
                    "entities_written": commit.get("entities_written", 0),
                    "raw_events_duplicated": commit.get("raw_events_duplicated"),
                } if commit else {},
                "event_log": str(args.event_log),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
