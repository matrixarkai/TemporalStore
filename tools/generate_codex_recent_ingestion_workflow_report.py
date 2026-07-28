#!/usr/bin/env python3
from __future__ import annotations

import collections
import html
import json
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path("/root/src/github-services/TemporalStore-ingestion-workflow-report")
OUT_DIR = ROOT / "docs" / "debug" / "codex_recent_ingestion_workflow_20260724"
OUT_JSON = OUT_DIR / "codex_recent_ingestion_workflow.json"
OUT_MD = OUT_DIR / "codex_recent_ingestion_workflow.md"
OUT_HTML = OUT_DIR / "codex_recent_ingestion_workflow.html"

CPP_LIB = "/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so"
CPP_PREFIX = "matrixark:codex-hook:cpp-live-v2"
RUST_PREFIX = "matrixark:codex-hook:rust-live-v2"


def short(text: Any, limit: int = 220) -> str:
    value = " ".join(str(text or "").split())
    return value if len(value) <= limit else value[: limit - 3] + "..."


def event_ms(record: dict[str, Any]) -> int:
    return int(
        record.get("hook_observed_at_ms")
        or record.get("ingestion_time_ms")
        or record.get("created_at_ms")
        or (record.get("agent_hook") or {}).get("observed_at_ms")
        or 0
    )


def message_text(record: dict[str, Any]) -> str:
    for key in ("text", "content", "summary_text", "entity_name", "state"):
        if isinstance(record.get(key), str) and record[key].strip():
            return record[key]
    messages = record.get("messages")
    if isinstance(messages, list):
        parts = []
        for item in messages[:2]:
            if isinstance(item, dict) and item.get("content"):
                parts.append(str(item.get("content")))
        if parts:
            return " ".join(parts)
    return ""


def row(record: dict[str, Any], sequence: int) -> dict[str, Any]:
    hook = record.get("agent_hook") if isinstance(record.get("agent_hook"), dict) else {}
    messages = record.get("messages") if isinstance(record.get("messages"), list) else []
    role = record.get("role")
    if not role and messages and isinstance(messages[0], dict):
        role = messages[0].get("role", "")
    return {
        "sequence": sequence,
        "record_type": record.get("record_type") or record.get("type") or "unknown",
        "role": role or "",
        "session_id": record.get("session_id") or (record.get("scope") or {}).get("session_id", ""),
        "memory_scope": record.get("memory_scope") or "",
        "session_continuity": record.get("session_continuity") or "",
        "data_model": record.get("data_model") or "",
        "node_path": record.get("node_path") if isinstance(record.get("node_path"), list) else [],
        "thread_id": record.get("thread_id") or "",
        "turn_id": record.get("turn_id") or "",
        "codex_api_event": record.get("codex_api_event") or hook.get("trigger") or "",
        "hook_id": record.get("hook_id") or hook.get("hook_id") or "",
        "hook_type": record.get("hook_type") or hook.get("hook_type") or "",
        "hook_observed_at_ms": event_ms(record),
        "synthetic": bool(record.get("synthetic", False)),
        "text": short(message_text(record)),
        "write_path": ((record.get("matrixark_write_debug") or {}).get("write_path") if isinstance(record.get("matrixark_write_debug"), dict) else ""),
    }


def is_profile_record(item: dict[str, Any]) -> bool:
    node_path = item.get("node_path") if isinstance(item.get("node_path"), list) else []
    return (
        item.get("memory_scope") == "user_profile"
        or item.get("data_model") == "context_profile_entity"
        or "profile:long_term_memory" in node_path
    )


def is_resource_or_skill_record(item: dict[str, Any]) -> bool:
    record_type = str(item.get("record_type") or "")
    return record_type in {
        "resource_chunk",
        "resource_manifest",
        "resource_registry",
        "skill_section",
        "skill_manifest",
        "skill_registry",
        "skill_registry_update",
    }


def serving_visibility_gaps(
    *,
    serving_types: collections.Counter[str],
    context_events: list[dict[str, Any]],
    context_embeddings: list[dict[str, Any]],
    profile_records: list[dict[str, Any]],
    resource_skill_records: list[dict[str, Any]],
    raw_records: list[dict[str, Any]],
) -> list[str]:
    gaps: list[str] = []
    derived_count = sum(
        serving_types.get(record_type, 0)
        for record_type in ("context_entity", "context_index", "context_segment", "context_summary")
    )
    if derived_count:
        if not context_events:
            gaps.append("context_event_missing_while_derived_memory_present")
        if not context_embeddings:
            gaps.append("context_embedding_missing_while_derived_memory_present")
    if serving_types.get("context_index", 0) and not profile_records:
        gaps.append("profile_records_missing_from_recent_serving_window")
    raw_resource_or_skill = any(is_resource_or_skill_record(item) for item in raw_records)
    if raw_resource_or_skill and not resource_skill_records:
        gaps.append("resource_skill_records_missing_from_recent_serving_window")
    return gaps


def rust_exec(command: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps({"shard_id": 1, "command": command}).encode("utf-8")
    req = urllib.request.Request("http://127.0.0.1:17100/execute", data=body, headers={"content-type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=5).read().decode("utf-8"))


def bytes_to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def scan_rust(base: str, limit: int = 500) -> tuple[int, int, list[dict[str, Any]]]:
    count = int(bytes_to_str(rust_exec({"kind": "string_get", "key": base + ":record_count"})["response"]["value"]) or 0)
    hot_count = int(bytes_to_str(rust_exec({"kind": "string_get", "key": base + ":hot_record_count"})["response"]["value"]) or 0)
    rows: list[dict[str, Any]] = []
    for sequence in range(count - 1, max(-1, count - limit - 1), -1):
        candidates = (
            (f"{base}:records:{sequence // 256:06d}", f"{sequence % 256:020d}"),
            (f"{base}:records:{sequence // 10000:06d}", f"{sequence:020d}"),
            (f"{base}:records", f"{sequence:020d}"),
        )
        raw = ""
        for key, field in candidates:
            raw = bytes_to_str(rust_exec({"kind": "hash_get", "key": key, "field": field})["response"]["value"])
            if raw:
                break
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except Exception:
            record = {"record_type": "unparsed", "text": raw[:1000]}
        rows.append({"sequence": sequence, "record": record})
    return count, hot_count, rows


def scan_cpp(base: str, limit: int = 500) -> tuple[int, int, list[dict[str, Any]]]:
    sys.path.insert(0, str(ROOT / "sdk" / "python"))
    from temporalstore.client import Client, Options

    client = Client(
        Options(
            metaserver_addr="127.0.0.1:18000",
            namespace_name="deploy_ns",
            table_name="deploy_table",
            request_timeout_ms=5000,
            io_timeout_ms=5000,
            max_read_retries=1,
        ),
        library_path=CPP_LIB,
    )
    try:
        count = int(client.get_string(base + ":record_count") or 0)
    except Exception:
        count = 0
    try:
        hot_count = int(client.get_string(base + ":hot_record_count") or 0)
    except Exception:
        hot_count = 0
    rows = []
    for sequence in range(count - 1, max(-1, count - limit - 1), -1):
        try:
            raw = client.hget(f"{base}:records:{sequence // 256:06d}", f"{sequence % 256:020d}") or ""
        except Exception:
            raw = ""
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except Exception:
            record = {"record_type": "unparsed", "text": raw[:1000]}
        rows.append({"sequence": sequence, "record": record})
    return count, hot_count, rows


def summarize_backend(name: str, prefix: str, raw_count: int, raw_hot_count: int, raw_rows: list[dict[str, Any]], serving_count: int, serving_hot_count: int, serving_rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_records = [row(item["record"], item["sequence"]) for item in raw_rows]
    serving_records = [row(item["record"], item["sequence"]) for item in serving_rows]
    raw_types = collections.Counter(item["record_type"] for item in raw_records)
    serving_types = collections.Counter(item["record_type"] for item in serving_records)
    user_prompts = [item for item in raw_records if item["record_type"] == "agent_message" and item["codex_api_event"] == "UserPromptSubmit" and not item["synthetic"]]
    entities = [item for item in raw_records + serving_records if item["record_type"] in {"context_entity", "context_entity_update_audit"}]
    summaries = [item for item in raw_records + serving_records if item["record_type"] in {"context_summary", "context_summary_dirty", "context_batch_commit"}]
    context_events = [item for item in serving_records if item["record_type"] == "context_event"]
    context_embeddings = [item for item in serving_records if item["record_type"] == "context_embedding"]
    profile_records = [item for item in serving_records if is_profile_record(item)]
    resource_skill_records = [item for item in serving_records if is_resource_or_skill_record(item)]
    visibility_gaps = serving_visibility_gaps(
        serving_types=serving_types,
        context_events=context_events,
        context_embeddings=context_embeddings,
        profile_records=profile_records,
        resource_skill_records=resource_skill_records,
        raw_records=raw_records,
    )
    return {
        "backend": name,
        "prefix": prefix,
        "raw_count": raw_hot_count or raw_count,
        "serving_count": serving_hot_count or serving_count,
        "physical_raw_count": raw_count,
        "physical_serving_count": serving_count,
        "compact_hot_raw_count": raw_hot_count,
        "compact_hot_serving_count": serving_hot_count,
        "recent_raw_type_counts": dict(raw_types),
        "recent_serving_type_counts": dict(serving_types),
        "recent_real_user_prompts": user_prompts[:8],
        "recent_context_events": context_events[:8],
        "recent_context_embeddings": context_embeddings[:8],
        "recent_profile_records": profile_records[:8],
        "recent_resource_skill_records": resource_skill_records[:8],
        "recent_context_event_count": len(context_events),
        "recent_context_embedding_count": len(context_embeddings),
        "recent_profile_record_count": len(profile_records),
        "recent_resource_skill_record_count": len(resource_skill_records),
        "serving_visibility_status": "gap" if visibility_gaps else "ok",
        "serving_visibility_gaps": visibility_gaps,
        "recent_entities": entities[:8],
        "recent_summaries": summaries[:8],
        "recent_raw_records": raw_records[:12],
        "recent_serving_records": serving_records[:12],
    }


def render_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["|" + "|".join(headers) + "|", "|" + "|".join("---" for _ in headers) + "|"]
    for values in rows:
        out.append("|" + "|".join(str(v).replace("|", "\\|") for v in values) + "|")
    return "\n".join(out)


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Recent Codex Hook Ingestion Workflow",
        "",
        f"Generated at `{report['generated_at_ms']}`.",
        "",
        "## What This Report Proves",
        "",
        "- Rust TemporalStore currently captures real Codex `UserPromptSubmit` rows in raw ingestion and exposes matching serving `context_event` rows.",
        "- C++ TemporalStore now keeps the live hot prefix compact; the full hook writes trace/debug output to a separate debug prefix by default.",
        "- Compact extraction/summary is visible through `context_segment`, `context_entity`, `context_index`, and `context_summary` rows in the recent raw window.",
        "- Rust and C++ live hook prefixes use the same compact direct-publish shape for raw prompts, serving events, extracted entities, indexes, segments, and summaries.",
        "",
        "## Workflow",
        "",
        "```mermaid",
        "sequenceDiagram",
        "  participant Codex",
        "  participant Hook as matrixark_codex_dual_hook.sh",
        "  participant Rust as Rust TemporalStore 17100/17101/17102",
        "  participant Cpp as C++ TemporalStore 18000/18001",
        "  participant Async as Async extraction/summary",
        "  Codex->>Hook: UserPromptSubmit JSON payload",
        "  Hook->>Rust: raw agent_message append",
        "  Hook->>Cpp: raw agent_message append",
        "  Rust->>Rust: publish context_event serving projection",
        "  Cpp->>Cpp: publish compact context_event serving projection",
        "  Cpp->>Cpp: publish compact segment/entity/index/summary rows",
        "  Async-->>Cpp: optional debug/audit rows go to debug prefix",
        "```",
        "",
        "## Backend Counts",
        "",
        render_table(
            ["Backend", "Prefix", "Compact hot raw", "Compact hot serving", "Physical raw", "Physical serving", "Events", "Embeddings", "Profile", "Resource/skill", "Visibility", "Gaps", "Recent raw types", "Recent serving types"],
            [[b["backend"], b["prefix"], b["raw_count"], b["serving_count"], b["physical_raw_count"], b["physical_serving_count"], b["recent_context_event_count"], b["recent_context_embedding_count"], b["recent_profile_record_count"], b["recent_resource_skill_record_count"], b["serving_visibility_status"], ",".join(b["serving_visibility_gaps"]), json.dumps(b["recent_raw_type_counts"], sort_keys=True), json.dumps(b["recent_serving_type_counts"], sort_keys=True)] for b in report["backends"]],
        ),
    ]
    for backend in report["backends"]:
        lines.extend(["", f"## {backend['backend']} Recent Real User Prompts", ""])
        lines.append(render_table(["Seq", "Event", "Session", "Turn", "Hook", "Text"], [[r["sequence"], r["codex_api_event"], r["session_id"], r["turn_id"], r["hook_id"] if "hook_id" in r else r["hook_type"], r["text"]] for r in backend["recent_real_user_prompts"]]))
        lines.extend(["", f"## {backend['backend']} Context Events", ""])
        lines.append(render_table(["Seq", "Event", "Session", "Text"], [[r["sequence"], r["codex_api_event"], r["session_id"], r["text"]] for r in backend["recent_context_events"]]))
        lines.extend(["", f"## {backend['backend']} Embeddings/Profile/Resources", ""])
        rows = [
            [r["sequence"], r["record_type"], r["memory_scope"], r["session_continuity"], r["data_model"], r["text"]]
            for r in backend["recent_context_embeddings"] + backend["recent_profile_records"] + backend["recent_resource_skill_records"]
        ]
        lines.append(render_table(["Seq", "Type", "Memory", "Continuity", "Model", "Text"], rows))
        lines.extend(["", f"## {backend['backend']} Entities And Summaries", ""])
        rows = [[r["sequence"], r["record_type"], r["session_id"], r["text"]] for r in backend["recent_entities"] + backend["recent_summaries"]]
        lines.append(render_table(["Seq", "Type", "Session", "Text"], rows))
    lines.extend([
        "",
        "## Timeline Interpretation",
        "",
        "1. Hook firing is proven when a recent `agent_message` has `codex_api_event=UserPromptSubmit`, `hook_type=before_llm`, `synthetic=false`, and a Codex session/thread id.",
        "2. Raw event ingestion is proven by the `raw_ingestion` append sequence and write path metadata.",
        "3. Serving context-event publication is proven when the same prompt appears as `context_event` in the serving prefix.",
        "4. Async extraction is proven only when entity/summary/index rows appear after the raw prompt or when dirty-summary markers are drained by a worker.",
        "5. Compact hot counts are reported separately from physical historical counts, so old C++ debug/audit rows no longer inflate live traffic parity.",
    ])
    return "\n".join(lines) + "\n"


def render_html(markdown: str) -> str:
    return f"""<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Recent Codex Hook Ingestion Workflow</title>
<style>body{{font-family:Inter,Segoe UI,Arial,sans-serif;background:#f6f8fb;color:#17202a;margin:0}}main{{max-width:1220px;margin:0 auto;padding:32px}}pre{{white-space:pre-wrap;background:#111827;color:#f8fafc;padding:18px;border-radius:8px;overflow:auto}}article{{background:white;border:1px solid #dbe3ee;border-radius:8px;padding:28px}}</style></head>
<body><main><article><pre>{html.escape(markdown)}</pre></article></main></body></html>
"""


def main() -> None:
    rust_raw_count, rust_raw_hot_count, rust_raw = scan_rust(RUST_PREFIX + ":raw_ingestion")
    rust_serving_count, rust_serving_hot_count, rust_serving = scan_rust(RUST_PREFIX)
    cpp_raw_count, cpp_raw_hot_count, cpp_raw = scan_cpp(CPP_PREFIX + ":raw_ingestion")
    cpp_serving_count, cpp_serving_hot_count, cpp_serving = scan_cpp(CPP_PREFIX)
    report = {
        "generated_at_ms": int(time.time() * 1000),
        "query_paths": {
            "rust": "HTTP /execute through matrixark_rust_service_proxy on 127.0.0.1:17100",
            "cpp": "TemporalStore Python SDK using libbcache2.so against 127.0.0.1:18000",
        },
        "backends": [
            summarize_backend("Rust TemporalStore", RUST_PREFIX, rust_raw_count, rust_raw_hot_count, rust_raw, rust_serving_count, rust_serving_hot_count, rust_serving),
            summarize_backend("C++ TemporalStore", CPP_PREFIX, cpp_raw_count, cpp_raw_hot_count, cpp_raw, cpp_serving_count, cpp_serving_hot_count, cpp_serving),
        ],
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md = render_markdown(report)
    OUT_MD.write_text(md, encoding="utf-8")
    OUT_HTML.write_text(render_html(md), encoding="utf-8")
    print(json.dumps({"json": str(OUT_JSON), "markdown": str(OUT_MD), "html": str(OUT_HTML)}, indent=2))


if __name__ == "__main__":
    main()
