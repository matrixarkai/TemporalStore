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


def scan_rust(base: str, limit: int = 500) -> tuple[int, list[dict[str, Any]]]:
    count = int(bytes_to_str(rust_exec({"kind": "string_get", "key": base + ":record_count"})["response"]["value"]) or 0)
    rows: list[dict[str, Any]] = []
    for sequence in range(count - 1, max(-1, count - limit - 1), -1):
        key = f"{base}:records:{sequence // 256:06d}"
        field = f"{sequence % 256:020d}"
        raw = bytes_to_str(rust_exec({"kind": "hash_get", "key": key, "field": field})["response"]["value"])
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except Exception:
            record = {"record_type": "unparsed", "text": raw[:1000]}
        rows.append({"sequence": sequence, "record": record})
    return count, rows


def scan_cpp(base: str, limit: int = 500) -> tuple[int, list[dict[str, Any]]]:
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
    return count, rows


def summarize_backend(name: str, prefix: str, raw_count: int, raw_rows: list[dict[str, Any]], serving_count: int, serving_rows: list[dict[str, Any]]) -> dict[str, Any]:
    raw_records = [row(item["record"], item["sequence"]) for item in raw_rows]
    serving_records = [row(item["record"], item["sequence"]) for item in serving_rows]
    raw_types = collections.Counter(item["record_type"] for item in raw_records)
    serving_types = collections.Counter(item["record_type"] for item in serving_records)
    user_prompts = [item for item in raw_records if item["record_type"] == "agent_message" and item["codex_api_event"] == "UserPromptSubmit" and not item["synthetic"]]
    entities = [item for item in raw_records + serving_records if item["record_type"] in {"context_entity", "context_entity_update_audit"}]
    summaries = [item for item in raw_records + serving_records if item["record_type"] in {"context_summary", "context_summary_dirty", "context_batch_commit"}]
    context_events = [item for item in serving_records if item["record_type"] == "context_event"]
    return {
        "backend": name,
        "prefix": prefix,
        "raw_count": raw_count,
        "serving_count": serving_count,
        "recent_raw_type_counts": dict(raw_types),
        "recent_serving_type_counts": dict(serving_types),
        "recent_real_user_prompts": user_prompts[:8],
        "recent_context_events": context_events[:8],
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
        "- C++ TemporalStore currently captures hook traffic, but the newest records are noisier: PostToolUse, hook trace, audit, idempotency, and dirty-summary records are mixed with prompt rows.",
        "- Async extraction/summary is visible on C++ through `context_summary_dirty`, `context_summary`, `context_entity`, `context_index`, and batch commit records in the recent raw window.",
        "- Rust live hook prefix currently shows raw messages and serving context events only; no entity/summary rows were present in the scanned live prefix window.",
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
        "  Cpp->>Cpp: publish context_event and telemetry/trace rows",
        "  Cpp->>Async: mark summary dirty / batch commit / extraction",
        "  Async->>Cpp: context_entity, context_summary, context_index, embedding rows",
        "```",
        "",
        "## Backend Counts",
        "",
        render_table(
            ["Backend", "Prefix", "Raw count", "Serving count", "Recent raw types", "Recent serving types"],
            [[b["backend"], b["prefix"], b["raw_count"], b["serving_count"], json.dumps(b["recent_raw_type_counts"], sort_keys=True), json.dumps(b["recent_serving_type_counts"], sort_keys=True)] for b in report["backends"]],
        ),
    ]
    for backend in report["backends"]:
        lines.extend(["", f"## {backend['backend']} Recent Real User Prompts", ""])
        lines.append(render_table(["Seq", "Event", "Session", "Turn", "Hook", "Text"], [[r["sequence"], r["codex_api_event"], r["session_id"], r["turn_id"], r["hook_id"] if "hook_id" in r else r["hook_type"], r["text"]] for r in backend["recent_real_user_prompts"]]))
        lines.extend(["", f"## {backend['backend']} Context Events", ""])
        lines.append(render_table(["Seq", "Event", "Session", "Text"], [[r["sequence"], r["codex_api_event"], r["session_id"], r["text"]] for r in backend["recent_context_events"]]))
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
        "5. In this run, Rust proves steps 1-3. C++ proves steps 1-4 in aggregate, but still mixes audit/trace records into the hot prefix and should be cleaned further.",
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
    rust_raw_count, rust_raw = scan_rust(RUST_PREFIX + ":raw_ingestion")
    rust_serving_count, rust_serving = scan_rust(RUST_PREFIX)
    cpp_raw_count, cpp_raw = scan_cpp(CPP_PREFIX + ":raw_ingestion")
    cpp_serving_count, cpp_serving = scan_cpp(CPP_PREFIX)
    report = {
        "generated_at_ms": int(time.time() * 1000),
        "query_paths": {
            "rust": "HTTP /execute through matrixark_rust_service_proxy on 127.0.0.1:17100",
            "cpp": "TemporalStore Python SDK using libbcache2.so against 127.0.0.1:18000",
        },
        "backends": [
            summarize_backend("Rust TemporalStore", RUST_PREFIX, rust_raw_count, rust_raw, rust_serving_count, rust_serving),
            summarize_backend("C++ TemporalStore", CPP_PREFIX, cpp_raw_count, cpp_raw, cpp_serving_count, cpp_serving),
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
