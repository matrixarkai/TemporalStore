#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, os, sys, time
from pathlib import Path
from typing import Any
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.run_matrixark_mcp_backend_parity import McpProcess, _backend_command, _call_tool
Json = dict[str, Any]
REPORT_DIR = Path(os.environ.get("MATRIXARK_RESOURCE_SKILL_PARITY_REPORT_DIR", "/tmp/matrixark-resource-skill-parity"))

def sample_files(base: Path) -> dict[str, Path]:
    base.mkdir(parents=True, exist_ok=True)
    md = base / "gpu_runbook.md"
    md.write_text("# GPU Runbook\n\nGPU budget requests require finance approval before purchase.\n\n## Rollback\n\nRollback requires notifying finance and the infrastructure owner.\n", encoding="utf-8")
    txt = base / "oncall_policy.txt"
    txt.write_text("On-call incidents must be escalated to the storage lead after fifteen minutes.\n\nA resolved incident should be summarized for the team knowledge base.\n", encoding="utf-8")
    pdf = base / "budget_policy.pdf"
    pdf.write_text("PDF fallback text: budget exceptions over 40000 dollars require CFO approval.\n\nThe approval evidence must be cited in the final context pack.\n", encoding="utf-8")
    skill = base / "SKILL.md"
    skill.write_text("---\nname: context-debugger\ndescription: Inspect MatrixArk context packs and replay evidence.\ntriggers:\n  - inspect selected refs\n  - replay evidence\nallowed_tools:\n  - matrixark_replay\n  - matrixark_audit\nowner_scope: team\nstatus: active\nprecedence: high\n---\n# Context Debugger\n\nUse this skill when a user asks why a context pack selected a reference.\n\n## Steps\n\nOpen the replay, inspect selected refs, and verify source evidence.\n", encoding="utf-8")
    return {"md": md, "txt": txt, "pdf": pdf, "skill": skill}

def counts(records: list[Json]) -> dict[str, int]:
    out: dict[str, int] = {}
    for record in records:
        kind = str(record.get("record_type") or "")
        if kind:
            out[kind] = out.get(kind, 0) + 1
    return out

def embedding_types(records: list[Json]) -> list[str]:
    return sorted({str(record.get("embedding_type")) for record in records if record.get("record_type") == "context_embedding" and record.get("embedding_type")})

def index_names(records: list[Json]) -> list[str]:
    return sorted({str(record.get("index_name")) for record in records if record.get("record_type") == "context_index" and record.get("index_name")})

def require(ok: bool, message: str) -> None:
    if not ok:
        raise AssertionError(message)

def run_backend(backend: str, run_id: str) -> Json:
    env = os.environ.copy()
    env.setdefault("MATRIXARK_EMBEDDING_PROVIDER", "hash")
    env.setdefault("MATRIXARK_UNDERSTANDING_PROVIDER", "rules")
    env.setdefault("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "0")
    env.setdefault("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "0")
    env.setdefault("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "8000")
    env["MATRIXARK_TEMPORALSTORE_PREFIX"] = f"matrixark:resource-skill-parity:{backend}:{run_id}"
    files = sample_files(REPORT_DIR / f"samples-{run_id}-{backend}")
    scope = {"account_id": "acct_resource_skill_parity", "tenant_id": "tenant_resource_skill_parity", "user_id": f"user_{backend}", "session_id": f"session_{backend}_{run_id}", "team": "team_context"}
    proc = McpProcess(backend, _backend_command(backend), env)
    try:
        proc.request("initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "matrixark-resource-skill-parity", "version": "1.0"}}, timeout_s=120.0)
        proc.notify("notifications/initialized")
        ingests: dict[str, Json] = {}
        specs = [
            ("md", "resource", "md", files["md"], ["resources", "runbooks", "gpu"], 11000),
            ("txt", "resource", "txt", files["txt"], ["resources", "policies", "oncall"], 12000),
            ("pdf", "resource", "pdf", files["pdf"], ["resources", "policies", "budget"], 13000),
            ("skill", "skill", "skill", files["skill"], ["skills", "context-debugger"], 14000),
        ]
        for name, kind, resource_type, path, node_path, chunk_base in specs:
            ingests[name] = _call_tool(proc, "matrixark_ingest", {"kind": kind, "raw_uri": str(path), "resource_type": resource_type, "messages": [{"role": "tool", "content": "ingest resource from raw_uri"}], "scope": scope, "metadata": {"node_path": node_path}, "chunk_hash_base": chunk_base}, timeout_s=120.0)
        resources = _call_tool(proc, "matrixark_list_resources", {"scope": scope}, timeout_s=120.0)
        skills = _call_tool(proc, "matrixark_list_skills", {"scope": scope, "include_disabled": True}, timeout_s=120.0)
        resource_pack = _call_tool(proc, "matrixark_retrieve", {"query": "Which resource says GPU budget requests require finance approval?", "scope": scope, "max_context_tokens": 700}, timeout_s=120.0)
        skill_pack = _call_tool(proc, "matrixark_retrieve", {"query": "Which skill helps inspect selected refs and replay evidence?", "scope": scope, "max_context_tokens": 700}, timeout_s=120.0)
        replay = _call_tool(proc, "matrixark_replay", {"context_pack_id": "resource-skill-debug", "scope": scope}, timeout_s=120.0)
        records = replay.get("events", [])
        update = _call_tool(proc, "matrixark_update_skill", {"skill_hash": ingests["skill"].get("skill_hash"), "status": "disabled", "triggers": ["manual replay only"], "allowed_tools": ["matrixark_replay"]}, timeout_s=120.0)
        disabled_pack = _call_tool(proc, "matrixark_retrieve", {"query": "Which skill helps inspect selected refs and replay evidence?", "scope": scope, "max_context_tokens": 700}, timeout_s=120.0)
        other_scope = dict(scope)
        other_scope["user_id"] = f"other_user_{backend}"
        other_scope["session_id"] = f"other_session_{run_id}"
        other_pack = _call_tool(proc, "matrixark_retrieve", {"query": "GPU budget finance approval", "scope": other_scope, "max_context_tokens": 700}, timeout_s=120.0)
        resource_types = {str(ref.get("ref_type")) for ref in resource_pack.get("selected_refs", [])}
        skill_types = {str(ref.get("ref_type")) for ref in skill_pack.get("selected_refs", [])}
        disabled_types = {str(ref.get("ref_type")) for ref in disabled_pack.get("selected_refs", [])}
        record_counts = counts(records)
        checks = {
            "all_ingests_accepted": all(item.get("status") == "accepted" for item in ingests.values()),
            "chunks_present": all(item.get("resource_chunks") for item in ingests.values()),
            "skill_hash_present": isinstance(ingests["skill"].get("skill_hash"), int),
            "resource_registry_count": int(resources.get("count") or 0) == 3,
            "skill_registry_count": int(skills.get("count") or 0) == 1,
            "resource_chunk_selected": "resource_chunk" in resource_types,
            "skill_selected": bool({"skill", "skill_section"}.intersection(skill_types)),
            "disabled_skill_not_selected": not {"skill", "skill_section"}.intersection(disabled_types),
            "cross_user_isolated": len(other_pack.get("selected_refs", [])) == 0,
            "resource_manifests_written": record_counts.get("resource_manifest", 0) == 3,
            "skill_manifest_written": record_counts.get("skill_manifest", 0) == 1,
            "resource_chunks_written": record_counts.get("resource_chunk", 0) >= 4,
            "skill_sections_written": record_counts.get("skill_section", 0) >= 1,
            "summaries_written": record_counts.get("context_summary", 0) >= 4,
            "embeddings_written": record_counts.get("context_embedding", 0) >= 8,
            "indexes_written": record_counts.get("context_index", 0) >= 8,
        }
        for check_name, ok in checks.items():
            require(ok, f"{backend} failed {check_name}")
        return {"backend": backend, "ok": True, "storage_prefix": env["MATRIXARK_TEMPORALSTORE_PREFIX"], "checks": checks, "ingests": {key: {"status": value.get("status"), "resource_chunks": value.get("resource_chunks", []), "skill_hash": value.get("skill_hash")} for key, value in ingests.items()}, "registries": {"resource_count": resources.get("count"), "skill_count": skills.get("count"), "skill_update_status": update.get("status")}, "retrieval": {"resource_ref_types": sorted(resource_types), "resource_selected_count": len(resource_pack.get("selected_refs", [])), "skill_ref_types": sorted(skill_types), "skill_selected_count": len(skill_pack.get("selected_refs", [])), "disabled_skill_selected_count": len(disabled_pack.get("selected_refs", [])), "cross_user_selected_count": len(other_pack.get("selected_refs", []))}, "storage_records": {"record_counts": record_counts, "embedding_types": embedding_types(records), "index_names": index_names(records)}}
    finally:
        proc.close()

def compare(results: list[Json]) -> Json:
    by_backend = {item["backend"]: item for item in results if item.get("ok")}
    if "cpp" not in by_backend or "rust" not in by_backend:
        return {"status": "skipped", "reason": "need both cpp and rust"}
    cpp = by_backend["cpp"]
    rust = by_backend["rust"]
    checks = {
        "resource_registry_count_equal": cpp["registries"]["resource_count"] == rust["registries"]["resource_count"],
        "skill_registry_count_equal": cpp["registries"]["skill_count"] == rust["registries"]["skill_count"],
        "resource_ref_types_equal": cpp["retrieval"]["resource_ref_types"] == rust["retrieval"]["resource_ref_types"],
        "skill_ref_types_equal": cpp["retrieval"]["skill_ref_types"] == rust["retrieval"]["skill_ref_types"],
        "embedding_types_equal": cpp["storage_records"]["embedding_types"] == rust["storage_records"]["embedding_types"],
    }
    return {"status": "passed" if all(checks.values()) else "warning", "checks": checks}

def write_report(run_id: str, results: list[Json], failures: list[str]) -> tuple[Path, Path]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    comparison = compare(results)
    report = {"run_id": run_id, "all_ok": not failures, "failures": failures, "comparison": comparison, "results": results}
    report_json = REPORT_DIR / f"matrixark_resource_skill_backend_parity_{run_id}.json"
    report_md = REPORT_DIR / f"matrixark_resource_skill_backend_parity_{run_id}.md"
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# MatrixArk Resource And Skill Backend Parity", "", f"Run ID: {run_id}", f"All OK: {report['all_ok']}", f"Comparison: {comparison.get('status')}", "", "## What Was Tested", "", "- Markdown, text, and PDF resource ingestion", "- SKILL.md parsing into manifest and sections", "- ResourceManifest, ResourceChunk, SkillManifest, SkillSection writes", "- L0 summaries and embeddings for resources and skills", "- ContextIndex entries for resource and skill filtering", "- ResourceRegistry and SkillRegistry list APIs", "- ContextPack retrieval for resource chunks and selected skill instructions", "- Skill disable/update behavior", "- Cross-user scope isolation", ""]
    for item in results:
        storage_records = item.get("storage_records", {})
        lines.extend([f"## {item.get('backend')}", "", f"- OK: {item.get('ok')}", f"- Storage prefix: {item.get('storage_prefix', '')}", f"- Resource count: {item.get('registries', {}).get('resource_count')}", f"- Skill count: {item.get('registries', {}).get('skill_count')}", f"- Resource selected refs: {item.get('retrieval', {}).get('resource_selected_count')}", f"- Skill selected refs: {item.get('retrieval', {}).get('skill_selected_count')}", f"- Disabled skill selected refs: {item.get('retrieval', {}).get('disabled_skill_selected_count')}", f"- Cross-user selected refs: {item.get('retrieval', {}).get('cross_user_selected_count')}", f"- Record counts: {json.dumps(storage_records.get('record_counts', {}), sort_keys=True)}", f"- Embedding types: {', '.join(storage_records.get('embedding_types', []))}", f"- Index names: {', '.join(storage_records.get('index_names', [])[:20])}", ""])
        if item.get("error"):
            lines.append(f"Error: {item['error']}\n")
    lines.extend(["## C++ Vs Rust Comparison", "", json.dumps(comparison, indent=2, sort_keys=True), ""])
    report_md.write_text("\n".join(lines), encoding="utf-8")
    return report_json, report_md

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backends", nargs="+", default=["cpp", "rust"], choices=["local", "local-nometa", "cpp", "rust"])
    parser.add_argument("--run-id", default=str(int(time.time())))
    args = parser.parse_args()
    results: list[Json] = []
    failures: list[str] = []
    for backend in args.backends:
        started = time.monotonic()
        try:
            result = run_backend(backend, args.run_id)
            result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 2)
            results.append(result)
            print(json.dumps(result, indent=2, sort_keys=True))
        except Exception as exc:
            failure = {"backend": backend, "ok": False, "error": str(exc), "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}
            results.append(failure)
            failures.append(backend)
            print(json.dumps(failure, indent=2, sort_keys=True))
    report_json, report_md = write_report(args.run_id, results, failures)
    print(f"report_json={report_json}")
    print(f"report_md={report_md}")
    comparison = compare(results)
    return 0 if not failures and comparison.get("status") in {"passed", "skipped"} else 1
if __name__ == "__main__":
    raise SystemExit(main())

