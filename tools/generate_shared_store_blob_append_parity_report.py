#!/usr/bin/env python3
"""Generate Rust/C++ shared-store append-blob parity evidence.

The Rust side is live runtime evidence from the MatrixObject-backed protobuf
append-blob WAL workflow. The C++ side is contract evidence from the
MatrixObjectStore AppendObject implementation and RPC surface, because the
TemporalStore shared-store C++ integration delegates append placement to
MatrixObject's ObjectInfo metadata.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIXOBJECT_REPO = Path("/root/src/github-services/MatrixObjectStore")
SCHEMA = "temporalstore_shared_store_blob_append_cpp_rust_parity_v1"


def _run(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int = 180,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        argv,
        cwd=cwd,
        env=merged_env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def _git_rev(repo: Path, ref: str = "HEAD") -> str:
    result = _run(["git", "rev-parse", ref], cwd=repo, timeout=30)
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip()


def _load_rust_runtime_report(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    if args.rust_report:
        data = json.loads(Path(args.rust_report).read_text(encoding="utf-8"))
        return data, {
            "mode": "loaded",
            "path": str(args.rust_report),
            "returncode": 0,
            "stderr_tail": "",
        }

    env = {
        "CARGO_TARGET_DIR": args.cargo_target_dir,
        "TEMPORALSTORE_APPEND_BLOB_PARITY_ENTRIES": str(args.entries),
        "TEMPORALSTORE_APPEND_BLOB_PARITY_VALUE_BYTES": str(args.value_bytes),
    }
    command = [
        "cargo",
        "run",
        "-p",
        "temporalstore-rust",
        "--features",
        "matrixobject",
        "--example",
        "shared_store_append_blob_parity_report",
    ]
    started = time.time()
    result = _run(command, cwd=ROOT, env=env, timeout=args.timeout_seconds)
    command_report = {
        "mode": "executed",
        "argv": command,
        "returncode": result.returncode,
        "elapsed_ms": round((time.time() - started) * 1000, 3),
        "stderr_tail": result.stderr[-4000:],
    }
    if result.returncode != 0:
        return {"schema": "rust_runtime_failed", "stdout_tail": result.stdout[-4000:]}, command_report
    try:
        return json.loads(result.stdout), command_report
    except json.JSONDecodeError:
        return {
            "schema": "rust_runtime_invalid_json",
            "stdout_tail": result.stdout[-4000:],
        }, command_report


def _source_contains(path: Path, patterns: list[str]) -> dict[str, bool]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {pattern: False for pattern in patterns}
    return {pattern: bool(re.search(pattern, text, re.MULTILINE | re.DOTALL)) for pattern in patterns}


def _cpp_contract(matrixobject_repo: Path) -> dict[str, Any]:
    object_cc = matrixobject_repo / "matrixobjectstore/objectstore/objectstore.cc"
    rpc_cc = matrixobject_repo / "matrixobjectstore/objectstore/objectstore_rpc.cc"
    object_h = matrixobject_repo / "matrixobjectstore/objectstore/objectstore.h"
    rust_lib = matrixobject_repo / "rust/matrixobjectstore-rs/src/lib.rs"

    object_cc_checks = _source_contains(
        object_cc,
        [
            r"Status\s+ObjectStore::AppendObject",
            r"const\s+uint64_t\s+previous_size",
            r"info\.size\s*=\s*appended_size",
            r"info\.offset\s*=\s*extents\.front\(\)\.offset",
            r"info\.extents\s*=\s*extents",
            r"AppendCommittedObjectChangeLocked\(\"append_object\"",
        ],
    )
    rpc_checks = _source_contains(
        rpc_cc,
        [
            r"request\.method\s*==\s*\"AppendObject\"",
            r"\[prefix\s*\+\s*\"offset\"\]\s*=\s*std::to_string\(info\.offset\)",
            r"\[extent_prefix\s*\+\s*\"offset\"\]\s*=\s*std::to_string\(info\.extents\[i\]\.offset\)",
            r"\[extent_prefix\s*\+\s*\"length\"\]\s*=\s*std::to_string\(info\.extents\[i\]\.length\)",
        ],
    )
    header_checks = _source_contains(
        object_h,
        [
            r"struct\s+ObjectInfo",
            r"uint64_t\s+offset\s*=\s*0",
            r"std::vector<ObjectExtent>\s+extents",
            r"Status\s+AppendObject",
        ],
    )
    rust_api_checks = _source_contains(
        rust_lib,
        [
            r"pub\s+fn\s+append_object",
            r"->\s*Result<ObjectMetadata,\s*ObjectError>",
            r"pub\s+struct\s+ObjectMetadata",
            r"pub\s+extents:\s+Vec<ObjectExtent>",
        ],
    )
    all_checks = {
        **{f"object_cc:{key}": value for key, value in object_cc_checks.items()},
        **{f"rpc_cc:{key}": value for key, value in rpc_checks.items()},
        **{f"object_h:{key}": value for key, value in header_checks.items()},
        **{f"rust_api:{key}": value for key, value in rust_api_checks.items()},
    }
    return {
        "backend": "cpp",
        "matrixobject_repo": str(matrixobject_repo),
        "matrixobject_commit": _git_rev(matrixobject_repo),
        "evidence_type": "source_contract",
        "append_object_returns_object_info": all(object_cc_checks.values()) and all(header_checks.values()),
        "rpc_exposes_offset_and_extents": all(rpc_checks.values()),
        "rust_matrixobject_api_exposes_metadata": all(rust_api_checks.values()),
        "checks": all_checks,
        "source_files": {
            "object_cc": str(object_cc),
            "objectstore_rpc_cc": str(rpc_cc),
            "objectstore_h": str(object_h),
            "rust_lib": str(rust_lib),
        },
    }


def _rust_summary(rust_report: dict[str, Any]) -> dict[str, Any]:
    summary = rust_report.get("summary") if isinstance(rust_report, dict) else None
    if not isinstance(summary, dict):
        return {
            "runtime_valid": False,
            "reason": "missing summary",
        }
    return {
        "runtime_valid": rust_report.get("schema") == "temporalstore_shared_store_append_blob_parity_report_v1",
        "offsets_monotonic": bool(summary.get("direct_offsets_monotonic")),
        "offsets_contiguous": bool(summary.get("direct_offsets_contiguous")),
        "sync_reports_include_offsets": bool(summary.get("sync_reports_include_offsets")),
        "async_flush_reports_include_offsets": bool(summary.get("async_flush_reports_include_offsets")),
        "replay_recovered_all_records": bool(summary.get("replay_recovered_all_records")),
        "retrieval_recovered_all_records": bool(summary.get("retrieval_recovered_all_records")),
        "append_latency_avg_us": summary.get("append_latency_avg_us"),
        "append_latency_p95_us": summary.get("append_latency_p95_us"),
        "replay_latency_total_us": summary.get("replay_latency_total_us"),
        "retrieval_latency_avg_us": summary.get("retrieval_latency_avg_us"),
    }


def _parity_status(rust: dict[str, Any], cpp: dict[str, Any]) -> dict[str, Any]:
    rust_summary = _rust_summary(rust)
    checks = {
        "rust_runtime_valid": bool(rust_summary.get("runtime_valid")),
        "rust_offsets_monotonic": bool(rust_summary.get("offsets_monotonic")),
        "rust_offsets_contiguous": bool(rust_summary.get("offsets_contiguous")),
        "rust_sync_reports_include_offsets": bool(rust_summary.get("sync_reports_include_offsets")),
        "rust_async_flush_reports_include_offsets": bool(
            rust_summary.get("async_flush_reports_include_offsets")
        ),
        "rust_replay_recovered_all_records": bool(rust_summary.get("replay_recovered_all_records")),
        "rust_retrieval_recovered_all_records": bool(
            rust_summary.get("retrieval_recovered_all_records")
        ),
        "cpp_append_object_returns_object_info": bool(cpp.get("append_object_returns_object_info")),
        "cpp_rpc_exposes_offset_and_extents": bool(cpp.get("rpc_exposes_offset_and_extents")),
        "matrixobject_rust_api_exposes_metadata": bool(
            cpp.get("rust_matrixobject_api_exposes_metadata")
        ),
    }
    blockers = [name for name, passed in checks.items() if not passed]
    return {
        "status": "passed" if not blockers else "failed",
        "checks": checks,
        "blockers": blockers,
        "note": (
            "Rust live append/replay metrics are compared with C++ MatrixObject append contract "
            "evidence. Full same-hardware C++ runtime latency parity still requires a C++ "
            "TemporalStore append-blob runtime emitter."
        ),
    }


def _render_html(report: dict[str, Any]) -> str:
    status = report["parity"]["status"]
    rust_summary = report["rust_summary"]
    cpp = report["cpp_contract"]
    rows = [
        ("Status", status),
        ("TemporalStore commit", report["temporalstore_commit"]),
        ("MatrixObject commit", cpp["matrixobject_commit"]),
        ("Rust avg append latency us", rust_summary.get("append_latency_avg_us")),
        ("Rust p95 append latency us", rust_summary.get("append_latency_p95_us")),
        ("Rust total replay latency us", rust_summary.get("replay_latency_total_us")),
        ("Rust avg retrieval latency us", rust_summary.get("retrieval_latency_avg_us")),
    ]
    check_rows = "\n".join(
        f"<tr><td>{html.escape(key)}</td><td>{'pass' if value else 'fail'}</td></tr>"
        for key, value in report["parity"]["checks"].items()
    )
    summary_rows = "\n".join(
        f"<tr><td>{html.escape(str(name))}</td><td>{html.escape(str(value))}</td></tr>"
        for name, value in rows
    )
    raw = html.escape(json.dumps(report, indent=2, sort_keys=True))
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>TemporalStore Shared-Store Append Blob Parity</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 32px; color: #202124; }}
    h1, h2 {{ margin-bottom: 8px; }}
    table {{ border-collapse: collapse; margin: 16px 0; width: 100%; }}
    th, td {{ border: 1px solid #d0d7de; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f6f8fa; }}
    .pill {{ display: inline-block; padding: 4px 10px; border-radius: 999px; background: {'#dafbe1' if status == 'passed' else '#ffebe9'}; }}
    pre {{ white-space: pre-wrap; background: #f6f8fa; padding: 12px; border: 1px solid #d0d7de; overflow: auto; }}
  </style>
</head>
<body>
  <h1>TemporalStore Shared-Store Append Blob Parity</h1>
  <p><span class="pill">{html.escape(status)}</span></p>
  <h2>Summary</h2>
  <table><tbody>{summary_rows}</tbody></table>
  <h2>Parity Checks</h2>
  <table><thead><tr><th>Check</th><th>Result</th></tr></thead><tbody>{check_rows}</tbody></table>
  <h2>Raw Evidence</h2>
  <pre>{raw}</pre>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="docs/benchmarks/shared_store_blob_append_parity")
    parser.add_argument("--matrixobject-repo", default=str(DEFAULT_MATRIXOBJECT_REPO))
    parser.add_argument("--cargo-target-dir", default="/root/src/github-services/TemporalStore/target")
    parser.add_argument("--entries", type=int, default=8)
    parser.add_argument("--value-bytes", type=int, default=64)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--rust-report")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    rust_runtime, command_report = _load_rust_runtime_report(args)
    cpp_contract = _cpp_contract(Path(args.matrixobject_repo))
    report = {
        "schema": SCHEMA,
        "generated_at_unix": int(time.time()),
        "temporalstore_repo": str(ROOT),
        "temporalstore_commit": _git_rev(ROOT),
        "rust_command": command_report,
        "rust_summary": _rust_summary(rust_runtime),
        "rust_runtime": rust_runtime,
        "cpp_contract": cpp_contract,
    }
    report["parity"] = _parity_status(rust_runtime, cpp_contract)

    json_path = output_dir / "shared_store_blob_append_parity_report.json"
    html_path = output_dir / "shared_store_blob_append_parity_report.html"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    html_path.write_text(_render_html(report), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "html": str(html_path), "status": report["parity"]["status"]}, indent=2))
    return 0 if report["parity"]["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
