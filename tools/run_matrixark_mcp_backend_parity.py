#!/usr/bin/env python3
"""Run one MatrixArk MCP ingest/retrieve parity flow across storage backends.

This validates that the MCP server can use the same MatrixArk extraction,
ingestion, retrieval, summary, and context-pack logic over local JSONL, C++
TemporalStore direct SDK storage, and Rust TemporalStore direct SDK storage.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = Path(os.environ.get("MATRIXARK_MCP_PARITY_REPORT_DIR", "/tmp/matrixark-mcp-backend-parity"))


def _is_json_line(line: str) -> bool:
    return line.lstrip().startswith("{")


class McpProcess:
    def __init__(self, name: str, command: list[str], env: dict[str, str]) -> None:
        self.name = name
        self.proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.next_id = 1

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    def _read_response(self, request_id: int, timeout_s: float = 60.0) -> dict[str, Any]:
        assert self.proc.stdout is not None
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            line = self.proc.stdout.readline()
            if not line:
                if self.proc.poll() is not None:
                    stderr = self.proc.stderr.read() if self.proc.stderr else ""
                    raise RuntimeError(f"{self.name} exited early ({self.proc.returncode}): {stderr[-1000:]}")
                time.sleep(0.02)
                continue
            if not _is_json_line(line):
                continue
            payload = json.loads(line)
            if payload.get("id") == request_id:
                if "error" in payload:
                    raise RuntimeError(f"{self.name} MCP error: {payload['error']}")
                return payload
        stderr = self.proc.stderr.read() if self.proc.stderr else ""
        raise TimeoutError(f"timed out waiting for {self.name} response id={request_id}; stderr={stderr[-1000:]}")

    def request(self, method: str, params: dict[str, Any] | None = None, timeout_s: float = 60.0) -> dict[str, Any]:
        assert self.proc.stdin is not None
        request_id = self.next_id
        self.next_id += 1
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        self.proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()
        return self._read_response(request_id, timeout_s=timeout_s)

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        assert self.proc.stdin is not None
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self.proc.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.proc.stdin.flush()


def _call_tool(proc: McpProcess, name: str, arguments: dict[str, Any], timeout_s: float = 60.0) -> dict[str, Any]:
    response = proc.request("tools/call", {"name": name, "arguments": arguments}, timeout_s=timeout_s)
    result = response.get("result", {})
    content = result.get("content") or []
    if not content:
        return result
    text = content[0].get("text", "{}")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


def _backend_command(backend: str) -> list[str]:
    if backend == "local":
        return [sys.executable, str(ROOT / "tools" / "matrixark_mcp_server.py"), "--backend", "local", "--line-json"]
    if backend == "cpp":
        return ["bash", str(ROOT / "tools" / "matrixark_mcp_cpp_server.sh"), "--line-json"]
    if backend == "rust":
        return ["bash", str(ROOT / "tools" / "matrixark_mcp_rust_server.sh"), "--line-json"]
    raise ValueError(f"unknown backend {backend}")


def run_backend(backend: str, run_id: str) -> dict[str, Any]:
    env = os.environ.copy()
    env.setdefault("MATRIXARK_EMBEDDING_PROVIDER", "hash")
    env.setdefault("MATRIXARK_UNDERSTANDING_PROVIDER", "rules")
    env.setdefault("MATRIXARK_REQUIRE_OSS_EMBEDDINGS", "0")
    env.setdefault("MATRIXARK_REQUIRE_OSS_UNDERSTANDING", "0")
    env.setdefault("MATRIXARK_RETRIEVAL_TIMEOUT_MS", "5000")
    env["MATRIXARK_TEMPORALSTORE_PREFIX"] = f"matrixark:mcp:parity:{backend}:{run_id}"
    if backend == "local":
        env["MATRIXARK_MCP_EVENT_LOG"] = str(REPORT_DIR / f"{run_id}-{backend}.jsonl")
    proc = McpProcess(backend, _backend_command(backend), env)
    try:
        proc.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "matrixark-backend-parity", "version": "1.0"},
            },
            timeout_s=90.0,
        )
        proc.notify("notifications/initialized")
        scope = {
            "account_id": "acct_mcp_parity",
            "tenant_id": "tenant_mcp_parity",
            "user_id": f"user_{backend}",
            "session_id": f"session_{backend}_{run_id}",
        }
        ingest = _call_tool(
            proc,
            "matrixark_ingest",
            {
                "messages": [
                    {"role": "user", "content": "Alice approved the GPU purchase for Project Falcon."},
                    {"role": "assistant", "content": "Recorded approval for the Project Falcon GPU purchase."},
                ],
                "scope": scope,
                "metadata": {"node_path": ["memory", "approvals", "gpu"]},
            },
            timeout_s=90.0,
        )
        refresh = _call_tool(proc, "matrixark_refresh_summaries", {"scope": scope}, timeout_s=90.0)
        retrieve = _call_tool(
            proc,
            "matrixark_retrieve",
            {
                "query": "what GPU approvals are current for Project Falcon?",
                "scope": scope,
                "max_context_tokens": 1200,
            },
            timeout_s=90.0,
        )
        selected_refs = retrieve.get("selected_refs") or []
        sections = retrieve.get("sections") or []
        ok = bool(selected_refs or sections or retrieve.get("context_pack_id"))
        return {
            "backend": backend,
            "ok": ok,
            "ingest": ingest,
            "refresh": refresh,
            "retrieve_summary": {
                "context_pack_id": retrieve.get("context_pack_id"),
                "selected_ref_count": len(selected_refs),
                "section_count": len(sections),
                "used_context_tokens": retrieve.get("used_context_tokens"),
                "quality_warnings": retrieve.get("quality_warnings", []),
            },
        }
    finally:
        proc.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backends", nargs="+", default=["local", "cpp", "rust"], choices=["local", "cpp", "rust"])
    parser.add_argument("--run-id", default=str(int(time.time())))
    args = parser.parse_args()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    failures = []
    for backend in args.backends:
        started = time.monotonic()
        try:
            result = run_backend(backend, args.run_id)
            result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 2)
            if not result.get("ok"):
                failures.append(backend)
            results.append(result)
            print(json.dumps(result, indent=2, sort_keys=True))
        except Exception as exc:
            failure = {"backend": backend, "ok": False, "error": str(exc), "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}
            results.append(failure)
            failures.append(backend)
            print(json.dumps(failure, indent=2, sort_keys=True))
    report = {
        "run_id": args.run_id,
        "backends": args.backends,
        "all_ok": not failures,
        "failures": failures,
        "results": results,
    }
    report_json = REPORT_DIR / f"matrixark_mcp_backend_parity_{args.run_id}.json"
    report_md = REPORT_DIR / f"matrixark_mcp_backend_parity_{args.run_id}.md"
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    lines = ["# MatrixArk MCP Backend Parity", "", f"Run ID: `{args.run_id}`", "", f"All OK: `{report['all_ok']}`", ""]
    for item in results:
        lines.extend([
            f"## {item['backend']}",
            "",
            f"- OK: `{item.get('ok')}`",
            f"- Elapsed ms: `{item.get('elapsed_ms')}`",
        ])
        if item.get("error"):
            lines.append(f"- Error: `{item['error']}`")
        else:
            summary = item.get("retrieve_summary", {})
            lines.extend([
                f"- Context pack: `{summary.get('context_pack_id')}`",
                f"- Selected refs: `{summary.get('selected_ref_count')}`",
                f"- Sections: `{summary.get('section_count')}`",
                f"- Used tokens: `{summary.get('used_context_tokens')}`",
            ])
        lines.append("")
    report_md.write_text("\n".join(lines))
    print(f"report_json={report_json}")
    print(f"report_md={report_md}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
