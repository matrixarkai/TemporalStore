#!/usr/bin/env python3
"""MatrixArk MCP scale and failover smoke test."""
from __future__ import annotations

import argparse
import json
import os
import select
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


class McpProcess:
    def __init__(self, command: list[str], env: dict[str, str] | None = None) -> None:
        self.proc = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env={**os.environ, **(env or {})},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._next_id = 1

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)

    def request(self, method: str, params: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]:
        if not self.proc.stdin or not self.proc.stdout:
            raise RuntimeError("MCP process pipes are closed")
        request_id = self._next_id
        self._next_id += 1
        payload: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                stderr = self.proc.stderr.read() if self.proc.stderr else ""
                raise RuntimeError(f"MCP process exited with {self.proc.returncode}: {stderr[-2000:]}")
            ready, _, _ = select.select([self.proc.stdout], [], [], min(0.25, max(0.0, deadline - time.time())))
            if not ready:
                continue
            line = self.proc.stdout.readline()
            if not line:
                continue
            msg = json.loads(line)
            if msg.get("id") == request_id:
                if "error" in msg:
                    raise RuntimeError(f"MCP error for {method}: {msg['error']}")
                return msg.get("result", {})
        raise TimeoutError(f"Timed out waiting for MCP {method}")

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        if not self.proc.stdin:
            raise RuntimeError("MCP process stdin is closed")
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()


def pct(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((percentile / 100.0) * (len(ordered) - 1))))
    return ordered[index]


def latency_summary(samples_ms: list[float]) -> dict[str, float]:
    return {
        "count": float(len(samples_ms)),
        "min_ms": min(samples_ms) if samples_ms else 0.0,
        "avg_ms": statistics.mean(samples_ms) if samples_ms else 0.0,
        "p50_ms": pct(samples_ms, 50),
        "p95_ms": pct(samples_ms, 95),
        "p99_ms": pct(samples_ms, 99),
        "max_ms": max(samples_ms) if samples_ms else 0.0,
    }


def parse_tool_content(result: dict[str, Any]) -> dict[str, Any]:
    content = result.get("content") or []
    if not content:
        return {}
    text = content[0].get("text", "") if isinstance(content[0], dict) else str(content[0])
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"text": text}


def call_tool(mcp: McpProcess, name: str, arguments: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
    result = mcp.request("tools/call", {"name": name, "arguments": arguments}, timeout=timeout)
    return parse_tool_content(result)


def backend_command(args: argparse.Namespace) -> tuple[list[str], dict[str, str]]:
    env = {
        "MATRIXARK_EMBEDDING_PROVIDER": "hash",
        "MATRIXARK_REQUIRE_OSS_EMBEDDINGS": "0",
        "MATRIXARK_UNDERSTANDING_PROVIDER": "rules",
        "MATRIXARK_REQUIRE_OSS_UNDERSTANDING": "0",
        "MATRIXARK_RETRIEVAL_TIMEOUT_MS": "5000",
        "MATRIXARK_MCP_PROFILE": "dev",
    }
    if args.backend == "rust":
        env.update({
            "MATRIXARK_TEMPORALSTORE_RUST_CLI": str(args.rust_cli),
            "MATRIXARK_TEMPORALSTORE_RUST_ROOT": str(args.rust_root),
            "MATRIXARK_TEMPORALSTORE_PREFIX": args.storage_prefix,
                "MATRIXARK_TEMPORALSTORE_METASERVER": "",
                "MATRIXARK_TEMPORALSTORE_NAMESPACE": "matrixark_local",
                "MATRIXARK_TEMPORALSTORE_TABLE": "context_records",
        })
        return [sys.executable, str(ROOT / "tools" / "matrixark_mcp_server.py"), "--backend", "temporalstore-rust", "--rust-cli", str(args.rust_cli), "--storage-prefix", args.storage_prefix, "--line-json"], env
    env["MATRIXARK_LOCAL_EVENT_LOG"] = str(args.local_event_log)
    return [sys.executable, str(ROOT / "tools" / "matrixark_mcp_server.py"), "--backend", "local", "--line-json"], env


def run_mcp_scale(args: argparse.Namespace) -> dict[str, Any]:
    command, env = backend_command(args)
    mcp = McpProcess(command, env)
    ingest_latencies: list[float] = []
    retrieve_latencies: list[float] = []
    errors: list[str] = []
    selected_refs = 0
    try:
        mcp.request("initialize", {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "matrixark-scale-failover", "version": "1"}}, timeout=20)
        mcp.notify("notifications/initialized", {})
        readiness = call_tool(mcp, "matrixark_backend_ready", {}, timeout=30)
        scope = {"account_id": "acct_scale", "tenant_id": "tenant_scale", "user_id": "user_scale", "session_id": "session_scale"}
        for i in range(args.ingest_count):
            started = time.perf_counter()
            try:
                call_tool(mcp, "matrixark_ingest", {"kind": "message", "messages": [{"role": "user", "content": f"Scale event {i}: Alice approved GPU budget item {i % 7} for the inference cluster."}], "scope": scope, "metadata": {"node_path": ["memory", "scale", "gpu"]}, "wait": True}, timeout=30)
            except Exception as exc:
                errors.append(f"ingest[{i}]: {exc}")
            ingest_latencies.append((time.perf_counter() - started) * 1000.0)
        try:
            call_tool(mcp, "matrixark_refresh_summaries", {"scope": scope, "limit": 100}, timeout=60)
        except Exception as exc:
            errors.append(f"refresh_summaries: {exc}")
        for i in range(args.retrieve_count):
            started = time.perf_counter()
            try:
                retrieved = call_tool(mcp, "matrixark_retrieve", {"query": "Which GPU budget approvals are current?", "scope": scope, "max_context_tokens": 2048}, timeout=30)
                selected_refs += len(retrieved.get("selected_refs") or [])
            except Exception as exc:
                errors.append(f"retrieve[{i}]: {exc}")
            retrieve_latencies.append((time.perf_counter() - started) * 1000.0)
        metrics = call_tool(mcp, "matrixark_backend_metrics", {}, timeout=30)
        readiness_ok = readiness.get("ready") is True or readiness.get("status") == "ready"
        service_health_ok = bool((metrics.get("health") or {}).get("ok"))
        return {"ok": (readiness_ok or service_health_ok) and not errors, "backend": args.backend, "readiness": readiness, "errors": errors, "selected_refs_total": selected_refs, "ingest_latency": latency_summary(ingest_latencies), "retrieve_latency": latency_summary(retrieve_latencies), "metrics": metrics}
    finally:
        mcp.close()


def run_rust_gateway_failover(args: argparse.Namespace) -> dict[str, Any]:
    sys.path.insert(0, str(ROOT / "tools"))
    from matrixark_mcp_server import MatrixArkRustCliClient
    previous_root = os.environ.get("MATRIXARK_TEMPORALSTORE_RUST_ROOT")
    os.environ["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = str(args.rust_root)
    client = MatrixArkRustCliClient(
        cli_path=str(args.rust_cli),
        metaserver=os.environ.get("MATRIXARK_TEMPORALSTORE_METASERVER", ""),
        namespace=os.environ.get("MATRIXARK_TEMPORALSTORE_NAMESPACE", "matrixark_local"),
        table=os.environ.get("MATRIXARK_TEMPORALSTORE_TABLE", "context_records"),
        request_timeout_ms=5000,
        io_timeout_ms=5000,
    )
    key = "mcp_failover_probe"
    try:
        ready = client.readiness()
        client.hset(key, "value", "before-restart")
        before_pid = client._proc.pid if client._proc else None
        if client._proc:
            client._proc.kill()
            client._proc.wait(timeout=5)
            client._proc = None
        value_after_restart = client.hget(key, "value")
        after_pid = client._proc.pid if client._proc else None
        client.batch_hset([
            {"key": key, "field": "batch_a", "value": "A"},
            {"key": key, "field": "batch_b", "value": "B"},
        ])
        batch_records = client.batch_hget([
            {"key": key, "field": "value"},
            {"key": key, "field": "batch_a"},
            {"key": key, "field": "batch_b"},
        ])
        batch_values = [record.get("value") for record in batch_records]
        scanned = client.scan_hash(key)
        scan_count = int(scanned.get("count", 0)) if isinstance(scanned, dict) else 0
        metrics = client.metrics_snapshot()
        ready_ok = ready.get("ready") is True or ready.get("ok") is True or ready.get("status") == "ready"
        ok = ready_ok and value_after_restart == "before-restart" and batch_values == ["before-restart", "A", "B"] and scan_count >= 3 and before_pid is not None and after_pid is not None and before_pid != after_pid
        return {"ok": ok, "readiness": ready, "before_pid": before_pid, "after_pid": after_pid, "value_after_restart": value_after_restart, "batch_values": batch_values, "scan_count": scan_count, "metrics": metrics}
    finally:
        client.close()
        if previous_root is None:
            os.environ.pop("MATRIXARK_TEMPORALSTORE_RUST_ROOT", None)
        else:
            os.environ["MATRIXARK_TEMPORALSTORE_RUST_ROOT"] = previous_root


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["local", "rust"], default="rust")
    parser.add_argument("--ingest-count", type=int, default=40)
    parser.add_argument("--retrieve-count", type=int, default=20)
    parser.add_argument("--report-json", type=Path, default=Path("/tmp/matrixark_mcp_scale_failover_report.json"))
    parser.add_argument("--storage-prefix", default=f"matrixark:mcp:scale:{int(time.time())}")
    parser.add_argument("--rust-cli", type=Path, default=ROOT / "target" / "debug" / "matrixark_record_log")
    parser.add_argument("--rust-root", type=Path, default=Path(tempfile.gettempdir()) / "matrixark_mcp_scale_rust")
    parser.add_argument("--local-event-log", type=Path, default=Path(tempfile.gettempdir()) / "matrixark_mcp_scale_local.jsonl")
    args = parser.parse_args()
    report: dict[str, Any] = {"status": "failed", "backend": args.backend, "ingest_count": args.ingest_count, "retrieve_count": args.retrieve_count, "storage_prefix": args.storage_prefix, "started_at_ms": int(time.time() * 1000)}
    report["mcp_scale"] = run_mcp_scale(args)
    if args.backend == "rust":
        report["rust_gateway_failover"] = run_rust_gateway_failover(args)
    checks = [report["mcp_scale"].get("ok")]
    if args.backend == "rust":
        checks.append(report["rust_gateway_failover"].get("ok"))
    report["status"] = "passed" if all(checks) else "failed"
    report["finished_at_ms"] = int(time.time() * 1000)
    args.report_json.parent.mkdir(parents=True, exist_ok=True)
    args.report_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
