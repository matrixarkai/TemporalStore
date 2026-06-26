#!/usr/bin/env python3
"""In-process metrics for the MatrixArk MCP service."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import *
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import *

class MatrixArkServiceMetrics:
    """In-process Prometheus metrics for MatrixArk MCP pipeline work."""

    LATENCY_BUCKETS_MS = (25, 50, 100, 250, 500, 1000, 2000, 5000, 10000, float("inf"))

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._started_at = time.time()
        self._ops: dict[str, Json] = {}
        self._model: dict[str, Json] = {}
        self._timeout_count = 0
        self._backpressure_count = 0
        self._partial_context_pack_count = 0
        self._token_pressure_samples: list[float] = []
        self._last_token_pressure = 0.0
        self._last_backend_ready = 0
        self._last_backend_ready_status = "unknown"
        self._last_resource_queue_depth = 0
        self._last_resource_import_lag_ms = 0
        self._last_dirty_summary_lag_ms = 0
        self._last_audit_write_failures = 0

    def observe_operation(self, operation: str, status: str, elapsed_ms: float, *, timeout: bool = False) -> None:
        with self._lock:
            row = self._ops.setdefault(
                operation,
                {"ok": 0, "error": 0, "latencies": [], "buckets": [0 for _ in self.LATENCY_BUCKETS_MS]},
            )
            row["ok" if status == "ok" else "error"] += 1
            samples = row["latencies"]
            samples.append(float(elapsed_ms))
            if len(samples) > 4096:
                del samples[: len(samples) - 4096]
            for index, bucket in enumerate(self.LATENCY_BUCKETS_MS):
                if elapsed_ms <= bucket:
                    row["buckets"][index] += 1
            if timeout:
                self._timeout_count += 1

    def observe_backpressure(self, operation: str) -> None:
        with self._lock:
            self._backpressure_count += 1
            row = self._ops.setdefault(
                operation,
                {"ok": 0, "error": 0, "latencies": [], "buckets": [0 for _ in self.LATENCY_BUCKETS_MS]},
            )
            row["error"] += 1

    def observe_model_latency(self, stage: str, elapsed_ms: float) -> None:
        with self._lock:
            row = self._model.setdefault(stage, {"count": 0, "latencies": [], "buckets": [0 for _ in self.LATENCY_BUCKETS_MS]})
            row["count"] += 1
            samples = row["latencies"]
            samples.append(float(elapsed_ms))
            if len(samples) > 4096:
                del samples[: len(samples) - 4096]
            for index, bucket in enumerate(self.LATENCY_BUCKETS_MS):
                if elapsed_ms <= bucket:
                    row["buckets"][index] += 1

    def observe_retrieve_result(self, result: Json) -> None:
        with self._lock:
            if result.get("partial_context_pack"):
                self._partial_context_pack_count += 1
            budget = int(result.get("remote_context_budget_tokens") or result.get("max_context_tokens") or 0)
            used = int(result.get("used_remote_context_tokens") or result.get("used_context_tokens") or 0)
            pressure = min(1.0, used / budget) if budget > 0 else 0.0
            self._last_token_pressure = pressure
            self._token_pressure_samples.append(pressure)
            if len(self._token_pressure_samples) > 4096:
                del self._token_pressure_samples[: len(self._token_pressure_samples) - 4096]

    def observe_ingest_result(self, result: Json) -> None:
        task = result.get("resource_import_task") if isinstance(result, dict) else {}
        if isinstance(task, dict) and task.get("metrics"):
            metrics = task.get("metrics") or {}
            try:
                self.observe_model_latency("resource_import", float(metrics.get("duration_ms") or 0.0))
            except (TypeError, ValueError):
                pass

    def observe_resource_queue_depth(self, depth: int) -> None:
        with self._lock:
            self._last_resource_queue_depth = max(0, int(depth))

    def observe_backend_ready(self, ready: bool, status: str = "") -> None:
        with self._lock:
            self._last_backend_ready = 1 if ready else 0
            self._last_backend_ready_status = status or ("ready" if ready else "not_ready")

    def update_gauges(self, *, dirty_summary_lag_ms: int, resource_import_lag_ms: int, queue_depth: int, audit_write_failures: int) -> None:
        with self._lock:
            self._last_dirty_summary_lag_ms = max(0, int(dirty_summary_lag_ms))
            self._last_resource_import_lag_ms = max(0, int(resource_import_lag_ms))
            self._last_resource_queue_depth = max(0, int(queue_depth))
            self._last_audit_write_failures = max(0, int(audit_write_failures))

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
        return round(float(ordered[index]), 3)

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def snapshot(self) -> Json:
        with self._lock:
            return {
                "started_at": self._started_at,
                "ops": json.loads(json.dumps(self._ops)),
                "model": json.loads(json.dumps(self._model)),
                "timeout_count": self._timeout_count,
                "backpressure_count": self._backpressure_count,
                "partial_context_pack_count": self._partial_context_pack_count,
                "last_token_pressure": round(self._last_token_pressure, 6),
                "avg_token_pressure": round(sum(self._token_pressure_samples) / len(self._token_pressure_samples), 6)
                if self._token_pressure_samples
                else 0.0,
                "backend_ready": self._last_backend_ready,
                "backend_ready_status": self._last_backend_ready_status,
                "resource_import_queue_depth": self._last_resource_queue_depth,
                "resource_import_lag_ms": self._last_resource_import_lag_ms,
                "dirty_summary_lag_ms": self._last_dirty_summary_lag_ms,
                "audit_write_failures": self._last_audit_write_failures,
            }

    def render_prometheus(self, *, backend: str, storage_mode: str) -> str:
        snap = self.snapshot()
        backend_label = self._escape(backend)
        storage_label = self._escape(storage_mode)
        base_labels = f'backend="{backend_label}",storage_mode="{storage_label}"'
        elapsed_s = max(0.001, time.time() - float(snap["started_at"]))
        lines = [
            "# HELP matrixark_backend_info MatrixArk backend identity and storage mode.",
            "# TYPE matrixark_backend_info gauge",
            f"matrixark_backend_info{{{base_labels}}} 1",
            "# HELP matrixark_backend_ready MatrixArk backend readiness state, 1 for ready and 0 for not ready.",
            "# TYPE matrixark_backend_ready gauge",
            f'matrixark_backend_ready{{{base_labels},status="{self._escape(str(snap["backend_ready_status"]))}"}} {snap["backend_ready"]}',
            "# HELP matrixark_service_requests_total MatrixArk MCP service requests by operation and status.",
            "# TYPE matrixark_service_requests_total counter",
            "# HELP matrixark_service_qps MatrixArk MCP service request QPS by operation.",
            "# TYPE matrixark_service_qps gauge",
            "# HELP matrixark_service_latency_ms MatrixArk MCP service latency quantiles by operation.",
            "# TYPE matrixark_service_latency_ms gauge",
            "# HELP matrixark_service_latency_ms_bucket MatrixArk MCP service latency histogram buckets by operation.",
            "# TYPE matrixark_service_latency_ms_bucket counter",
        ]
        for operation, row in sorted(snap["ops"].items()):
            op_label = self._escape(operation)
            total = int(row.get("ok", 0)) + int(row.get("error", 0))
            for status in ("ok", "error"):
                lines.append(
                    f'matrixark_service_requests_total{{{base_labels},operation="{op_label}",status="{status}"}} {int(row.get(status, 0))}'
                )
            lines.append(f'matrixark_service_qps{{{base_labels},operation="{op_label}"}} {round(total / elapsed_s, 6)}')
            samples = [float(value) for value in row.get("latencies", [])]
            for quantile, percentile in (("0.5", 0.50), ("0.95", 0.95), ("0.99", 0.99)):
                lines.append(
                    f'matrixark_service_latency_ms{{{base_labels},operation="{op_label}",quantile="{quantile}"}} {self._percentile(samples, percentile)}'
                )
            for bucket, count in zip(self.LATENCY_BUCKETS_MS, row.get("buckets", [])):
                le = "+Inf" if bucket == float("inf") else str(int(bucket))
                lines.append(f'matrixark_service_latency_ms_bucket{{{base_labels},operation="{op_label}",le="{le}"}} {int(count)}')

        lines.extend(
            [
                "# HELP matrixark_timeouts_total MatrixArk MCP timeout count.",
                "# TYPE matrixark_timeouts_total counter",
                f"matrixark_timeouts_total{{{base_labels}}} {int(snap['timeout_count'])}",
                "# HELP matrixark_backpressure_rejections_total MatrixArk MCP service backpressure rejection count.",
                "# TYPE matrixark_backpressure_rejections_total counter",
                f"matrixark_backpressure_rejections_total{{{base_labels}}} {int(snap['backpressure_count'])}",
                "# HELP matrixark_partial_context_pack_total MatrixArk partial ContextPack count.",
                "# TYPE matrixark_partial_context_pack_total counter",
                f"matrixark_partial_context_pack_total{{{base_labels}}} {int(snap['partial_context_pack_count'])}",
                "# HELP matrixark_token_pressure_ratio Remote context budget pressure.",
                "# TYPE matrixark_token_pressure_ratio gauge",
                f"matrixark_token_pressure_ratio{{{base_labels},window=\"last\"}} {snap['last_token_pressure']}",
                f"matrixark_token_pressure_ratio{{{base_labels},window=\"avg\"}} {snap['avg_token_pressure']}",
                "# HELP matrixark_dirty_summary_lag_ms Oldest pending dirty summary lag in milliseconds.",
                "# TYPE matrixark_dirty_summary_lag_ms gauge",
                f"matrixark_dirty_summary_lag_ms{{{base_labels}}} {int(snap['dirty_summary_lag_ms'])}",
                "# HELP matrixark_resource_import_lag_ms Oldest queued/running resource import lag in milliseconds.",
                "# TYPE matrixark_resource_import_lag_ms gauge",
                f"matrixark_resource_import_lag_ms{{{base_labels}}} {int(snap['resource_import_lag_ms'])}",
                "# HELP matrixark_resource_import_queue_depth Current MatrixArk resource import queue depth.",
                "# TYPE matrixark_resource_import_queue_depth gauge",
                f"matrixark_resource_import_queue_depth{{{base_labels}}} {int(snap['resource_import_queue_depth'])}",
                "# HELP matrixark_audit_write_failures_total MatrixArk audit write flush failure count.",
                "# TYPE matrixark_audit_write_failures_total counter",
                f"matrixark_audit_write_failures_total{{{base_labels}}} {int(snap['audit_write_failures'])}",
                "# HELP matrixark_model_latency_ms MatrixArk parser/model latency quantiles by stage.",
                "# TYPE matrixark_model_latency_ms gauge",
                "# HELP matrixark_model_latency_ms_bucket MatrixArk parser/model latency buckets by stage.",
                "# TYPE matrixark_model_latency_ms_bucket counter",
            ]
        )
        for stage, row in sorted(snap["model"].items()):
            stage_label = self._escape(stage)
            samples = [float(value) for value in row.get("latencies", [])]
            for quantile, percentile in (("0.5", 0.50), ("0.95", 0.95), ("0.99", 0.99)):
                lines.append(
                    f'matrixark_model_latency_ms{{{base_labels},stage="{stage_label}",quantile="{quantile}"}} {self._percentile(samples, percentile)}'
                )
            for bucket, count in zip(self.LATENCY_BUCKETS_MS, row.get("buckets", [])):
                le = "+Inf" if bucket == float("inf") else str(int(bucket))
                lines.append(f'matrixark_model_latency_ms_bucket{{{base_labels},stage="{stage_label}",le="{le}"}} {int(count)}')
        return "\n".join(lines) + "\n"


