#!/usr/bin/env python3
"""Service metric helpers for the MatrixArk MCP server."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json, _mcp_debug_log, now_ms, python_hot_cache_allowed
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json, _mcp_debug_log, now_ms, python_hot_cache_allowed


class MatrixArkServerMetricsMixin:
    """Prometheus and service-gauge helpers for MatrixArkMcpServer."""

    def _backend_storage_mode_from_metrics(self, result: Json) -> str:
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        return str(metrics.get("mode") or result.get("gateway_mode") or metrics.get("audit_mode") or "unknown")

    def _refresh_service_metric_gauges(self) -> None:
        now = now_ms()
        dirty_lag_ms = 0
        import_lag_ms = 0
        try:
            backend_label = str(getattr(self.adapter, "_backend_label", lambda: "local")())
            if python_hot_cache_allowed(backend_label=backend_label):
                records = self.adapter.read_all()
                dirty_times = [
                    int(record.get("updated_at_ms") or record.get("created_at_ms") or now)
                    for record in records
                    if record.get("record_type") == "context_summary_dirty"
                ]
                import_times = [
                    int(record.get("updated_at_ms") or record.get("created_at_ms") or now)
                    for record in records
                    if record.get("record_type") == "resource_import_task" and str(record.get("status") or "") in {"queued", "running"}
                ]
                if dirty_times:
                    dirty_lag_ms = max(0, now - min(dirty_times))
                if import_times:
                    import_lag_ms = max(0, now - min(import_times))
            else:
                _mcp_debug_log("matrixark metrics gauge refresh skipped Python read_all in thin native profile")
        except Exception as exc:
            _mcp_debug_log(f"matrixark metrics gauge refresh failed: {exc}")
        queue_depth = 0
        queue_obj = getattr(self.adapter, "_resource_import_queue", None)
        if queue_obj is not None:
            try:
                queue_depth = int(queue_obj.qsize())
            except Exception:
                queue_depth = 0
        audit_write_failures = int(getattr(self.adapter, "_audit_flush_failures", 0) or 0)
        audit_buffer = getattr(self.adapter, "_audit_buffer", [])
        try:
            audit_queue_depth = len(audit_buffer)
        except Exception:
            audit_queue_depth = 0
        self.metrics.update_gauges(
            dirty_summary_lag_ms=dirty_lag_ms,
            resource_import_lag_ms=import_lag_ms,
            queue_depth=queue_depth,
            audit_write_failures=audit_write_failures,
            audit_queue_depth=audit_queue_depth,
        )

    def _merge_service_prometheus(self, result: Json) -> Json:
        self._refresh_service_metric_gauges()
        raw_backend = str(result.get("backend") or getattr(self.adapter, "_backend_label", lambda: "local")())
        backend = {
            "temporalstore-cpp": "cpp",
            "temporalstore-direct": "cpp",
            "temporalstore-rust": "rust",
        }.get(raw_backend, raw_backend)
        storage_mode = self._backend_storage_mode_from_metrics(result)
        service_prometheus = self.metrics.render_prometheus(backend=backend, storage_mode=storage_mode)
        combined = str(result.get("prometheus") or "")
        if combined and not combined.endswith("\n"):
            combined += "\n"
        result = dict(result)
        result["metrics_format"] = "prometheus"
        result["prometheus"] = combined + service_prometheus
        metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
        result["metrics"] = {**metrics, "service": self.metrics.snapshot()}
        return result
