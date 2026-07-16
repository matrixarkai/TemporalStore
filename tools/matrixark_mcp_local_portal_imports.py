#!/usr/bin/env python3
"""Portal, registry, and resource-import facade methods for MatrixArk local adapter."""

from __future__ import annotations

try:
    from tools.matrixark_mcp_core import Json
    from tools import matrixark_mcp_dashboard as dashboard_helpers
    from tools import matrixark_mcp_registry as registry_helpers
    from tools import matrixark_mcp_resource_import_runtime as resource_import_runtime
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import Json
    import matrixark_mcp_dashboard as dashboard_helpers
    import matrixark_mcp_registry as registry_helpers
    import matrixark_mcp_resource_import_runtime as resource_import_runtime


class MatrixArkLocalPortalImportMixin:
    """Local adapter wrappers for portal tables, registry APIs, and imports."""

    def latest_skill_controls(self, records: list[Json] | None = None) -> dict[int, Json]:
        return registry_helpers.latest_skill_controls(self, records)

    def _dashboard_record_scope(self, record: Json) -> Json:
        return dashboard_helpers.dashboard_record_scope(record)

    def _dashboard_message_rows(self, records: list[Json], scope: Json) -> list[Json]:
        return dashboard_helpers.dashboard_message_rows(records, scope)

    def _dashboard_rows_for_table(self, records: list[Json], table: str, scope: Json) -> list[Json]:
        return dashboard_helpers.dashboard_rows_for_table(records, table, scope)

    def ingestion_dashboard(self, args: Json) -> Json:
        return dashboard_helpers.ingestion_dashboard(self, args)

    def list_resources(self, args: Json) -> Json:
        return registry_helpers.list_resources(self, args)

    def list_skills(self, args: Json) -> Json:
        return registry_helpers.list_skills(self, args)

    def update_skill(self, args: Json) -> Json:
        return registry_helpers.update_skill(self, args)

    def _resource_import_pool_status(self) -> Json:
        return resource_import_runtime.resource_import_pool_status(self)

    def _ensure_resource_import_workers(self) -> None:
        resource_import_runtime.ensure_resource_import_workers(self)

    def _resource_import_worker_loop(self) -> None:
        resource_import_runtime.resource_import_worker_loop(self)

    def close(self, *, timeout_s: float = 5.0) -> None:
        """Drain async import work and stop background workers."""
        resource_import_runtime.close_resource_import_runtime(self, timeout_s=timeout_s)

    def _enqueue_resource_import(self, *, args: Json, hook: Json | None, task_hash: int) -> Json:
        return resource_import_runtime.enqueue_resource_import(self, args=args, hook=hook, task_hash=task_hash)

    def _run_background_resource_import(self, args: Json, hook: Json | None) -> None:
        resource_import_runtime.run_background_resource_import(self, args, hook)

    def _resource_import_async_default_reason(self, args: Json, envelope: Json, raw_uri: str) -> str:
        return resource_import_runtime.resource_import_async_default_reason(args, envelope, raw_uri)
