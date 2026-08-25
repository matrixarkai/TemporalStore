# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_AccessPortalMixin methods split from matrixark_access.MatrixArkAccessManager (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403


class _AccessPortalMixin:
    def management_portal(self, args: Json, identity: Json) -> Json:
        scope = optional_object(args, "scope")
        account_id = canonical_account_id(optional_string(args, "account_id") or str(scope.get("account_id") or identity["account_id"]))
        tenant_id = canonical_tenant_id(optional_string(args, "tenant_id") or str(scope.get("tenant_id") or identity["tenant_id"]))
        self.ensure_identity_can_read_scope(identity, account_id, tenant_id, scope)
        effective_scope = enrich_scope_with_identity({**scope, "account_id": account_id, "tenant_id": tenant_id}, identity)
        page_size = args.get("page_size", 10)
        if not isinstance(page_size, int) or page_size <= 0 or page_size > 50:
            raise MatrixArkError("page_size must be an integer between 1 and 50")
        page_token = args.get("page_token", 0)
        if isinstance(page_token, str) and page_token.isdigit():
            page_token = int(page_token)
        if not isinstance(page_token, int) or page_token < 0:
            raise MatrixArkError("page_token must be a non-negative integer offset")

        def page_table_rows(rows: list[Json]) -> Json:
            page = rows[page_token : page_token + page_size]
            next_token = page_token + page_size if page_token + page_size < len(rows) else None
            return {"total": len(rows), "rows": page, "next_page_token": next_token, "page_token": page_token, "page_size": page_size}
        include_revoked = bool(args.get("include_revoked", False))
        records = self.adapter.read_all() + self.metadata.read_all()
        tables = [
            "messages",
            "resources",
            "skills",
            "events",
            "entities",
            "context_packs",
            "summary_refresh",
            "async_pipeline",
        ]
        dashboard = {}
        totals = {}
        for table in tables:
            rows = self.adapter._dashboard_rows_for_table(records, table, effective_scope)
            totals[table] = len(rows)
            dashboard[table] = page_table_rows(rows)
        identity_scopes = set(identity.get("scopes", []))
        is_dev_identity = identity.get("mode") == "dev"
        if is_dev_identity or "admin:account" in identity_scopes:
            account_rows = self.list_accounts({"account_id": account_id, "tenant_id": tenant_id, "limit": 50}, identity)
        else:
            account_rows = {
                "status": "ok",
                "accounts": [{"account_id": account_id, "tenant_id": tenant_id, "account_status": "scoped", "tenant_status": "scoped"}],
                "count": 1,
            }
        if is_dev_identity or "admin:user" in identity_scopes:
            user_rows = self.list_users({"account_id": account_id, "tenant_id": tenant_id, "limit": 50}, identity)
        else:
            user_rows = {
                "status": "ok",
                "account_id": account_id,
                "tenant_id": tenant_id,
                "users": ([{"user_id": effective_scope.get("user_id", ""), "status": "scoped"}] if effective_scope.get("user_id") else []),
                "count": 1 if effective_scope.get("user_id") else 0,
            }
        if is_dev_identity or "admin:api_key" in identity_scopes:
            api_key_rows = self.list_api_keys(
                {"account_id": account_id, "tenant_id": tenant_id, "limit": 50, "include_revoked": include_revoked},
                identity,
            )
        else:
            api_key_rows = {"status": "ok", "account_id": account_id, "tenant_id": tenant_id, "api_keys": [], "count": 0}
        if is_dev_identity or "admin:audit" in identity_scopes:
            audit_rows = self.audit({"account_id": account_id, "tenant_id": tenant_id, "limit": 50}, identity)
        else:
            audit_rows = {"status": "ok", "audit_logs": [], "count": 0}
        dashboard["users"] = page_table_rows(user_rows.get("users", []))
        dashboard["api_keys"] = page_table_rows(api_key_rows.get("api_keys", []))
        dashboard["audit_logs"] = page_table_rows(audit_rows.get("audit_logs", []))
        scoped_records = [record for record in records if scope_matches(self.adapter._dashboard_record_scope(record), effective_scope)]
        nodes = [record for record in scoped_records if record.get("record_type") == "context_node"]
        summaries = [record for record in scoped_records if record.get("record_type") == "context_summary"]
        embeddings = [record for record in scoped_records if record.get("record_type") == "context_embedding"]
        dirty = [record for record in scoped_records if record.get("record_type") == "context_summary_dirty"]
        resource_records = [record for record in scoped_records if str(record.get("record_type", "")).startswith("resource_")]
        skill_records = [record for record in scoped_records if str(record.get("record_type", "")).startswith("skill_")]

        def count_by_node(rows: list[Json]) -> dict[Any, int]:
            counts: dict[Any, int] = {}
            for row in rows:
                node_hash = row.get("node_hash")
                if node_hash is None:
                    continue
                counts[node_hash] = counts.get(node_hash, 0) + 1
            return counts

        summary_counts = count_by_node(summaries)
        embedding_counts = count_by_node(embeddings)
        dirty_counts = count_by_node(dirty)
        resource_counts = count_by_node(resource_records)
        skill_counts = count_by_node(skill_records)
        topology_nodes = sorted(
            [
                {
                    "node_hash": record.get("node_hash", 0),
                    "parent_hash": record.get("parent_hash", 0),
                    "node_name": record.get("node_name", (record.get("node_path") or [""])[-1] if isinstance(record.get("node_path"), list) and record.get("node_path") else ""),
                    "node_path": record.get("node_path", []),
                    "depth": record.get("depth", 0),
                    "updated_at_ms": record.get("updated_at_ms", 0),
                    "summary_count": summary_counts.get(record.get("node_hash"), 0),
                    "embedding_count": embedding_counts.get(record.get("node_hash"), 0),
                    "dirty_summary_count": dirty_counts.get(record.get("node_hash"), 0),
                    "resource_record_count": resource_counts.get(record.get("node_hash"), 0),
                    "skill_record_count": skill_counts.get(record.get("node_hash"), 0),
                }
                for record in nodes
            ],
            key=lambda row: (int(row.get("depth") or 0), str(row.get("node_path"))),
        )[:100]
        topology_records = {
            "context_nodes": page_table_rows(nodes),
            "context_summaries": page_table_rows(summaries),
            "context_embeddings": page_table_rows(embeddings),
            "dirty_summaries": page_table_rows(dirty),
            "resources": page_table_rows(resource_records),
            "skills": page_table_rows(skill_records),
        }
        metrics = {
            "record_count": len(records),
            "scoped_record_count": len(scoped_records),
            "context_node_count": len(nodes),
            "context_summary_count": len(summaries),
            "context_embedding_count": len(embeddings),
            "dirty_summary_count": len(dirty),
            "api_key_count": api_key_rows.get("count", 0),
            "user_count": user_rows.get("count", 0),
            "message_count": totals.get("messages", 0),
            "resource_count": totals.get("resources", 0),
            "skill_count": totals.get("skills", 0),
            "event_count": totals.get("events", 0),
            "entity_count": totals.get("entities", 0),
            "context_pack_count": totals.get("context_packs", 0),
            "metadata_backend": self.metadata.backend_info(),
        }
        try:
            backend_metrics = self.adapter.backend_metrics()
        except Exception as exc:
            backend_metrics = {"backend": "unknown", "health": {"ok": False, "error": str(exc)}, "readiness": {"ok": False, "error": str(exc)}, "metrics": {}}
        backend_identity = {
            "backend": backend_metrics.get("backend", "unknown"),
            "storage_mode": backend_metrics.get("gateway_mode") or backend_metrics.get("production_path") or backend_metrics.get("backend", "unknown"),
            "metrics_format": backend_metrics.get("metrics_format", "prometheus"),
            "readiness_status": (backend_metrics.get("readiness") or {}).get("status") or ("ready" if (backend_metrics.get("readiness") or {}).get("ok") else "unknown"),
            "health_ok": bool((backend_metrics.get("health") or {}).get("ok", True)),
        }
        backend_inner_metrics = backend_metrics.get("metrics") if isinstance(backend_metrics.get("metrics"), dict) else {}
        rust_client_metrics = backend_inner_metrics.get("rust_client") if isinstance(backend_inner_metrics.get("rust_client"), dict) else {}
        context_pack_records = sorted(
            [record for record in scoped_records if record.get("record_type") in {"context_pack_audit", "context_pack_telemetry"}],
            key=lambda row: int(row.get("created_at_ms") or 0),
            reverse=True,
        )
        latest_pack_audit = context_pack_records[0] if context_pack_records else {}
        dropped_refs = latest_pack_audit.get("dropped_refs", {})
        if latest_pack_audit.get("record_type") == "context_pack_telemetry":
            dropped_refs = latest_pack_audit.get("dropped_ref_bucket_counts", {})
        context_pack_debugger = {
            "context_pack_id": latest_pack_audit.get("context_pack_id", ""),
            "query": latest_pack_audit.get("query", "") if latest_pack_audit.get("record_type") == "context_pack_audit" else f"hash:{latest_pack_audit.get('query_hash', '')}",
            "selected_refs": latest_pack_audit.get("selected_refs", []),
            "dropped_refs": dropped_refs,
            "used_context_tokens": latest_pack_audit.get("used_context_tokens", latest_pack_audit.get("used_remote_context_tokens", 0)),
            "used_local_context_tokens": latest_pack_audit.get("used_local_context_tokens", 0),
            "used_remote_context_tokens": latest_pack_audit.get("used_remote_context_tokens", 0),
            "total_prompt_context_tokens": latest_pack_audit.get("total_prompt_context_tokens", 0),
            "remote_context_budget_tokens": latest_pack_audit.get("remote_context_budget_tokens", 0),
            "local_context_policy": latest_pack_audit.get("local_context_policy", {}),
            "quality_warnings": latest_pack_audit.get("quality_warnings", {"count": latest_pack_audit.get("quality_warning_count", 0)}),
            "recall_policy": latest_pack_audit.get("recall_policy", latest_pack_audit.get("recall_policy_summary", {})),
            "replay_link": {
                "tool": "matrixark_replay",
                "arguments": {"context_pack_id": latest_pack_audit.get("context_pack_id", ""), "scope": effective_scope},
            } if latest_pack_audit else {},
        }
        import_tasks = [record for record in scoped_records if record.get("record_type") == "resource_import_task"]
        import_lag_count = len([record for record in import_tasks if record.get("status") not in {"completed", "failed"}])
        audit_write_failures = int(backend_inner_metrics.get("audit_flush_failures") or rust_client_metrics.get("commands_failed_total") or 0)
        retrieve_count = len(context_pack_records)
        used_tokens = sum(int(record.get("used_context_tokens") or record.get("used_remote_context_tokens") or 0) for record in context_pack_records)
        max_tokens = sum(int(record.get("max_context_tokens") or record.get("requested_max_context_tokens") or record.get("remote_context_budget_tokens") or 0) for record in context_pack_records) or max(1, retrieve_count * 10000)
        token_pressure_pct = round(min(100.0, used_tokens * 100.0 / max_tokens), 2)
        fallback_count = sum(
            1
            for record in context_pack_records
            if ((record.get("recall_policy") or {}).get("tree_traversal", {}).get("fallback_to_flat")
                or ((record.get("recall_policy_summary") or {}).get("tree") or {}).get("fallback_to_flat")
                or bool(record.get("tree_fallback_to_flat")))
        )
        fallback_rate_pct = round(fallback_count * 100.0 / max(1, retrieve_count), 2)
        revoked_key_usage = len([row for row in audit_rows.get("audit_logs", []) if row.get("status") == "denied" and "revoked" in json.dumps(row).lower()])
        model_fallback_flags = {
            "embedding_fallback_used": any("hashing" in json.dumps(record).lower() or "fallback" in json.dumps(record).lower() for record in scoped_records if record.get("record_type") in {"context_embedding", "matrixark_metric"}),
            "oss_model_fallback_used": any("oss" in json.dumps(record).lower() and "fallback" in json.dumps(record).lower() for record in scoped_records if record.get("record_type") == "matrixark_metric"),
        }
        observability = {
            "backend_identity": backend_identity,
            "prometheus_source": "matrixark_backend_metrics.prometheus plus MatrixArk record-log counters",
            "prometheus_panels": [
                {"name": "Ingest QPS", "metric": "matrixark_ingest_qps", "native_value": backend_inner_metrics.get("ingest_qps", 0), "rust_value": rust_client_metrics.get("qps", 0), "unit": "ops/s"},
                {"name": "Retrieve QPS", "metric": "matrixark_retrieve_qps", "native_value": backend_inner_metrics.get("retrieve_qps", retrieve_count), "rust_value": rust_client_metrics.get("qps", 0), "unit": "ops/s"},
                {"name": "p50 latency", "metric": "matrixark_request_latency_ms_p50", "native_value": backend_inner_metrics.get("p50_latency_ms", 0), "rust_value": rust_client_metrics.get("p50_latency_ms", 0), "unit": "ms"},
                {"name": "p95 latency", "metric": "matrixark_request_latency_ms_p95", "native_value": backend_inner_metrics.get("p95_latency_ms", 0), "rust_value": rust_client_metrics.get("p95_latency_ms", 0), "unit": "ms"},
                {"name": "p99 latency", "metric": "matrixark_request_latency_ms_p99", "native_value": backend_inner_metrics.get("p99_latency_ms", 0), "rust_value": rust_client_metrics.get("p99_latency_ms", 0), "unit": "ms"},
                {"name": "Errors", "metric": "matrixark_errors_total", "native_value": backend_inner_metrics.get("errors_total", 0), "rust_value": rust_client_metrics.get("commands_failed_total", 0), "unit": "count"},
                {"name": "Timeouts", "metric": "matrixark_timeouts_total", "native_value": backend_inner_metrics.get("timeouts_total", 0), "rust_value": rust_client_metrics.get("timeouts_total", 0), "unit": "count"},
                {"name": "Token pressure", "metric": "matrixark_token_pressure_ratio", "native_value": token_pressure_pct, "rust_value": token_pressure_pct, "unit": "%"},
                {"name": "Dirty summary lag", "metric": "matrixark_dirty_summary_lag_count", "native_value": len(dirty), "rust_value": len(dirty), "unit": "nodes"},
                {"name": "Resource/skill import lag", "metric": "matrixark_resource_skill_import_lag_count", "native_value": import_lag_count, "rust_value": import_lag_count, "unit": "tasks"},
                {"name": "Audit write failures", "metric": "matrixark_audit_write_failures_total", "native_value": audit_write_failures, "rust_value": audit_write_failures, "unit": "count"},
                {"name": "Backend readiness", "metric": "matrixark_backend_ready", "native_value": 1 if backend_identity["readiness_status"] in {"ready", "ok"} else 0, "rust_value": 1 if backend_identity["readiness_status"] in {"ready", "ok"} else 0, "unit": "bool"},
            ],
            "model_fallback_flags": model_fallback_flags,
            "alert_posture": [
                {"name": "unhealthy_backend", "status": "alert" if not backend_identity["health_ok"] else "ok", "detail": backend_identity["backend"]},
                {"name": "topology_not_ready", "status": "alert" if backend_identity["readiness_status"] not in {"ready", "ok"} else "ok", "detail": backend_identity["readiness_status"]},
                {"name": "revoked_key_usage", "status": "alert" if revoked_key_usage else "ok", "detail": revoked_key_usage},
                {"name": "high_token_pressure", "status": "warning" if token_pressure_pct >= 85 else "ok", "detail": f"{token_pressure_pct}%"},
                {"name": "stale_summary_lag", "status": "warning" if len(dirty) else "ok", "detail": len(dirty)},
                {"name": "retrieval_fallback_rate", "status": "warning" if fallback_rate_pct >= 5 else "ok", "detail": f"{fallback_rate_pct}%"},
                {"name": "audit_gaps", "status": "alert" if audit_write_failures else "ok", "detail": audit_write_failures},
            ],
            "backend_metrics": backend_metrics,
        }
        security_governance = {
            "account_id": account_id,
            "tenant_id": tenant_id,
            "effective_user_id": effective_scope.get("user_id", ""),
            "effective_session_id": effective_scope.get("session_id", ""),
            "role": identity.get("role", ""),
            "scopes": sorted(identity.get("scopes", [])),
            "allowed_user_ids": sorted(identity.get("allowed_user_ids", [])),
            "allowed_session_ids": sorted(identity.get("allowed_session_ids", [])),
            "api_key_id": identity.get("api_key_id", ""),
            "api_keys_redacted": True,
            "raw_oauth_tokens_stored": False,
            "access_enforcement": {
                "scope_filter_before_portal_rows": True,
                "account_tenant_active_required_for_portal": True,
                "disabled_user_blocked": bool(effective_scope.get("user_id")),
                "audit_every_portal_read": True,
                "audit_every_context_replay": True,
                "secret_storage": "api_key_hash_only",
            },
            "metadata_backend": self.metadata.backend_info(),
        }
        portal_actions = {
            "register_user": {
                "tool": "matrixark_admin_create_user",
                "arguments": {
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "user_id": effective_scope.get("user_id", "new_user"),
                    "display_name": effective_scope.get("user_id", "New User"),
                    "external_subject": "local:" + str(effective_scope.get("user_id", "new_user")),
                },
            },
            "apply_api_key": {
                "tool": "matrixark_admin_apply_api_key",
                "arguments": {
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "agent_name": effective_scope.get("agent_name", "local_agent"),
                    "user_id": effective_scope.get("user_id", ""),
                    "scopes": ["context:ingest", "context:retrieve", "context:feedback", "context:replay", "resource:read", "skill:read"],
                },
            },
            "sso_login": {
                "tool": "matrixark_auth_sso_login",
                "arguments": {
                    "account_id": account_id,
                    "tenant_id": tenant_id,
                    "provider": "google",
                    "email": str(effective_scope.get("user_id", "user")) + "@example.com",
                    "id_token_verified": False,
                },
            },
            "open_ingestion_history": {
                "tool": "matrixark_ingestion_dashboard",
                "arguments": {"scope": effective_scope, "table": "messages", "page_size": page_size},
            },
        }
        return {
            "status": "ok",
            "account_id": account_id,
            "tenant_id": tenant_id,
            "scope": effective_scope,
            "accounts": account_rows.get("accounts", []),
            "users": user_rows.get("users", []),
            "api_keys": api_key_rows.get("api_keys", []),
            "audit_logs": audit_rows.get("audit_logs", []),
            "dashboard": dashboard,
            "topology": {"nodes": topology_nodes, "count": len(nodes), "records": topology_records},
            "context_pack_debugger": context_pack_debugger,
            "metrics": metrics,
            "observability": observability,
            "security_governance": security_governance,
            "metadata_store": self.metadata.backend_info(),
            "portal_actions": portal_actions,
        }

    def audit(self, args: Json, identity: Json) -> Json:
        # Audit rows have one reading funnel (this method; the portal dashboard calls it too), so
        # this is where buffered audits must become readable. Flushing here instead of on every
        # metadata read keeps the per-request auth lookups (find_active_api_key and friends, which
        # read metadata on every enforced-mode call) from paying the durable append the buffered
        # path exists to move off the request path.
        flusher = getattr(self.adapter, "flush_audits", None)
        if callable(flusher):
            flusher()
        limit = args.get("limit", 100)
        if not isinstance(limit, int) or limit <= 0:
            raise MatrixArkError("limit must be a positive integer")
        account_id = optional_string(args, "account_id", identity["account_id"])
        tenant_id = optional_string(args, "tenant_id", identity["tenant_id"])
        self.ensure_identity_can_manage(identity, account_id, tenant_id)
        rows = [
            record
            for record in reversed(self.metadata.read_all())
            if record.get("record_type") in {"matrixark_audit_log", "matrixark_api_key_usage"}
            and (not account_id or record.get("account_id") == account_id)
            and (not tenant_id or record.get("tenant_id") == tenant_id)
        ][:limit]
        self.append_audit("admin.audit", identity, status="ok", details={"account_id": account_id, "tenant_id": tenant_id, "count": len(rows)})
        return {"status": "ok", "audit_logs": rows, "count": len(rows)}

