#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI

from __future__ import annotations

import functools
import json
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import threading
import textwrap
import tempfile
import unittest
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
UI_DIR = ROOT / "tools" / "temporalstore-monitoring-ui"

# The eight test_context_app_js_* cases below run app.js under node. app.js is written for the
# browsers the portal targets and uses syntax added after node 12 (nullish coalescing, optional
# chaining), so an old local node fails to PARSE it and every one of those tests dies on a
# CalledProcessError that says nothing about the behaviour under test. CI runs a modern node and
# they pass there, so skipping locally costs no coverage -- but reporting eight failures that are
# really one missing toolchain costs a reader real time, and did.
APP_JS_TESTS_PREFIX = "test_context_app_js_"


@functools.lru_cache(maxsize=1)
def node_cannot_parse_app_js() -> str:
    """Empty when the available node can parse app.js; otherwise why it cannot."""
    app_js = UI_DIR / "app.js"
    if not app_js.exists():
        return ""
    try:
        check = subprocess.run(["node", "--check", str(app_js)], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        return "node is not usable here (%s), so app.js cannot be exercised" % type(exc).__name__
    if check.returncode == 0:
        return ""
    try:
        found = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        found = "unknown"
    first_error = next((line.strip() for line in (check.stderr or "").splitlines()
                        if "Error" in line), "").strip()
    return (
        "node %s cannot parse %s -- %s. app.js targets browsers and uses syntax added after "
        "node 12; node 14 or newer is required to exercise it. CI runs a modern node and these "
        "tests pass there. This skips the %s* cases only." % (
            found or "unknown", app_js.name, first_error or "it failed --check", APP_JS_TESTS_PREFIX)
    )


class IdCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.classes: set[str] = set()
        self.headings: list[str] = []
        self._capture_heading = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        if attrs_dict.get("id"):
            self.ids.add(attrs_dict["id"] or "")
        if attrs_dict.get("class"):
            self.classes.update((attrs_dict["class"] or "").split())
        if tag in {"h1", "h2", "h3"}:
            self._capture_heading = True

    def handle_endtag(self, tag: str) -> None:
        if tag in {"h1", "h2", "h3"}:
            self._capture_heading = False

    def handle_data(self, data: str) -> None:
        if self._capture_heading and data.strip():
            self.headings.append(data.strip())


class MonitoringUiContextOpsTest(unittest.TestCase):
    def setUp(self) -> None:
        # Scoped to the app.js cases by name: a module-level skip would take the whole file with
        # it, and the other six here read the HTML and never invoke node.
        if self._testMethodName.startswith(APP_JS_TESTS_PREFIX):
            reason = node_cannot_parse_app_js()
            if reason:
                self.skipTest(reason)

    def test_context_operations_markup_contract(self) -> None:
        parser = IdCollector()
        parser.feed((UI_DIR / "index.html").read_text(encoding="utf-8"))

        required_ids = {
            "health-source-banner",
            "context-runtime-status",
            "resource-skill-status",
            "resource-skill-metrics",
            "resource-import-tasks",
            "resource-parse-warnings",
            "resource-tree-view",
            "resource-chunk-preview",
            "skill-registry-view",
            "resource-version-history",
            "resource-summary-lag",
            "resource-retrieval-replay",
            "context-ops-workspace",
            "context-data-plane",
            "context-kpis",
            "context-flow",
            "context-tests",
            "context-pipeline-body",
            "context-e2e-parity-body",
            "context-request-builder",
            "context-runbook",
            "context-query-workbench",
            "context-config",
            "context-model-registry",
            "context-tree",
            "context-pack",
            "context-filesystem-explorer",
            "context-observation",
            "context-alerts",
            "context-audit",
            "context-operators",
            "context-safeguards",
            "context-ui-readiness-summary",
            "context-ui-readiness",
        }
        self.assertTrue(required_ids.issubset(parser.ids))
        self.assertIn("LLM Context Operations", parser.headings)
        self.assertIn("Resource And Skill Operations", parser.headings)
        self.assertIn("Import Tasks", parser.headings)
        self.assertIn("Parse Warnings", parser.headings)
        self.assertIn("Resource Tree", parser.headings)
        self.assertIn("Chunk Preview", parser.headings)
        self.assertIn("Skill Registry", parser.headings)
        self.assertIn("Version History", parser.headings)
        self.assertIn("Dirty Summary Lag", parser.headings)
        self.assertIn("Retrieval Replay", parser.headings)
        self.assertIn("Operations Workspace", parser.headings)
        self.assertIn("Context Data Plane", parser.headings)
        self.assertIn("Pipeline Test Console", parser.headings)
        self.assertIn("End-to-End Parity", parser.headings)
        self.assertIn("Query Workbench", parser.headings)
        self.assertIn("Context Config", parser.headings)
        self.assertIn("Open Source Model Registry", parser.headings)
        self.assertIn("Context Tree And Retrieval Pack", parser.headings)
        self.assertIn("Operator Console", parser.headings)
        self.assertIn("Safeguards", parser.headings)
        self.assertIn("UI Production Readiness", parser.headings)
        self.assertIn("Replay Audit", parser.headings)
        self.assertIn("context-layout", parser.classes)
        self.assertIn("ingestion-dashboard.html", (UI_DIR / "index.html").read_text(encoding="utf-8"))
        self.assertIn("management-portal.html", (UI_DIR / "index.html").read_text(encoding="utf-8"))

    def test_ingestion_dashboard_markup_contract(self) -> None:
        parser = IdCollector()
        parser.feed((UI_DIR / "ingestion-dashboard.html").read_text(encoding="utf-8"))
        required_ids = {
            "dashboard-status",
            "scope-account",
            "scope-tenant",
            "scope-user",
            "scope-session",
            "scope-agent",
            "dashboard-table",
            "page-size",
            "load-sample",
            "copy-request",
            "total-messages",
            "total-resources",
            "total-events-entities",
            "total-packs",
            "dashboard-head",
            "dashboard-body",
            "row-details",
            "request-preview",
        }
        self.assertTrue(required_ids.issubset(parser.ids))
        self.assertIn("Ingestion Dashboard", parser.headings)
        self.assertIn("Scoped Context Inventory", parser.headings)
        html = (UI_DIR / "ingestion-dashboard.html").read_text(encoding="utf-8")
        self.assertIn("matrixark_ingestion_dashboard", html)
        self.assertIn("Context Packs", html)
        self.assertIn("Resources", html)
        self.assertIn("Summary Refresh", html)
        self.assertIn("Async Pipeline", html)
        self.assertIn("summary_refresh", html)
        self.assertIn("async_pipeline", html)


    def test_management_portal_markup_contract(self) -> None:
        html = (UI_DIR / "management-portal.html").read_text(encoding="utf-8")
        parser = IdCollector()
        parser.feed(html)

        required_ids = {
            "portal-status",
            "portal-account",
            "portal-tenant",
            "portal-user",
            "portal-session",
            "portal-agent",
            "portal-provider",
            "portal-email",
            "portal-mode",
            "portal-page-size",
            "portal-refresh",
            "portal-prev-page",
            "portal-next-page",
            "portal-copy",
            "portal-users",
            "portal-items",
            "portal-nodes",
            "portal-keys",
            "portal-register-payload",
            "portal-key-payload",
            "portal-sso-payload",
            "portal-link-payload",
            "portal-key-management-payload",
            "portal-policy-decision",
            "portal-request",
            "portal-install-command",
            "portal-mcp-config",
            "portal-verify-command",
            "portal-identity-policy",
            "portal-token-metrics",
            "portal-limit-policy",
            "portal-table-pager",
            "portal-topology-records",
            "portal-contextpack-debugger",
            "portal-replay-links",
            "portal-backend-identity",
            "portal-prometheus-panels",
            "portal-alert-posture",
            "portal-model-fallbacks",

            "portal-metadata-backend",
            "portal-metadata-env",
            "portal-metadata-schema",
            "portal-metadata-policy",
            "portal-metadata-active",
            "portal-table-head",
            "portal-table-body",
            "portal-row-details",
            "portal-topology",
            "portal-metrics",
        }
        self.assertTrue(required_ids.issubset(parser.ids))
        for heading in [
            "Management Portal",
            "User Backend Portal",
            "Quick Start",
            "Registration, SSO, And API Keys",
            "Token And Usage Monitoring",
            "Security And Governance",
            "Roles And Service Keys",
            "Prometheus Observability",
            "Alert Posture",
            "Model Fallback Flags",
            "Metadata Store",
            "Active Metadata Backend",
            "Agent Install Snippets",
            "Identity Resolution",
            "Ingestion And Access History",
            "Context Topology",
            "Topology Backing Records",
            "ContextPack Audit Debugger",
            "Metrics And Audit",
        ]:
            self.assertIn(heading, parser.headings)

        self.assertIn("matrixark_management_portal", html)
        self.assertIn("Summary Refresh", html)
        self.assertIn("Async Pipeline", html)
        self.assertIn("summary_refresh", html)
        self.assertIn("async_pipeline", html)
        self.assertIn("matrixark_admin_apply_api_key", html)
        self.assertIn("matrixark_auth_signup", html)
        self.assertIn("matrixark_auth_sso_callback", html)
        self.assertIn("matrixark_admin_map_sso_user", html)
        self.assertIn("matrixark_admin_rotate_api_key", html)
        self.assertIn("matrixark_admin_revoke_api_key", html)
        self.assertIn("Google / Gmail", html)
        self.assertIn("GitHub", html)
        self.assertIn("Used context tokens", html)
        self.assertIn("Live paged table", html)
        self.assertIn("ContextPack Audit Debugger", html)
        self.assertIn("context_pack_debugger", html)
        self.assertIn("selected_refs", html)
        self.assertIn("dropped_refs", html)
        self.assertIn("used_local_context_tokens", html)
        self.assertIn("used_remote_context_tokens", html)
        self.assertIn("replay_link", html)
        self.assertIn("context_summaries", html)
        self.assertIn("context_embeddings", html)
        self.assertIn("dirty_summaries", html)
        self.assertIn("matrixark_backend_metrics", html)
        self.assertIn("matrixark_ingest_qps", html)
        self.assertIn("matrixark_retrieve_qps", html)
        self.assertIn("matrixark_request_latency_ms_p95", html)
        self.assertIn("matrixark_audit_write_failures_total", html)
        self.assertIn("topology_not_ready", html)
        self.assertIn("mcpServers", html)
        self.assertIn("MATRIXARK_METADATA_BACKEND", html)
        self.assertIn("matrixkv_sql", html)
        self.assertIn("matrixkv_sql", html)
        self.assertIn("matrixkv+mysql://matrixark", html)
        self.assertIn("mysql://matrixark", html)
        self.assertIn("matrixark_metadata_records", html)
        self.assertIn("last_used_at_ms", html)
        self.assertIn("usage_count", html)
        self.assertIn("allowed_session_ids", html)
        self.assertIn("expires_at_ms", html)
        self.assertIn("redacted", html)
        self.assertNotIn("const samplePortal", html)
        self.assertIn("portal-next-page", html)
        self.assertIn("portal-prev-page", html)
        self.assertIn("Refresh Live Portal", html)
        self.assertIn("offline sample", html)
        self.assertIn("/api/tools/call", html)
        self.assertIn("/api/backend_metrics", html)
        self.assertIn("/api/ingestion_dashboard", html)
        self.assertIn("/api/management_portal", html)
        self.assertIn("portalAutoRefresh", html)
        self.assertIn("loadLivePortal", html)
        self.assertIn("portalState", html)
        self.assertIn("fallbackPortal", html)


    def test_context_sample_health_has_operable_pipeline(self) -> None:
        health = json.loads((UI_DIR / "health.json").read_text(encoding="utf-8"))
        context = health["context_ops"]

        self.assertEqual("ready", context["status"])
        self.assertGreaterEqual(len(context["kpis"]), 4)
        self.assertEqual(
            ["Raw input", "Extraction", "Ingestion", "Retrieval", "Replay"],
            [step["name"] for step in context["flow"]],
        )
        self.assertEqual(
            [
                "Context Nodes",
                "Events",
                "Extractions",
                "Ingestions",
                "Resources",
                "Feedback",
                "Summaries",
                "Context Packs",
            ],
            [lane["label"] for lane in context["data_plane"]],
        )
        self.assertTrue(all(lane.get("value") for lane in context["data_plane"]))
        self.assertTrue(all(lane.get("detail") for lane in context["data_plane"]))
        self.assertTrue(all(lane.get("evidence") for lane in context["data_plane"]))
        self.assertTrue(all(lane.get("status") in {"ready", "passed"} for lane in context["data_plane"]))
        self.assertTrue(any(lane["evidence"] == "ContextEvent" for lane in context["data_plane"]))
        self.assertTrue(any(lane["evidence"] == "ContextCompressionEvent" for lane in context["data_plane"]))
        self.assertTrue(any(lane["evidence"] == "ContextPackAudit" for lane in context["data_plane"]))
        self.assertTrue(any(row["step"] == "retrieve_with_resources" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "budgeted_pack" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "parse_layered_resource" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "batch_ingest_x8" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "api_stream_batch_ingest" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "time_compression" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "parity_gates_x8" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "parity_gates_x9" for row in context["pipeline"]))
        self.assertTrue(any(row["step"] == "model_config_parity_x10" for row in context["pipeline"]))
        self.assertEqual(10, len(context["e2e_parity_runs"]))
        self.assertTrue(all(row["status"] == "passed" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["run"] == "API idempotency" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["run"] == "Stream replay checkpoint" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["evidence"] == "context_nine_ingestion_compression_parity_gates" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["run"] == "Compression source audit" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["run"] == "Model/config parity" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["evidence"] == "context_ten_model_config_parity_gates" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(row["run"] == "module parity" for row in context["e2e_parity_runs"]))
        self.assertTrue(any(alert["label"] == "Token budget" for alert in context["alerts"]))
        self.assertTrue(any(row["label"] == "Feedback memory" for row in context["audit"]))
        resource_skill = context["resource_skill_ops"]
        self.assertEqual("ready", resource_skill["status"])
        metric_labels = {metric["label"] for metric in resource_skill["metrics"]}
        self.assertTrue(
            {
                "Import duration",
                "Parser duration by type",
                "Chunk count",
                "Dedupe count",
                "Embedding duration",
                "Extraction duration",
                "Summary dirty lag",
                "Resource retrieval hit rate",
                "Skill retrieval hit rate",
                "Parse failure count",
            }.issubset(metric_labels)
        )
        self.assertTrue(any(metric["value"] == "92%" for metric in resource_skill["metrics"]))
        self.assertTrue(any(metric["label"] == "Parse failure count" and metric["value"] == "0" for metric in resource_skill["metrics"]))
        self.assertTrue(any(task["status"] == "completed" for task in resource_skill["import_tasks"]))
        self.assertTrue(any(warning["value"] == "warning" for warning in resource_skill["parse_warnings"]))
        self.assertTrue(any(row["path"] == "/resources/runbooks/gpu.md" for row in resource_skill["resource_tree"]))
        self.assertTrue(any(chunk["chunk_hash"] == "7101" and chunk["selected"] == "yes" for chunk in resource_skill["chunk_preview"]))
        self.assertTrue(any(skill["skill"] == "context-debugger" for skill in resource_skill["skill_registry"]))
        self.assertTrue(any(version["supersedes"] == "v2" for version in resource_skill["version_history"]))
        self.assertTrue(any(lag["worker"] == "refresh queued" for lag in resource_skill["summary_lag"]))
        self.assertTrue(any(replay["audit"] == "ContextPackAudit" for replay in resource_skill["retrieval_replay"]))
        self.assertEqual("77027771", context["query_workbench"]["query_id"])
        self.assertTrue(any(group["group"] == "Traversal" for group in context["config"]))
        self.assertTrue(any(row["role"] == "Reranker" for row in context["model_registry"]))
        self.assertTrue(any(row["role"] == "PDF/VLM parser" for row in context["model_registry"]))
        self.assertEqual("1001000", context["topology"]["selected_path"][0])
        self.assertTrue(any(node["label"] == "approvals" for node in context["topology"]["nodes"]))
        self.assertTrue(
            any(
                node["label"] == "gpu_purchase_request_8891"
                and node.get("parent") == "1001300"
                for node in context["topology"]["nodes"]
            )
        )
        self.assertTrue(any(node["status"] == "deduped child ref" for node in context["topology"]["nodes"]))
        self.assertTrue(any(node["metadata"].get("object_key") == "ctx:node:1001:1001300" for node in context["topology"]["nodes"]))
        self.assertTrue(any(node["metadata"].get("raw_uri") == "incident_77.pdf" for node in context["topology"]["nodes"]))
        self.assertTrue(any(row["path"] == "/company_a/infra_team/project_1/approvals" for row in context["filesystem"]))
        self.assertTrue(any(row["model"] == "ContextNode + ContextEvent + ContextEntity" for row in context["filesystem"]))
        self.assertTrue(any(row["storage"] == "ctx:event, ctx:entity, ctxidx" for row in context["filesystem"]))
        self.assertTrue(any(row["label"] == "Index health" and row["status"] == "passed" for row in context["observations"]))
        self.assertTrue(any(row["label"] == "Summary lag" and row["status"] == "watch" for row in context["observations"]))
        self.assertTrue(any(row["command"] == "context_retrieve_with_resources" for row in context["operators"]))
        self.assertTrue(any(row["label"] == "Serving threshold" for row in context["safeguards"]))
        self.assertEqual(8, len(context["readiness_gates"]))
        self.assertTrue(all(gate["value"] == "pass" for gate in context["readiness_gates"]))
        self.assertTrue(all(gate.get("owner") for gate in context["readiness_gates"]))
        self.assertTrue(all(gate.get("evidence") for gate in context["readiness_gates"]))
        self.assertTrue(all(gate.get("severity") in {"blocker", "high", "medium"} for gate in context["readiness_gates"]))
        self.assertTrue(any(gate["label"] == "Contract parity" for gate in context["readiness_gates"]))
        self.assertTrue(any(gate["label"] == "Idempotent writes" for gate in context["readiness_gates"]))
        self.assertTrue(any(gate["evidence"] == "context_nine_ingestion_compression_parity_gates" for gate in context["readiness_gates"]))
        self.assertEqual(9, len(context["ui_readiness_gates"]))
        self.assertTrue(all(gate["value"] == "pass" for gate in context["ui_readiness_gates"]))
        self.assertTrue(all(gate.get("owner") for gate in context["ui_readiness_gates"]))
        self.assertTrue(all(gate.get("evidence") for gate in context["ui_readiness_gates"]))
        self.assertTrue(any(gate["label"] == "Overflow guard" for gate in context["ui_readiness_gates"]))
        self.assertTrue(any(gate["label"] == "Actionable runbook" for gate in context["ui_readiness_gates"]))
        self.assertTrue(any(gate["label"] == "Ten-lane parity" for gate in context["ui_readiness_gates"]))

    def test_context_renderer_and_styles_are_wired(self) -> None:
        app_js = (UI_DIR / "app.js").read_text(encoding="utf-8")
        styles = (UI_DIR / "styles.css").read_text(encoding="utf-8")

        for token in [
            "function renderContextOps",
            "renderContextOps(data.context_ops)",
            "function renderOpsWorkspace",
            "renderOpsWorkspace(data)",
            "function renderResourceSkillOps",
            "function renderResourceSkillMetrics",
            "defaultResourceSkillOps",
            "resource_skill_ops",
            "resource-skill-metrics",
            "Resource retrieval hit rate",
            "Skill retrieval hit rate",
            "Parse failure count",
            "resource-import-tasks",
            "resource-parse-warnings",
            "resource-tree-view",
            "resource-chunk-preview",
            "skill-registry-view",
            "resource-version-history",
            "resource-summary-lag",
            "resource-retrieval-replay",
            "renderCardStack",
            "renderCompactRecords",
            "renderMetadataRow",
            "ContextPackAudit",
            "latest-version filter",

            "function renderDataPlane",
            "renderDataPlane(data)",
            "function countMatches",
            "data_plane",
            "Context Nodes",
            "Context Packs",
            "ContextEvent",
            "ContextCompressionEvent",
            "renderScaleTestsInto(\"context-tests\"",
            "context-e2e-parity-body",
            "e2e_parity_runs",
            "renderPackGroup(\"Events\"",
            "function renderQueryWorkbench",
            "function renderContextConfig",
            "function renderModelRegistry",
            "function renderRuntimeConfigControl",
            "function buildRuntimeConfigDraft",
            "function renderModelConfigControls",
            "function splitModelOptions",
            "function buildModelConfigDraft",
            "function renderContextOperators",
            "function renderMetadataRows",
            "function renderSafeguardHtml",
            "Metadata Details",
            "Draft Runtime Config",
            "Draft Model Config",
            "data-model-role",
            "OpenAI compatible",
            "agent supplied",
            "topology-detail-grid",
            "topology-selected",
            "topology-meta-row",
            "renderFilesystemExplorer",
            "renderContextObservation",
            "Filesystem-Like Explorer",
            "context-filesystem-explorer",
            "context-observation",
            "filesystem-table",
            "observation-card",
            "Node traversal",
            "Index health",
            "readiness_gates",
            "ui_readiness_gates",
            "ui-readiness-card",
            "function renderUiReadinessSummary",
            "function renderHealthSource",
            "function markHealthStale",
            "function setRefreshBusy",
            "function fetchHealthJson",
            "function validateHealthPayload",
            "function saveCachedHealth",
            "function loadCachedHealth",
            "function autoRefreshHealth",
            "function handleVisibilityChange",
            "refreshTimeoutMs",
            "refreshIntervalMs",
            "refreshInFlight",
            "healthCacheKey",
            "healthCacheMaxAgeMs",
            "withHealthSource",
            "lastGoodHealth",
            "health-source-banner",
            "context-ui-readiness-summary",
            "Production posture",
            "Fallback sample data",
            "Stale live health data",
            "Cached health data",
            "expires after",
            "paused while hidden",
            "refresh timeout after",
            "invalid health payload",
            "Gate: ",
            "UI Gate: ",
            "context_ops",
        ]:
            self.assertIn(token, app_js)

        for selector in [
            ".context-kpis",
            ".flow-grid",
            ".ops-workspace-grid",
            ".ops-workspace-card",
            ".data-plane-grid",
            ".data-plane-card",
            ".request-card",
            ".query-workbench",
            ".config-card",
            ".config-control-grid",
            ".config-control",
            ".config-card-wide",
            ".model-registry-grid",
            ".registry-card",
            ".registry-card-wide",
            ".model-config-controls",
            ".model-toggle",
            ".operator-grid",
            ".health-source-banner",
            ".ui-readiness-summary",
            ".ui-readiness-summary-grid",
            ".ui-readiness-grid",
            ".ui-readiness-card",
            ".resource-skill-metrics",
            ".portal-scope-grid",
            ".portal-action-grid",
            ".portal-tabs",
            ".portal-topology-tree",
            ".portal-node",
            ".resource-metric-card",
            ".tree-node",
            ".topology-map",
            ".topology-node",
            ".topology-state",
            ".topology-flags",
            ".topology-detail-grid",
            ".topology-selected",
            ".topology-metadata",
            ".topology-metadata[open]",
            ".topology-meta-row",
            ".context-observability",
            ".filesystem-explorer",
            ".filesystem-head",
            ".filesystem-table",
            ".observation-grid",
            ".observation-card",
            ".pack-columns",
            "@media (max-width: 720px)",
        ]:
            self.assertIn(selector, styles)

    def test_context_ui_serves_end_to_end_over_http(self) -> None:
        class Handler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(UI_DIR), **kwargs)

            def log_message(self, format: str, *args) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            index = fetch_text(f"{base_url}/")
            app_js = fetch_text(f"{base_url}/app.js")
            styles = fetch_text(f"{base_url}/styles.css")
            health = json.loads(fetch_text(f"{base_url}/health.json"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        parser = IdCollector()
        parser.feed(index)
        self.assertIn("context-tests", parser.ids)
        self.assertIn("context-ops-workspace", parser.ids)
        self.assertIn("context-data-plane", parser.ids)
        self.assertIn("context-e2e-parity-body", parser.ids)
        self.assertIn("context-query-workbench", parser.ids)
        self.assertIn("context-config", parser.ids)
        self.assertIn("context-model-registry", parser.ids)
        self.assertIn("context-pack", parser.ids)
        self.assertIn("context-filesystem-explorer", parser.ids)
        self.assertIn("context-observation", parser.ids)
        self.assertIn("context-operators", parser.ids)
        self.assertIn("health-source-banner", parser.ids)
        self.assertIn("context-ui-readiness-summary", parser.ids)
        self.assertIn("context-ui-readiness", parser.ids)
        self.assertIn("resource-skill-metrics", parser.ids)
        self.assertIn("renderContextOps", app_js)
        self.assertIn("renderResourceSkillMetrics", app_js)
        self.assertIn("function fetchHealthJson", app_js)
        self.assertIn("Promise.race([request, timeout])", app_js)
        self.assertIn("controller ? { signal: controller.signal }", app_js)
        self.assertIn(".context-tree-pack", styles)
        self.assertIn(".ops-workspace-card", styles)
        self.assertIn(".data-plane-card", styles)
        self.assertIn(".resource-metric-card", styles)
        self.assertIn(".topology-node", styles)
        self.assertIn(".topology-metadata", styles)
        self.assertIn(".filesystem-explorer", styles)
        self.assertIn(".observation-card", styles)
        self.assertIn(".query-workbench", styles)
        self.assertIn(".model-config-controls", styles)
        self.assertIn(".health-source-banner", styles)
        self.assertIn(".ui-readiness-summary", styles)
        self.assertIn(".ui-readiness-card", styles)
        self.assertEqual("ready", health["context_ops"]["status"])
        self.assertTrue(
            any(row["step"] == "retrieve_with_resources" for row in health["context_ops"]["pipeline"])
        )
        self.assertEqual("77027771", health["context_ops"]["query_workbench"]["query_id"])
        self.assertTrue(
            any(test["name"] == "Layered resource parsing" for test in health["context_ops"]["tests"])
        )
        self.assertTrue(
            any(model["default_model"] == "BAAI/bge-reranker-base" for model in health["context_ops"]["model_registry"])
        )

    def test_context_app_js_renders_real_health_payload(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.fetch = async (url) => {{
              if (!String(url).startsWith("/health.json")) {{
                throw new Error(`unexpected fetch URL: ${{url}}`);
              }}
              return {{
                ok: true,
                json: async () => JSON.parse(fs.readFileSync(`${{uiDir}}/health.json`, "utf8")),
              }};
            }};
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(() => {{
              const required = {{
                contextKpis: elements["context-kpis"].innerHTML,
                opsWorkspace: elements["context-ops-workspace"].innerHTML,
                dataPlane: elements["context-data-plane"].innerHTML,
                contextFlow: elements["context-flow"].innerHTML,
                contextTests: elements["context-tests"].innerHTML,
                contextPipeline: elements["context-pipeline-body"].innerHTML,
                e2eParity: elements["context-e2e-parity-body"].innerHTML,
                requestBuilder: elements["context-request-builder"].innerHTML,
                queryWorkbench: elements["context-query-workbench"].innerHTML,
                contextConfig: elements["context-config"].innerHTML,
                modelRegistry: elements["context-model-registry"].innerHTML,
                resourceMetrics: elements["resource-skill-metrics"].innerHTML,
                contextTree: elements["context-tree"].innerHTML,
                contextPack: elements["context-pack"].innerHTML,
                filesystemExplorer: elements["context-filesystem-explorer"].innerHTML,
                observation: elements["context-observation"].innerHTML,
                alerts: elements["context-alerts"].innerHTML,
                audit: elements["context-audit"].innerHTML,
                operators: elements["context-operators"].innerHTML,
                safeguards: elements["context-safeguards"].innerHTML,
                healthSource: elements["health-source-banner"].innerHTML,
                uiReadinessSummary: elements["context-ui-readiness-summary"].innerHTML,
                uiReadiness: elements["context-ui-readiness"].innerHTML,
              }};
              const checks = [
                required.contextKpis.includes("TemporalStore"),
                required.opsWorkspace.includes("Operations") && required.opsWorkspace.includes("#operations"),
                required.opsWorkspace.includes("Configurations") && required.opsWorkspace.includes("#configuration"),
                required.opsWorkspace.includes("Testing") && required.opsWorkspace.includes("#testing"),
                required.opsWorkspace.includes("Evidence") && required.opsWorkspace.includes("#evidence"),
                required.opsWorkspace.includes("10/10 e2e passed"),
                required.dataPlane.includes("Context Nodes") && required.dataPlane.includes("ContextNode + ChildRef"),
                required.dataPlane.includes("7 visible"),
                required.dataPlane.includes("Events") && required.dataPlane.includes("ContextEvent"),
                required.dataPlane.includes("4 writes"),
                required.dataPlane.includes("Extractions") && required.dataPlane.includes("optional LLM"),
                required.dataPlane.includes("2 lanes"),
                required.dataPlane.includes("Ingestions") && required.dataPlane.includes("idempotency"),
                required.dataPlane.includes("6 paths"),
                required.dataPlane.includes("Resources") && required.dataPlane.includes("ResourceChunk"),
                required.dataPlane.includes("3 lanes"),
                required.dataPlane.includes("Feedback") && required.dataPlane.includes("future retrievable memory"),
                required.dataPlane.includes("Summaries") && required.dataPlane.includes("ContextCompressionEvent"),
                required.dataPlane.includes("Context Packs") && required.dataPlane.includes("ContextPackAudit"),
                required.dataPlane.includes("60 / 70"),
                required.contextFlow.includes("Extraction") && required.contextFlow.includes("Retrieval"),
                required.contextTests.includes("Layered resource parsing"),
                required.contextPipeline.includes("parse_layered_resource"),
                required.contextPipeline.includes("batch_ingest_x8"),
                required.contextPipeline.includes("api_stream_batch_ingest"),
                required.contextPipeline.includes("time_compression"),
                required.contextPipeline.includes("parity_gates_x8"),
                required.contextPipeline.includes("parity_gates_x9"),
                required.contextPipeline.includes("model_config_parity_x10"),
                required.e2eParity.includes("API idempotency") && required.e2eParity.includes("duplicate 996088 absent"),
                required.e2eParity.includes("Stream replay checkpoint") && required.e2eParity.includes("offset 13 produced 996007"),
                required.e2eParity.includes("Compression source audit") && required.e2eParity.includes("context_nine_ingestion_compression_parity_gates"),
                required.e2eParity.includes("Model/config parity") && required.e2eParity.includes("context_ten_model_config_parity_gates"),
                required.e2eParity.includes("query embedding, reranker, provider, top-k, token budget"),
                required.e2eParity.includes("module parity") && required.e2eParity.includes("7 context module tests passed"),
                required.requestBuilder.includes("/v1/context/retrieve_with_resources"),
                required.requestBuilder.includes("/v1/context/batch_ingest"),
                required.requestBuilder.includes("/v1/context/stream_ingest"),
                required.queryWorkbench.includes("INC-77") && required.queryWorkbench.includes("max_prompt_tokens"),
                required.contextConfig.includes("Resources") && required.contextConfig.includes("raw_uri"),
                required.contextConfig.includes("Draft Runtime Config") && required.contextConfig.includes("max_depth"),
                required.contextConfig.includes("config-traversal-max_depth"),
                required.modelRegistry.includes("Qwen2.5-7B-Instruct") && required.modelRegistry.includes("bge-reranker-base"),
                required.modelRegistry.includes("Draft Model Config") && required.modelRegistry.includes("Provider"),
                required.modelRegistry.includes("OpenAI compatible") && required.modelRegistry.includes("agent supplied"),
                required.modelRegistry.includes('data-model-role="Extraction LLM"'),
                required.resourceMetrics.includes("Import duration") && required.resourceMetrics.includes("1.8"),
                required.resourceMetrics.includes("Parser duration by type") && required.resourceMetrics.includes("pdf 820ms"),
                required.resourceMetrics.includes("Chunk count") && required.resourceMetrics.includes("8"),
                required.resourceMetrics.includes("Dedupe count") && required.resourceMetrics.includes("1"),
                required.resourceMetrics.includes("Embedding duration") && required.resourceMetrics.includes("310"),
                required.resourceMetrics.includes("Extraction duration") && required.resourceMetrics.includes("540"),
                required.resourceMetrics.includes("Summary dirty lag") && required.resourceMetrics.includes("1.8"),
                required.resourceMetrics.includes("Resource retrieval hit rate") && required.resourceMetrics.includes("92%"),
                required.resourceMetrics.includes("Skill retrieval hit rate") && required.resourceMetrics.includes("88%"),
                required.resourceMetrics.includes("Parse failure count") && required.resourceMetrics.includes("0"),
                required.contextTree.includes("Node Topology") && required.contextTree.includes("child refs"),
                required.contextTree.includes("incident_77_postmortem") && required.contextTree.includes("deduped child ref"),
                required.contextTree.includes("1001300") && required.contextTree.includes("children"),
                required.contextTree.includes("selected path") && required.contextTree.includes("Metadata Details"),
                required.contextTree.includes("object key") && required.contextTree.includes("child ref"),
                required.contextTree.includes("embedding") && required.contextTree.includes("filter"),
                required.contextTree.includes("ctx:node:1001:1001300") && required.contextTree.includes("ctx:resource:1001:10029901"),
                required.contextPack.includes("10029901") && required.contextPack.includes("994020"),
                required.filesystemExplorer.includes("Filesystem-Like Explorer"),
                required.filesystemExplorer.includes("/company_a/infra_team/project_1/approvals"),
                required.filesystemExplorer.includes("ContextNode + ContextEvent + ContextEntity"),
                required.filesystemExplorer.includes("ctx:event, ctx:entity, ctxidx"),
                required.filesystemExplorer.includes("dirty marker queued"),
                required.observation.includes("Node traversal") && required.observation.includes("7 nodes"),
                required.observation.includes("Index health") && required.observation.includes("AND filters"),
                required.observation.includes("Summary lag") && required.observation.includes("dirty markers"),
                required.observation.includes("Replay audit") && required.observation.includes("77027771"),
                required.alerts.includes("Dirty summaries"),
                required.audit.includes("Feedback memory"),
                required.operators.includes("context_retrieve_with_resources"),
                required.safeguards.includes("Serving threshold"),
                required.healthSource.includes("Live health data"),
                required.healthSource.includes("health.json loaded"),
                required.safeguards.includes("Gate: Contract parity") && required.safeguards.includes("Gate: Local model fallback"),
                required.safeguards.includes("Idempotent writes") && required.safeguards.includes("Replay audit"),
                required.safeguards.includes("owner=runtime") && required.safeguards.includes("severity=blocker"),
                required.safeguards.includes("evidence=context_nine_ingestion_compression_parity_gates"),
                required.safeguards.includes("UI Gate: Overflow guard") && required.safeguards.includes("UI Gate: Actionable runbook"),
                required.safeguards.includes("browser check confirms scrollWidth &lt;= clientWidth"),
                required.uiReadinessSummary.includes("Production posture"),
                required.uiReadinessSummary.includes("Gates passed") && required.uiReadinessSummary.includes("17/17"),
                required.uiReadinessSummary.includes("Evidence linked") && required.uiReadinessSummary.includes("17/17"),
                required.uiReadinessSummary.includes("Runbook commands") && required.uiReadinessSummary.includes("5"),
                required.uiReadiness.includes("Accessible controls"),
                required.uiReadiness.includes("Actionable runbook"),
                required.uiReadiness.includes("Ten-lane parity"),
                required.uiReadiness.includes("owner=frontend"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify(required, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_keeps_last_good_payload_when_refresh_fails(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            let fetchCount = 0;
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.fetch = async () => {{
              fetchCount += 1;
              if (fetchCount === 1) {{
                return {{
                  ok: true,
                  json: async () => JSON.parse(fs.readFileSync(`${{uiDir}}/health.json`, "utf8")),
                }};
              }}
              return {{ ok: false, status: 503 }};
            }};
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(async () => {{
              await refreshHealth();
              const banner = elements["health-source-banner"];
              const checks = [
                banner.innerHTML.includes("Stale live health data"),
                banner.innerHTML.includes("HTTP 503"),
                banner.className.includes("warn"),
                elements["last-refresh"].textContent.includes("stale"),
                elements["context-kpis"].innerHTML.includes("TemporalStore"),
                elements["context-e2e-parity-body"].innerHTML.includes("module parity"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  banner: banner.innerHTML,
                  className: banner.className,
                  lastRefresh: elements["last-refresh"].textContent,
                  contextKpis: elements["context-kpis"].innerHTML,
                  parity: elements["context-e2e-parity-body"].innerHTML,
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_uses_cached_health_after_reload_outage(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);
            const health = JSON.parse(fs.readFileSync(`${{uiDir}}/health.json`, "utf8"));

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            const storage = new Map([
              ["temporalstore.monitoring.lastGoodHealth.v1", JSON.stringify({{
                saved_at_ms: Date.now(),
                data: health,
              }})],
            ]);
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.localStorage = {{
              getItem: (key) => storage.get(key) || null,
              setItem: (key, value) => storage.set(key, value),
              removeItem: (key) => storage.delete(key),
            }};
            global.fetch = async () => ({{ ok: false, status: 503 }});
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(() => {{
              const banner = elements["health-source-banner"];
              const checks = [
                banner.innerHTML.includes("Stale live health data"),
                banner.innerHTML.includes("HTTP 503"),
                banner.innerHTML.includes("Cached health data"),
                banner.className.includes("warn"),
                elements["last-refresh"].textContent.includes("stale"),
                elements["context-kpis"].innerHTML.includes("TemporalStore"),
                elements["context-e2e-parity-body"].innerHTML.includes("module parity"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  banner: banner.innerHTML,
                  className: banner.className,
                  lastRefresh: elements["last-refresh"].textContent,
                  contextKpis: elements["context-kpis"].innerHTML,
                  parity: elements["context-e2e-parity-body"].innerHTML,
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_ignores_expired_cached_health(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);
            const health = JSON.parse(fs.readFileSync(`${{uiDir}}/health.json`, "utf8"));

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            const storage = new Map([
              ["temporalstore.monitoring.lastGoodHealth.v1", JSON.stringify({{
                saved_at_ms: 1,
                data: health,
              }})],
            ]);
            global.TEMPORALSTORE_HEALTH_CACHE_MAX_AGE_MS = 5;
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.localStorage = {{
              getItem: (key) => storage.get(key) || null,
              setItem: (key, value) => storage.set(key, value),
              removeItem: (key) => storage.delete(key),
            }};
            global.fetch = async () => ({{ ok: false, status: 503 }});
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(() => {{
              const banner = elements["health-source-banner"];
              const checks = [
                !storage.has("temporalstore.monitoring.lastGoodHealth.v1"),
                banner.innerHTML.includes("Fallback sample data"),
                banner.innerHTML.includes("HTTP 503"),
                !banner.innerHTML.includes("Cached health data"),
                banner.className.includes("warn"),
                elements["last-refresh"].textContent.includes("offline sample"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  cachePresent: storage.has("temporalstore.monitoring.lastGoodHealth.v1"),
                  banner: banner.innerHTML,
                  className: banner.className,
                  lastRefresh: elements["last-refresh"].textContent,
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_pauses_interval_while_document_hidden(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);
            const health = JSON.parse(fs.readFileSync(`${{uiDir}}/health.json`, "utf8"));

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            let fetchCount = 0;
            let intervalCallback = null;
            let visibilityHandler = null;
            const documentMock = {{
              hidden: false,
              getElementById: (id) => elements[id] || null,
              addEventListener: (event, handler) => {{
                if (event === "visibilitychange") {{
                  visibilityHandler = handler;
                }}
              }},
            }};
            global.document = documentMock;
            global.fetch = async () => {{
              fetchCount += 1;
              return {{
                ok: true,
                json: async () => health,
              }};
            }};
            global.setInterval = (callback) => {{
              intervalCallback = callback;
              return 1;
            }};
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(async () => {{
              documentMock.hidden = true;
              intervalCallback();
              documentMock.hidden = false;
              await visibilityHandler();
              const checks = [
                fetchCount === 2,
                elements["last-refresh"].textContent.includes("refreshed"),
                elements["context-kpis"].innerHTML.includes("TemporalStore"),
                elements["health-source-banner"].innerHTML.includes("Live health data"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  fetchCount,
                  lastRefresh: elements["last-refresh"].textContent,
                  banner: elements["health-source-banner"].innerHTML,
                  contextKpis: elements["context-kpis"].innerHTML,
                  intervalCallback: Boolean(intervalCallback),
                  visibilityHandler: Boolean(visibilityHandler),
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_rejects_invalid_health_payload_shape(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.fetch = async () => ({{
              ok: true,
              json: async () => [],
            }});
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(() => {{
              const banner = elements["health-source-banner"];
              const checks = [
                banner.innerHTML.includes("Fallback sample data"),
                banner.innerHTML.includes("invalid health payload: expected object"),
                banner.className.includes("warn"),
                elements["last-refresh"].textContent.includes("offline sample"),
                elements["context-kpis"].innerHTML.includes("TemporalStore"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  banner: banner.innerHTML,
                  className: banner.className,
                  lastRefresh: elements["last-refresh"].textContent,
                  contextKpis: elements["context-kpis"].innerHTML,
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_times_out_and_dedupes_refreshes(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this.disabled = false;
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            let fetchCount = 0;
            global.TEMPORALSTORE_REFRESH_TIMEOUT_MS = 5;
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.fetch = async () => {{
              fetchCount += 1;
              return new Promise(() => {{}});
            }};
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});
            refreshHealth();
            refreshHealth();

            setTimeout(() => {{
              const checks = [
                fetchCount === 1,
                elements["health-source-banner"].innerHTML.includes("Fallback sample data"),
                elements["health-source-banner"].innerHTML.includes("refresh timeout after 5 ms"),
                elements["health-source-banner"].className.includes("warn"),
                elements["refresh"].disabled === false,
                elements["refresh"].textContent === "Refresh",
                elements["refresh"]["aria-busy"] === "false",
                elements["last-refresh"].textContent.includes("offline sample"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  fetchCount,
                  banner: elements["health-source-banner"].innerHTML,
                  className: elements["health-source-banner"].className,
                  refreshDisabled: elements["refresh"].disabled,
                  refreshText: elements["refresh"].textContent,
                  refreshBusy: elements["refresh"]["aria-busy"],
                  lastRefresh: elements["last-refresh"].textContent,
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)

    def test_context_app_js_shows_fallback_source_when_health_fetch_fails(self) -> None:
        script = textwrap.dedent(
            f"""
            const fs = require("fs");
            const vm = require("vm");
            const uiDir = {json.dumps(str(UI_DIR))};
            const html = fs.readFileSync(`${{uiDir}}/index.html`, "utf8");
            const ids = [...html.matchAll(/id="([^"]+)"/g)].map((match) => match[1]);

            class Element {{
              constructor(id) {{
                this.id = id;
                this.className = "";
                this.textContent = "";
                this._innerHTML = "";
              }}
              set innerHTML(value) {{
                this._innerHTML = String(value);
                this.textContent = String(value).replace(/<[^>]*>/g, " ").replace(/\\s+/g, " ").trim();
              }}
              get innerHTML() {{
                return this._innerHTML;
              }}
              setAttribute(name, value) {{
                this[name] = String(value);
              }}
              addEventListener() {{}}
            }}

            const elements = Object.fromEntries(ids.map((id) => [id, new Element(id)]));
            global.document = {{ getElementById: (id) => elements[id] || null }};
            global.fetch = async () => ({{ ok: false, status: 503 }});
            global.setInterval = () => 0;
            global.window = {{ innerWidth: 1280 }};

            vm.runInThisContext(fs.readFileSync(`${{uiDir}}/app.js`, "utf8"), {{ filename: "app.js" }});

            setTimeout(() => {{
              const banner = elements["health-source-banner"];
              const checks = [
                banner.innerHTML.includes("Fallback sample data"),
                banner.innerHTML.includes("health.json unavailable"),
                banner.className.includes("warn"),
                elements["last-refresh"].textContent.includes("offline sample"),
                elements["context-kpis"].innerHTML.includes("TemporalStore"),
              ];
              if (checks.some((ok) => !ok)) {{
                console.error(JSON.stringify({{
                  banner: banner.innerHTML,
                  className: banner.className,
                  lastRefresh: elements["last-refresh"].textContent,
                  contextKpis: elements["context-kpis"].innerHTML,
                }}, null, 2));
                process.exit(1);
              }}
            }}, 25);
            """
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as handle:
            handle.write(script)
            script_path = Path(handle.name)
        try:
            subprocess.run(["node", str(script_path)], check=True, timeout=10)
        finally:
            script_path.unlink(missing_ok=True)


def fetch_text(url: str) -> str:
    with urlopen(url, timeout=5) as response:
        assert response.status == 200
        return response.read().decode("utf-8")


if __name__ == "__main__":
    unittest.main()
