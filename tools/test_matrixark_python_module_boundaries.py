#!/usr/bin/env python3
"""Regression tests for the MatrixArk Python MCP module layout."""

from __future__ import annotations

import ast
import importlib
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = REPO_ROOT / "tools"


class MatrixArkPythonModuleBoundaryTest(unittest.TestCase):
    def test_package_imports_expose_server_and_schema_catalog(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        schemas_mod = importlib.import_module("tools.matrixark_mcp_schemas")
        self.assertTrue(hasattr(server_mod, "MatrixArkMcpServer"))
        self.assertTrue(hasattr(server_mod, "MatrixArkLocalAdapter"))
        self.assertGreaterEqual(len(schemas_mod.TOOLS), 10)

    def test_memory_phase_and_retrieval_budget_fields_are_public_schema(self) -> None:
        schemas_mod = importlib.import_module("tools.matrixark_mcp_schemas")
        tools_by_name = {tool["name"]: tool for tool in schemas_mod.TOOLS}
        session_props = tools_by_name["matrixark_session_commit"]["inputSchema"]["properties"]
        batch_props = tools_by_name["matrixark_batch_extract"]["inputSchema"]["properties"]
        retrieve_props = tools_by_name["matrixark_retrieve"]["inputSchema"]["properties"]

        self.assertEqual(["provisional", "final", "standalone"], session_props["extraction_phase"]["enum"])
        self.assertEqual(["provisional", "final", "standalone"], batch_props["extraction_phase"]["enum"])
        self.assertIn("final_session_boundary", session_props)
        self.assertIn("final_session_boundary", batch_props)
        self.assertIn("include_retrieval_metrics", retrieve_props)
        self.assertIn("include_retrieval_debug", retrieve_props)
        self.assertIn("debug_context_pack", retrieve_props)
        self.assertIn("remote MatrixArk budget", retrieve_props["local_context_safety_margin_tokens"]["description"])

    def test_direct_tools_path_imports_still_work_for_script_launches(self) -> None:
        sys.path.insert(0, str(TOOLS_DIR))
        try:
            server_mod = importlib.import_module("matrixark_mcp_server")
            self.assertTrue(hasattr(server_mod, "MatrixArkMcpServer"))
            self.assertTrue(hasattr(server_mod, "MatrixArkTemporalStoreDirectAdapter"))
        finally:
            try:
                sys.path.remove(str(TOOLS_DIR))
            except ValueError:
                pass

    def test_modular_async_ingest_reports_session_buffer_readiness(self) -> None:
        async_mod = importlib.import_module("tools.matrixark_mcp_async_ingest")

        class Batch:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class Target:
            def __init__(self) -> None:
                self.records = []
                self.pending = []
                self.commit_calls = []

            def auto_batch_extract_enabled(self, args, *, kind):
                return bool(args.get("auto_batch_extract"))

            def default_session_node_path(self, scope):
                return ["tenant:t", "user:u", f"session:{scope['session_id']}"]

            def ensure_context_node_path(self, **kwargs):
                return {"created": True}

            def write_batch(self, _name):
                return Batch()

            def mark_node_summary_dirty(self, **_kwargs):
                return [10]

            def append(self, record):
                self.records.append(record)

            def session_buffer_enabled(self, args, *, kind):
                return True

            def append_session_buffer_event(self, **kwargs):
                self.pending.append({"event_id_hash": kwargs["event_id_hash"], "envelope": kwargs["envelope"]})

            def pending_session_events(self, _scope):
                return list(self.pending)

            def session_boundary_commit_requested(self, args, *, hook=None):
                return False

            def session_commit(self, args, *, hook=None):
                self.commit_calls.append(args)
                self.pending.clear()
                return {"status": "committed", "trigger_policy": "threshold"}

        target = Target()
        envelope = {
            "kind": "message",
            "scope": {
                "account_id": "acct",
                "tenant_id": "tenant",
                "user_id": "user",
                "session_id": "session",
            },
            "metadata": {},
            "messages": [{"role": "user", "content": "first live message"}],
            "ingestion_time_ms": 123,
            "storage_options": {},
            "storage_route": {},
        }
        args = {"async_processing": True, "auto_batch_extract": True, "session_buffer_threshold": 2}

        first = async_mod.lightweight_async_accept(target, args, envelope=envelope, hook=None, idle_commit_result={})
        self.assertFalse(first["session_buffer"]["threshold_ready"])
        self.assertFalse(first["session_buffer"]["idle_ready"])
        self.assertEqual(1, first["session_buffer"]["pending_event_count"])
        self.assertIsNone(first["auto_batch_extract_result"])

        second_envelope = {**envelope, "messages": [{"role": "assistant", "content": "second live message"}], "ingestion_time_ms": 124}
        second = async_mod.lightweight_async_accept(target, args, envelope=second_envelope, hook=None, idle_commit_result={})
        self.assertTrue(second["session_buffer"]["threshold_ready"])
        self.assertFalse(second["session_buffer"]["idle_ready"])
        self.assertEqual(2, second["session_buffer"]["pending_event_count"])
        self.assertEqual("committed", second["auto_batch_extract_result"]["status"])
        self.assertEqual(1, len(target.commit_calls))

    def test_modular_session_runtime_reports_trigger_evidence(self) -> None:
        runtime_mod = importlib.import_module("tools.matrixark_mcp_session_runtime")

        class Adapter:
            def __init__(self) -> None:
                self.pending = []
                self.appended = []

            def pending_session_events(self, _scope):
                return list(self.pending)

            def default_session_node_path(self, scope):
                return ["tenant:t", "user:u", f"session:{scope['session_id']}"]

            def read_all(self):
                return []

            def batch_extract(self, args, *, hook=None):
                return {
                    "batch_id_hash": 7,
                    "node_hash": 8,
                    "node_path": args["metadata"]["node_path"],
                    "extraction_context_event_count": len(args.get("extraction_context_event_ids", [])),
                    "segments_written": 1,
                    "entities_written": 3,
                    "profile_entities_written": 1,
                    "indexes_written": 5,
                    "summary_refresh": {"status": "dirty_marked", "dirty_hashes": [10, 11]},
                }

            def append(self, record):
                self.appended.append(record)

        scope = {
            "account_id": "acct",
            "tenant_id": "tenant",
            "user_id": "user",
            "session_id": "session",
        }
        adapter = Adapter()
        adapter.pending = [
            {
                "event_id_hash": 1,
                "updated_at_ms": 100,
                "envelope": {
                    "messages": [{"role": "user", "content": "first pending message"}],
                    "ingestion_time_ms": 100,
                },
            }
        ]

        deferred = runtime_mod.session_commit(
            adapter,
            {"scope": scope, "threshold_messages": 2, "force": False, "commit_reason": "threshold"},
        )
        self.assertEqual("deferred", deferred["status"])
        self.assertFalse(deferred["trigger_evidence"]["threshold_ready"])
        self.assertFalse(deferred["trigger_evidence"]["idle_ready"])
        self.assertEqual(1, deferred["trigger_evidence"]["pending_event_count"])

        committed = runtime_mod.session_commit(
            adapter,
            {"scope": scope, "threshold_messages": 1, "force": False, "commit_reason": "threshold"},
        )
        self.assertEqual("committed", committed["status"])
        self.assertTrue(committed["trigger_evidence"]["threshold_ready"])
        self.assertFalse(committed["trigger_evidence"]["idle_ready"])
        self.assertEqual("threshold", committed["trigger_policy"])
        self.assertEqual(0, committed["memory_layers_written"]["context_events"])
        self.assertEqual(1, committed["memory_layers_written"]["segments"])
        self.assertEqual(3, committed["memory_layers_written"]["session_entities"])
        self.assertEqual(1, committed["memory_layers_written"]["profile_entities"])
        self.assertEqual(5, committed["memory_layers_written"]["secondary_indexes"])
        self.assertEqual(2, committed["memory_layers_written"]["summary_dirty_nodes"])
        self.assertEqual("dirty_marked", committed["memory_layers_written"]["summary_refresh_status"])
        self.assertEqual("provisional", committed["memory_layers_written"]["extraction_phase"])
        self.assertEqual(committed["trigger_evidence"], adapter.appended[0]["trigger_evidence"])

    def test_modular_batch_extract_promotes_profile_entities(self) -> None:
        batch_mod = importlib.import_module("tools.matrixark_mcp_local_batch_extract_runtime")

        class Adapter:
            def __init__(self) -> None:
                self.records = []
                self.nodes = []
                self.dirty_counter = 0

            def read_all(self):
                return []

            def _observe_model_latency(self, *_args):
                return None

            def default_session_node_path(self, scope):
                return ["tenant:tenant_mod", "user:user_mod", f"session:{scope['session_id']}"]

            def ensure_context_node_path(self, **kwargs):
                self.nodes.append(kwargs)
                return {"created": True, "node_path": kwargs["node_path"]}

            def find_latest_entity(self, **_kwargs):
                return None

            def append_many(self, records):
                self.records.extend(records)

            def node_summary_dirty_records(self, **kwargs):
                self.dirty_counter += 1
                dirty_hash = 44 + self.dirty_counter
                return [
                    dirty_hash
                ], [
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_path": kwargs["node_path"],
                        "dirty_reason": kwargs["dirty_reason"],
                        "source_ref_type": kwargs["source_ref_type"],
                        "scope": kwargs["scope"],
                    }
                ]

        adapter = Adapter()
        envelope = {
            "kind": "message",
            "scope": {
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
                "session_id": "session_mod",
            },
            "metadata": {},
            "messages": [{"role": "assistant", "content": "Commit abc123 was pushed."}],
            "ingestion_time_ms": 123,
            "storage_options": {},
            "storage_route": {},
        }
        extraction = {
            "mode": "one_pass",
            "segment_provider": {"name": "deterministic"},
            "classification": "NEW_EVENT",
            "event_type": "memory_update",
            "schema": "matrixark.memory.v1",
            "message_count": 1,
            "token_count_estimate": 6,
            "batch_summary": "Commit abc123 was pushed.",
            "events": [],
            "entities": [
                {
                    "entity_type": "assistant_decision",
                    "entity_name": "commit",
                    "state": "Commit abc123 was pushed.",
                    "confidence": 0.9,
                    "operator": "UPSERT",
                    "source_refs": ["assistant:0"],
                    "field_patches": [],
                }
            ],
            "segments": [],
            "indexes": ["entity_type:assistant_decision"],
        }

        with mock.patch.object(batch_mod, "one_pass_memory_extraction", return_value=extraction):
            result = batch_mod.batch_extract_after_start(
                adapter,
                {"skip_prior_context": True},
                {
                    "envelope": envelope,
                    "hook": {"hook_type": "hook_boundary", "codex_event": "Stop"},
                    "threshold": 1,
                    "derive_from_existing_events": True,
                    "source_event_ids": [101],
                    "extraction_phase": "final",
                    "final_session_boundary": True,
                    "force": True,
                    "deferred_result": None,
                },
            )

        self.assertEqual(1, result["entities_written"])
        self.assertEqual(1, result["profile_entities_written"])
        self.assertIn("summary_refresh", result)
        self.assertGreaterEqual(len(result["summary_refresh"]["dirty_hashes"]), 2)
        self.assertTrue(result["summary_refresh"]["session_dirty_hashes"])
        self.assertTrue(result["summary_refresh"]["profile_dirty_hashes"])
        session_entities = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_entity"
            and record.get("memory_scope") == "session"
            and record.get("session_continuity") == "same_session"
        ]
        profile_entities = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_entity"
            and record.get("memory_scope") == "user_profile"
            and record.get("session_continuity") == "cross_session"
        ]
        self.assertEqual(1, len(session_entities))
        self.assertEqual(1, len(profile_entities))
        self.assertEqual(["session_mod"], profile_entities[0]["source_session_ids"])
        self.assertEqual([session_entities[0]["entity_hash"]], profile_entities[0]["source_entity_hashes"])
        self.assertEqual(
            ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            profile_entities[0]["node_path"],
        )
        profile_indexes = [
            record
            for record in adapter.records
            if record.get("record_type") == "context_index"
            and record.get("data_model") == "context_profile_entity"
        ]
        self.assertTrue(any(record.get("index_name") == "memory_scope:user_profile" for record in profile_indexes))
        self.assertTrue(any(record.get("index_name") == "session_continuity:cross_session" for record in profile_indexes))
        dirty_records = [record for record in adapter.records if record.get("record_type") == "context_summary_dirty"]
        self.assertTrue(any(record.get("dirty_reason") == "new_event" for record in dirty_records))
        profile_dirty = [
            record
            for record in dirty_records
            if record.get("dirty_reason") == "profile_entity_promoted"
        ]
        self.assertEqual(1, len(profile_dirty))
        self.assertEqual(
            ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            profile_dirty[0]["node_path"],
        )

    def test_modular_entity_scan_admits_cross_session_profile_bridge(self) -> None:
        scan_mod = importlib.import_module("tools.matrixark_mcp_retrieve_entity_scan")
        record = {
            "record_type": "context_entity",
            "entity_hash": 77,
            "node_hash": 88,
            "node_path": ["tenant:tenant_mod", "user:user_mod", "profile:long_term_memory"],
            "access_scope": {
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
            },
            "entity_type": "assistant_decision",
            "entity_name": "commit",
            "state": "Commit abc123 was pushed for modular profile retrieval.",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "source_session_ids": ["session-a", "session-b"],
            "source_entity_hashes": [11, 22],
            "source_roles": ["assistant"],
            "source_hook_types": ["hook_boundary"],
            "source_codex_events": ["Stop"],
            "updated_at_ms": 200,
        }

        primary, auxiliary, dropped, matched, reason = scan_mod.scan_entity_candidates(
            [record],
            retrieval_scope={
                "account_id": "acct_mod",
                "tenant_id": "tenant_mod",
                "user_id": "user_mod",
                "session_id": "session-c",
            },
            selected_by_tree=lambda _record: False,
            index_terms_by_batch={},
            index_terms_by_node={},
            index_terms_by_ref={},
            secondary_index_filter_groups=[],
            secondary_index_filter_mode="off",
            admit_candidate_for_node=lambda _record: True,
            query_terms={"commit", "abc123"},
            query_embedding=[],
            entity_embedding_vectors={},
            node_scores={},
            annotate_session_continuity=lambda candidate, _record: candidate,
            ranking={},
            reference_time_ms=300,
            deadline_exceeded=lambda: False,
        )

        self.assertEqual("", reason)
        self.assertEqual(0, dropped)
        self.assertEqual(1, matched)
        self.assertEqual(1, len(primary))
        self.assertGreaterEqual(len(auxiliary), 1)
        candidate = primary[0]
        self.assertEqual("user_profile", candidate["memory_scope"])
        self.assertEqual("cross_session", candidate["session_continuity"])
        self.assertEqual(["session-a", "session-b"], candidate["source_session_ids"])
        self.assertEqual([11, 22], candidate["source_entity_hashes"])
        self.assertEqual("selected as cross-session user-profile entity bridge", candidate["selection_reason"])

    def test_mcp_entrypoint_reexports_split_modules(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")
        admin_mod = importlib.import_module("tools.matrixark_mcp_admin")
        backends_mod = importlib.import_module("tools.matrixark_mcp_backends")
        dispatch_mod = importlib.import_module("tools.matrixark_mcp_dispatch")
        ingestion_mod = importlib.import_module("tools.matrixark_mcp_ingestion")
        retrieval_mod = importlib.import_module("tools.matrixark_mcp_retrieval")
        requests_mod = importlib.import_module("tools.matrixark_mcp_requests")

        self.assertIs(server_mod.MatrixArkServiceMetrics, metrics_mod.MatrixArkServiceMetrics)
        self.assertIs(server_mod.MatrixArkLocalAdapter, local_mod.MatrixArkLocalAdapter)
        self.assertIs(server_mod.MatrixArkTemporalStoreDirectAdapter, temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        self.assertIs(server_mod.MatrixArkTemporalStoreRustAdapter, temporal_mod.MatrixArkTemporalStoreRustAdapter)
        self.assertTrue(ingestion_mod.is_ingestion_tool("matrixark_ingest"))
        self.assertTrue(retrieval_mod.is_retrieval_tool("matrixark_retrieve"))
        self.assertTrue(admin_mod.is_admin_tool("matrixark_management_portal"))
        self.assertTrue(callable(backends_mod.build_mcp_adapter))
        self.assertTrue(callable(dispatch_mod.dispatch_matrixark_tool))
        self.assertTrue(callable(requests_mod.normalize_mcp_tool_request))

    def test_mcp_entrypoint_stays_small(self) -> None:
        server_lines = (TOOLS_DIR / "matrixark_mcp_server.py").read_text(encoding="utf-8").splitlines()
        self.assertLessEqual(len(server_lines), 750)

    def test_public_mcp_modules_avoid_wildcard_imports(self) -> None:
        module_names = [
            "matrixark_mcp_server.py",
            "matrixark_mcp_backends.py",
            "matrixark_mcp_dispatch.py",
            "matrixark_mcp_requests.py",
            "matrixark_mcp_ingestion.py",
            "matrixark_mcp_retrieval.py",
            "matrixark_mcp_admin.py",
        ]
        offenders: list[str] = []
        for module_name in module_names:
            tree = ast.parse((TOOLS_DIR / module_name).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
                    offenders.append(f"{module_name}:{node.lineno}")
        self.assertEqual([], offenders)

    def test_pyproject_exposes_matrixark_console_scripts(self) -> None:
        pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn('matrixark-mcp-server = "tools.matrixark_mcp_server:main"', pyproject_text)
        self.assertIn('matrixark-http-portal = "tools.matrixark_admin:http_portal_main"', pyproject_text)
        self.assertIn('matrixark-agent-hook = "tools.matrixark_agent_hook:main"', pyproject_text)
        self.assertIn('matrixark-admin = "tools.matrixark_admin:main"', pyproject_text)
        self.assertIn('matrixark-local-recovery = "tools.matrixark_mcp_recovery:main"', pyproject_text)

    def test_production_defaults_doc_records_control_plane_boundary(self) -> None:
        defaults_doc = (REPO_ROOT / "docs" / "matrixark_mcp_production_defaults.md").read_text(encoding="utf-8")
        required_snippets = [
            "Python remains the MCP/HTTP/control-plane layer",
            "Native C++ or Rust MCP servers are future optimizations, not a v1 requirement",
            "C++ and Rust TemporalStore remain the serving engines",
            "compact and audit-light",
            "Full replay/debug audit is opt-in",
            "Cloud mode requires an API key or trusted SSO gateway identity",
            "Local/dev mode may use generated local scope defaults",
            "Codex is the only production-supported hook client today",
            "Claude Code,",
            "Claude Desktop, Cursor, OpenClaw, OpenCode, Aider, Continue, Cline/Roo",
            "visible local context only",
            "before LLM: `matrixark_retrieve`",
            "after answer/tool: `matrixark_ingest`",
            "resource added: import resource or skill",
            "feedback: `matrixark_feedback`",
            "session boundary: `matrixark_session_commit`",
            "PromptSubmit, assistant responses, and selected tool evidence",
            "`extraction_phase=provisional`",
            "`final_session_boundary=false`",
            "Idle timeout triggers the same provisional checkpoint",
            "Stop, SubagentStop, and PostCompact trigger the final session boundary",
            "`extraction_phase=final`",
            "`final_session_boundary=true`",
            "Retrieval keeps visible local context and a safety margin first",
            "Retrieval metrics and debug",
            "ContextPacks are opt-in audit fields",
            "ingest/retrieve QPS",
            "p50/p95/p99 latency",
            "partial ContextPack count",
            "audit write failures",
            "dirty summary lag",
            "resource import lag",
            "model fallback flags",
            "backend readiness",
            "messages;",
            "resources;",
            "skills;",
            "events/entities;",
            "ContextPacks;",
            "users;",
            "API keys;",
            "audit logs.",
            "scoped, redacted, paged tables",
            "no private checkout paths",
            "no local credentials or secrets",
            "no vendored build outputs",
            "reproducible local validation commands",
            "C++/Rust scale matrix gate",
        ]
        for snippet in required_snippets:
            self.assertIn(snippet, defaults_doc)

    def test_metrics_prometheus_contract_includes_production_signals(self) -> None:
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        metrics = metrics_mod.MatrixArkServiceMetrics()
        metrics.observe_operation("ingest", "ok", 12.5)
        metrics.observe_operation("retrieve", "ok", 25.0, timeout=True)
        metrics.observe_retrieve_result({"partial": True, "tokens": {"remote_budget": 100, "remote": 80}})
        metrics.observe_model_latency("embedding", 8.0)
        metrics.update_gauges(dirty_summary_lag_ms=10, resource_import_lag_ms=20, queue_depth=3, audit_write_failures=1)
        metrics.observe_backend_ready(True, "ready")
        metrics.update_model_fallback_flags(embedding=True, reader=False, judge=True)

        prometheus = metrics.render_prometheus(backend="temporalstore-direct", storage_mode="shared_store")
        required_metrics = [
            'matrixark_backend_info{backend="temporalstore-direct",storage_mode="shared_store"} 1',
            "matrixark_backend_ready",
            'matrixark_service_qps{backend="temporalstore-direct",storage_mode="shared_store",operation="ingest"}',
            'matrixark_service_qps{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.5"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.95"}',
            'matrixark_service_latency_ms{backend="temporalstore-direct",storage_mode="shared_store",operation="retrieve",quantile="0.99"}',
            "matrixark_timeouts_total",
            "matrixark_partial_context_pack_total",
            "matrixark_resource_import_queue_depth",
            "matrixark_audit_write_failures_total",
            "matrixark_dirty_summary_lag_ms",
            "matrixark_resource_import_lag_ms",
            "matrixark_token_pressure_ratio",
            'matrixark_model_fallback_flag{backend="temporalstore-direct",storage_mode="shared_store",flag="embedding"} 1',
            'matrixark_model_fallback_flag{backend="temporalstore-direct",storage_mode="shared_store",flag="judge"} 1',
            "matrixark_model_latency_ms",
        ]
        for metric in required_metrics:
            self.assertIn(metric, prometheus)

    def test_request_boundary_generates_idempotency_and_validates_storage(self) -> None:
        requests_mod = importlib.import_module("tools.matrixark_mcp_requests")
        args = requests_mod.normalize_mcp_tool_request(
            "matrixark_ingest",
            {
                "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
                "storage_options": {"route": "raft_async"},
                "agent_name": "codex",
            },
            write_tools={"matrixark_ingest"},
        )
        self.assertRegex(args["idempotency_key"], r"^auto:matrixark_ingest:")
        self.assertEqual("raft", args["storage_options"]["storage_family"])
        self.assertEqual("async", args["storage_options"]["write_mode"])
        self.assertEqual("acct_local", args["scope"]["account_id"])
        self.assertEqual("tenant_codex", args["scope"]["tenant_id"])
        self.assertIn("scope_key", args["scope"])
        self.assertGreater(args["scope"]["tenant_hash"], 0)

        with self.assertRaises(Exception):
            requests_mod.normalize_mcp_tool_request(
                "matrixark_ingest",
                {"messages": [{"role": "user", "content": "bad"}], "storage_options": {"route": "not_real"}},
                write_tools={"matrixark_ingest"},
            )

    def test_all_mutating_mcp_routes_include_idempotency_boundary(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        self.assertIn("matrixark_auth_sso_login", server_mod.MatrixArkMcpServer.IDEMPOTENT_WRITE_TOOLS)

    def test_core_module_has_no_duplicate_top_level_symbols(self) -> None:
        module_path = TOOLS_DIR / "matrixark_mcp_core.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        seen: dict[str, int] = {}
        duplicates: dict[str, list[int]] = {}
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            previous = seen.get(node.name)
            if previous is not None:
                duplicates.setdefault(node.name, [previous]).append(node.lineno)
            else:
                seen[node.name] = node.lineno
        self.assertEqual({}, duplicates)

    def test_token_selector_stops_on_hard_deadline(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        calls = {"count": 0}

        def deadline_after_first_candidate() -> bool:
            calls["count"] += 1
            return calls["count"] > 1

        candidates = [
            {
                "ref_type": "event",
                "ref_hash": index,
                "score": 0.9 - (index * 0.01),
                "text": f"important answer-bearing context {index}",
            }
            for index in range(5)
        ]
        selected, used_tokens, dropped = core_mod.select_token_budgeted_refs(
            candidates,
            [],
            max_context_tokens=200,
            auxiliary_quota=0,
            deadline_exceeded=deadline_after_first_candidate,
            deadline_reason="test_deadline",
        )
        self.assertEqual(1, len(selected))
        self.assertGreater(used_tokens, 0)
        self.assertTrue(dropped["deadline_exceeded"])
        self.assertEqual("test_deadline", dropped["deadline_reason"])
        self.assertEqual(4, dropped["deadline"])

    def test_serving_pack_lifts_repeated_session_continuity(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = core_mod.compact_context_pack_for_serving(
            {
                "context_pack_id": "pack-1",
                "selected_refs": [
                    {"ref_type": "event", "text": "same session fact", "token_estimate": 3, "session_continuity": "same_session"},
                    {"ref_type": "entity", "text": "same session entity", "token_estimate": 3, "session_continuity": "same_session"},
                    {"ref_type": "summary", "text": "cross session summary", "token_estimate": 4, "session_continuity": "cross_session"},
                ],
                "used_context_tokens": 10,
            }
        )
        self.assertNotIn("selected_refs", pack)
        self.assertNotIn("selected_ref_groups", pack)
        self.assertIn("groups", pack)
        self.assertEqual("same_session", pack["defaults"]["session_continuity"])
        self.assertEqual(2, pack["counts"]["session_continuity"]["same_session"])
        event_group = next(group for group in pack["groups"] if group["type"] == "event")
        summary_group = next(group for group in pack["groups"] if group["type"] == "summary")
        self.assertNotIn("ref_type", event_group["items"][0])
        self.assertNotIn("session_continuity", event_group["items"][0])
        self.assertEqual("cross_session", summary_group["items"][0]["session_continuity"])
        self.assertEqual(3, event_group["items"][0]["tokens"])

    def test_serving_pack_preserves_summary_cross_session_lineage(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = core_mod.compact_context_pack_for_serving(
            {
                "context_pack_id": "pack-2",
                "selected_refs": [
                    {
                        "ref_type": "event",
                        "text": "same-session working note.",
                        "token_estimate": 3,
                        "memory_scope": "session",
                        "session_continuity": "same_session",
                    },
                    {
                        "ref_type": "summary",
                        "text": "profile summary: user prefers Ubuntu shared repo folders.",
                        "token_estimate": 9,
                        "memory_scope": "user_profile",
                        "session_continuity": "cross_session",
                        "extraction_phase": "final",
                        "source_session_ids": ["session-a", "session-b"],
                        "source_entity_hashes": [101, 202, 303],
                        "source_roles": ["user", "assistant"],
                        "source_hook_types": ["hook_boundary"],
                        "source_codex_events": ["UserPromptSubmit", "Stop"],
                    }
                ],
                "used_context_tokens": 9,
            }
        )

        summary_group = next(group for group in pack["groups"] if group["type"] == "summary")
        item = summary_group["items"][0]
        self.assertEqual("user_profile", item["memory_scope"])
        self.assertEqual("cross_session", item["session_continuity"])
        self.assertEqual(["session-a", "session-b"], item["source_session_ids"])
        self.assertEqual(3, item["source_entity_count"])
        self.assertEqual(["user", "assistant"], item["source_roles"])
        self.assertEqual(["hook_boundary"], item["source_hook_types"])
        self.assertEqual(["UserPromptSubmit", "Stop"], item["source_codex_events"])
        self.assertNotIn("source_entity_hashes", item)

    def test_shared_pack_builder_exposes_memory_layer_budget(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_retrieve_metrics")
        selected = [
            {
                "ref_type": "entity",
                "ref_hash": 10,
                "text": "tool_evidence: tests = Exit code: 0 Ran 87 tests OK",
                "token_estimate": 8,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "entity_type": "tool_evidence",
                "source_roles": ["tool"],
                "source_hook_types": ["hook_boundary"],
                "source_codex_events": ["PostToolUse"],
            }
        ]
        pack = builder_mod.build_context_pack(
            context_pack_id=123,
            selected=selected,
            local_budget={"items": [], "tokens": 0},
            serving_selected=selected,
            dropped_over_budget={},
            serving_dropped=[],
            layer_scores=[],
            question_type="fact",
            query_plan={},
            retrieval_session_scope="prefer",
            cross_session_policy={"enabled": True},
            shared_context_policy={"enabled": True},
            retrieval_scan_stats={},
            ranking={},
            min_similarity_score=0.2,
            max_global_candidates=24,
            max_selected_refs=8,
            budget_fill_policy="quality_first",
            traversal={},
            top_k_per_layer=4,
            max_children_scored_per_parent=16,
            hard_max_children_scored_per_parent=64,
            max_candidates_per_node=8,
            max_raw_events_per_node=4,
            selected_node_hashes=set(),
            selected_paths=set(),
            tree_candidate_records_count=1,
            tree_prefilter_dropped_count=0,
            fanout_dropped_count=0,
            raw_event_time_window_dropped_count=0,
            secondary_index_filter_groups=[],
            secondary_index_matched_count=0,
            secondary_index_dropped_count=0,
            secondary_index_filter_mode="off",
            rerank_policy={},
            time_weighted_recall={"freshness_tolerance_ms": 0, "half_life_ms": 0},
            reinforcement={},
            auxiliary_quota=0.0,
            storage_options={},
            deadline_ms=0,
            started_perf=0.0,
            partial_context_pack=False,
            primary_candidate_count=1,
            auxiliary_candidate_count=0,
            used_context_tokens=8,
            local_tokens=0,
            remote_context_budget_tokens=100,
            max_context_tokens=100,
            safety_margin_tokens=0,
            budget_source="test",
            quality_warnings=[],
            audit_mode="off",
            audit_sample_rate=0.0,
            debug_refs=False,
        )
        layer_budget = pack["recall_policy"]["memory_layer_budget"]
        self.assertEqual(1, layer_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, layer_budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, layer_budget["by_ref_type"]["entity"]["refs"])
        self.assertEqual(1, layer_budget["by_entity_type"]["tool_evidence"]["refs"])
        self.assertEqual(1, layer_budget["by_source_role"]["tool"]["refs"])
        self.assertEqual(1, layer_budget["by_hook_type"]["hook_boundary"]["refs"])
        self.assertEqual(1, layer_budget["by_codex_event"]["PostToolUse"]["refs"])
        self.assertEqual(1, layer_budget["final_session_boundary_ref_count"])

        metrics_mod.attach_python_retrieval_metrics(
            pack,
            {},
            stage_latencies_ms={},
            retrieval_scan_stats={},
            selected=selected,
            dropped_over_budget={},
            records=[],
        )
        self.assertEqual(layer_budget, pack["retrieval_metrics"]["memory_layer_budget"])

    def test_shared_pack_builder_reports_profile_shadowed_dropped_budget(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_retrieve_metrics")
        selected = [
            {
                "ref_type": "entity",
                "ref_hash": 22,
                "text": "decision: shared repo path = /root/src/github-services/TemporalStore-memory-next",
                "token_estimate": 9,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "entity_type": "decision",
                "extraction_phase": "final",
            }
        ]
        dropped = {
            "stale": 1,
            "estimated_tokens": {"stale": 6},
            "refs": [
                {
                    "ref_type": "entity",
                    "ref_hash": 11,
                    "drop_reason": "stale",
                    "token_estimate": 6,
                    "memory_scope": "session",
                    "session_continuity": "same_session",
                    "entity_type": "decision",
                    "stale_or_superseded": True,
                    "profile_shadowed_by_ref_hash": 22,
                    "profile_shadowed_reason": "source_entity_lineage",
                }
            ],
        }
        serving_selected, serving_dropped = builder_mod.prepare_serving_refs(
            selected=selected,
            dropped_over_budget=dropped,
            debug_refs=False,
        )
        pack = builder_mod.build_context_pack(
            context_pack_id=124,
            selected=selected,
            local_budget={"items": [], "tokens": 0},
            serving_selected=serving_selected,
            dropped_over_budget=dropped,
            serving_dropped=serving_dropped,
            layer_scores=[],
            question_type="current_state",
            query_plan={"query_type": "current_state"},
            retrieval_session_scope="prefer",
            cross_session_policy={"enabled": True},
            shared_context_policy={"enabled": True},
            retrieval_scan_stats={},
            ranking={},
            min_similarity_score=0.2,
            max_global_candidates=24,
            max_selected_refs=8,
            budget_fill_policy="quality_first",
            traversal={},
            top_k_per_layer=4,
            max_children_scored_per_parent=16,
            hard_max_children_scored_per_parent=64,
            max_candidates_per_node=8,
            max_raw_events_per_node=4,
            selected_node_hashes=set(),
            selected_paths=set(),
            tree_candidate_records_count=2,
            tree_prefilter_dropped_count=0,
            fanout_dropped_count=0,
            raw_event_time_window_dropped_count=0,
            secondary_index_filter_groups=[],
            secondary_index_matched_count=0,
            secondary_index_dropped_count=0,
            secondary_index_filter_mode="off",
            rerank_policy={},
            time_weighted_recall={"freshness_tolerance_ms": 0, "half_life_ms": 0},
            reinforcement={},
            auxiliary_quota=0.0,
            storage_options={},
            deadline_ms=0,
            started_perf=0.0,
            partial_context_pack=False,
            primary_candidate_count=2,
            auxiliary_candidate_count=0,
            used_context_tokens=9,
            local_tokens=0,
            remote_context_budget_tokens=100,
            max_context_tokens=100,
            safety_margin_tokens=0,
            budget_source="test",
            quality_warnings=[],
            audit_mode="off",
            audit_sample_rate=0.0,
            debug_refs=False,
        )
        dropped_budget = pack["recall_policy"]["dropped_memory_layer_budget"]
        self.assertEqual(1, dropped_budget["stale_ref_count"])
        self.assertEqual(6, dropped_budget["stale_token_estimate"])
        self.assertEqual(1, dropped_budget["profile_shadowed_ref_count"])
        self.assertEqual(6, dropped_budget["profile_shadowed_token_estimate"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_drop_reason"]["stale"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_memory_scope"]["session"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_session_continuity"]["same_session"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_profile_shadowed_reason"]["source_entity_lineage"])

        metrics_mod.attach_python_retrieval_metrics(
            pack,
            {},
            stage_latencies_ms={},
            retrieval_scan_stats={},
            selected=selected,
            dropped_over_budget=serving_dropped,
            records=[],
        )
        self.assertEqual(dropped_budget, pack["retrieval_metrics"]["dropped_memory_layer_budget"])


class MatrixArkMcpProtocolHardeningTest(unittest.TestCase):
    def _server(self):
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        event_log = Path(tmpdir.name) / "events.jsonl"
        return server_mod.MatrixArkMcpServer(server_mod.MatrixArkLocalAdapter(event_log), line_json=True)

    def test_jsonrpc_errors_are_specific_and_stable(self) -> None:
        server = self._server()
        invalid = server.handle({"jsonrpc": "1.0", "id": 1, "method": "tools/list"})
        self.assertEqual(-32600, invalid["error"]["code"])

        missing = server.handle({"jsonrpc": "2.0", "id": 2, "method": "unknown/method"})
        self.assertEqual(-32601, missing["error"]["code"])

        bad_params = server.handle({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": []})
        self.assertEqual(-32602, bad_params["error"]["code"])

    def test_initialize_reports_server_identity_and_version(self) -> None:
        server = self._server()
        response = server.handle({"jsonrpc": "2.0", "id": 7, "method": "initialize", "params": {}})
        self.assertEqual("matrixark-context", response["result"]["serverInfo"]["name"])
        self.assertRegex(response["result"]["serverInfo"]["version"], r"^\d+\.\d+\.\d+$")
        self.assertIn("tools", response["result"]["capabilities"])

    def test_call_tool_does_not_mutate_caller_arguments(self) -> None:
        server = self._server()
        args = {
            "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
            "agent_hook": {
                "hook_id": "hook-1",
                "hook_type": "before_llm",
                "source": "codex",
                "auto_captured": True,
                "observed_at_ms": 1,
            },
        }
        result = server.call_tool("matrixark_ingest", args)
        self.assertIn(result["status"], {"accepted", "ok"})
        self.assertIn("event_id_hash", result)
        self.assertIsInstance(result["idempotency_key_hash"], int)
        self.assertFalse(result["idempotent_replay"])
        self.assertIn("agent_hook", args)

    def test_retrieval_audit_is_off_by_default(self) -> None:
        server = self._server()
        server.call_tool("matrixark_ingest", {"messages": [{"role": "user", "content": "Alice approved the GPU budget."}]})
        pack = server.call_tool("matrixark_retrieve", {"query": "what did Alice approve?"})
        self.assertNotIn("operational_visibility_policy", pack)
        self.assertNotIn("question_type", pack)
        self.assertNotIn("recall_policy", pack)
        records = server.adapter.read_all()
        self.assertFalse(any(record.get("record_type") == "context_pack_audit" for record in records))
        self.assertFalse(any(record.get("record_type") == "context_pack_telemetry" for record in records))

    def test_storage_options_are_validated_stored_and_audited(self) -> None:
        core_modules = []
        for module_name in ("matrixark_mcp_core", "tools.matrixark_mcp_core"):
            try:
                module = importlib.import_module(module_name)
                core_modules.append((module, module.ENABLE_CONTEXT_DEBUG_RECORDS))
                module.ENABLE_CONTEXT_DEBUG_RECORDS = True
            except ModuleNotFoundError:
                pass
        server = self._server()
        try:
            ingest = server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": "Alice approved the GPU budget."}],
                    "storage_options": {"oplog_mode": "async", "raft_mode": True, "consistency": "linearizable"},
                },
            )
        finally:
            for module, old_debug in core_modules:
                module.ENABLE_CONTEXT_DEBUG_RECORDS = old_debug
        self.assertEqual("accepted", ingest["status"])
        records = server.adapter.read_all()
        event = next(record for record in records if record.get("record_type") == "context_event")
        debug = next(
            record
            for record in records
            if record.get("record_type") == "context_debug_record" and record.get("ref_hash") == event.get("event_id_hash")
        )
        storage_options = debug["debug_payload"]["storage_options"]
        self.assertEqual("async", storage_options["oplog_mode"])
        self.assertEqual("raft", storage_options["storage_mode"])
        self.assertEqual("raft", storage_options["replication_mode"])
        self.assertTrue(storage_options["raft_mode"])
        self.assertEqual("raft_async", storage_options["route"])
        self.assertEqual("raft_async", event["storage_route"]["route"])
        self.assertEqual("raft", event["storage_route"]["storage_mode"])
        self.assertEqual("async", event["storage_route"]["oplog_mode"])
        self.assertEqual("raft", event["storage_route"]["storage_family"])
        self.assertEqual("async", event["storage_route"]["write_mode"])
        self.assertTrue(event["storage_route"]["background_write"])
        self.assertEqual("ack_after_memory_append", event["storage_route"]["write_ack_policy"])
        self.assertEqual("context_node", event["context_event_parent_type"])
        self.assertNotIn("node_id", event)
        self.assertEqual(event["node_hash"], event["context_event_parent_hash"])
        self.assertNotIn("parent_type", event)
        self.assertNotIn("parent_hash", event)
        self.assertIn("context_event:context_node:", event["context_event_key"])
        self.assertRegex(event["event_time_key"], r"^\d{20}:\d+$")

        route_cases = [
            ("shared_store_async", "shared_store", "shared_store", "async", False),
            ("shared_store_sync", "shared_store", "shared_store", "sync", False),
            ("raft_async", "raft", "raft", "async", True),
            ("raft_sync", "raft", "raft", "sync", True),
        ]
        for route, storage_mode, replication_mode, oplog_mode, raft_mode in route_cases:
            normalized = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options({"storage_options": {"route": route}})
            self.assertEqual(route, normalized["route"])
            self.assertEqual(storage_mode, normalized["storage_mode"])
            self.assertEqual(replication_mode, normalized["replication_mode"])
            self.assertEqual(oplog_mode, normalized["oplog_mode"])
            self.assertEqual(oplog_mode, normalized["write_mode"])
            self.assertEqual(storage_mode, normalized["storage_family"])
            self.assertEqual(raft_mode, normalized["raft_mode"])
            self.assertEqual(route, normalized["route_key"])
            self.assertEqual(oplog_mode == "async", normalized["background_write"])
            self.assertEqual("ack_after_durable_commit" if oplog_mode == "sync" else "ack_after_memory_append", normalized["write_ack_policy"])

        friendly = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
            {
                "storage_options": {
                    "storage_family": "shared_store",
                    "write_mode": "sync",
                    "consistency": "read_your_writes",
                }
            }
        )
        self.assertEqual("shared_store_sync", friendly["route"])
        self.assertEqual("shared_store", friendly["storage_family"])
        self.assertEqual("sync", friendly["write_mode"])
        self.assertFalse(friendly["background_write"])

        top_level_alias = importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
            {
                "temporalstore_storage_family": "raft",
                "temporalstore_write_mode": "sync",
            }
        )
        self.assertEqual("raft_sync", top_level_alias["route"])
        self.assertEqual("raft", top_level_alias["storage_family"])
        self.assertEqual("sync", top_level_alias["write_mode"])
        self.assertTrue(top_level_alias["raft_mode"])

        with self.assertRaisesRegex(Exception, "background_write cannot be true"):
            importlib.import_module("tools.matrixark_mcp_core").normalize_storage_options(
                {"storage_options": {"storage_family": "raft", "write_mode": "sync", "background_write": True}}
            )

        pack = server.call_tool(
            "matrixark_retrieve",
            {
                "query": "Who approved the GPU budget?",
                "storage_options": {"storage_mode": "shared_store", "oplog_mode": "sync"},
                "audit_mode": "full",
            },
        )
        self.assertNotIn("recall_policy", pack)
        audit = next(record for record in reversed(server.adapter.read_all()) if record.get("record_type") == "context_pack_audit")
        self.assertEqual("shared_store", audit["recall_policy"]["storage_options"]["storage_mode"])
        self.assertEqual("sync", audit["storage_options"]["oplog_mode"])

    def test_invalid_storage_options_are_rejected(self) -> None:
        server = self._server()
        with self.assertRaises(Exception):
            server.call_tool(
                "matrixark_ingest",
                {
                    "messages": [{"role": "user", "content": "hello"}],
                    "storage_options": {"oplog_mode": "fast"},
                },
            )

        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        with self.assertRaisesRegex(Exception, "exactly one storage_family"):
            core_mod.normalize_storage_options(
                {"storage_options": {"storage_family": "raft", "storage_mode": "shared_store"}}
            )

    def test_live_backend_rejects_unconfigured_storage_family(self) -> None:
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")
        adapter = object.__new__(temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        adapter._supported_storage_families = {"default", "shared_store"}
        with self.assertRaisesRegex(Exception, "not configured"):
            adapter._validate_storage_routes_available(
                [{"record_type": "context_event", "storage_route": {"storage_family": "raft"}}]
            )



if __name__ == "__main__":
    unittest.main()
