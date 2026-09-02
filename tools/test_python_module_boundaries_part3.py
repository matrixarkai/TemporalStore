# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""_ModuleBoundaryPart3 methods split from test_matrixark_python_module_boundaries.MatrixArkPythonModuleBoundaryTest (mixin)."""
from __future__ import annotations

try:  # package path
    from tools.matrixark_mcp_core import *  # noqa: F401,F403
except ImportError:
    from matrixark_mcp_core import *  # noqa: F401,F403

try:  # names owned by the parent module
    from tools.test_matrixark_python_module_boundaries import (
    REPO_ROOT,
    TOOLS_DIR,
    ast,
    importlib,
)
except ImportError:
    from test_matrixark_python_module_boundaries import (
    REPO_ROOT,
    TOOLS_DIR,
    ast,
    importlib,
)


class _ModuleBoundaryPart3:
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
            "source_role_counts": {"assistant": 1},
            "source_hook_types": ["hook_boundary"],
            "source_hook_type_counts": {"hook_boundary": 1},
            "source_codex_events": ["Stop"],
            "source_codex_event_counts": {"Stop": 1},
            "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
            "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
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
        self.assertEqual(["selected_assistant_decision_outcome_only"], candidate["source_memory_selection_policies"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            candidate["source_memory_selection_policy_counts"],
        )
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

        # `assertIs` on a class cannot survive a module being loaded twice. Another file in this
        # suite drops a module from sys.modules and re-imports it to pick up an environment change,
        # which builds a second module object holding a second class of the same qualified name:
        #
        #   <class 'tools.matrixark_mcp_local_adapter.MatrixArkLocalAdapter'>
        #     is not <class 'tools.matrixark_mcp_local_adapter.MatrixArkLocalAdapter'>
        #
        # so whether these are one object depends on what ran first. It failed in 7 of 8 CI runs and
        # passed alone every time.
        #
        # What the test means is that the entrypoint re-exports these rather than defining its own
        # copies, and that holds however many times a module was loaded. A copy defined in the
        # entrypoint still fails it: its __module__ would be matrixark_mcp_server.
        for name, split_mod in (
            ("MatrixArkServiceMetrics", metrics_mod),
            ("MatrixArkLocalAdapter", local_mod),
            ("MatrixArkTemporalStoreDirectAdapter", temporal_mod),
            ("MatrixArkTemporalStoreRustAdapter", temporal_mod),
        ):
            exported = getattr(server_mod, name)
            defined = getattr(split_mod, name)
            self.assertEqual(exported.__qualname__, defined.__qualname__)
            self.assertEqual(exported.__module__.rsplit(".", 1)[-1],
                             defined.__module__.rsplit(".", 1)[-1],
                             "%s is not re-exported from the split module" % name)
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
            "Native or Rust MCP servers are future optimizations, not a v1 requirement",
            "and Rust TemporalStore remain the serving engines",
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
            "conformance scale matrix gate",
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
                    {"ref_type": "entity", "text": "profile entity", "token_estimate": 3, "memory_scope": "user_profile", "session_continuity": "cross_session"},
                    {"ref_type": "summary", "text": "cross session summary", "token_estimate": 4, "session_continuity": "cross_session"},
                ],
                "used_context_tokens": 13,
            }
        )
        self.assertNotIn("selected_refs", pack)
        self.assertNotIn("selected_ref_groups", pack)
        self.assertIn("groups", pack)
        self.assertEqual("same_session", pack["defaults"]["session_continuity"])
        self.assertEqual(2, pack["counts"]["session_continuity"]["same_session"])
        self.assertEqual("session", pack["defaults"]["memory_layer"])
        self.assertEqual(2, pack["counts"]["memory_layer"]["session"])
        self.assertEqual(1, pack["counts"]["memory_layer"]["profile"])
        self.assertEqual(1, pack["counts"]["memory_layer"]["cross_session"])
        event_group = next(group for group in pack["groups"] if group["type"] == "event")
        entity_group = next(group for group in pack["groups"] if group["type"] == "entity")
        summary_group = next(group for group in pack["groups"] if group["type"] == "summary")
        self.assertNotIn("ref_type", event_group["items"][0])
        self.assertNotIn("session_continuity", event_group["items"][0])
        self.assertNotIn("memory_layer", event_group["items"][0])
        profile_item = next(item for item in entity_group["items"] if item["text"] == "profile entity")
        self.assertEqual("profile", profile_item["memory_layer"])
        self.assertEqual("cross_session", summary_group["items"][0]["session_continuity"])
        self.assertEqual("cross_session", summary_group["items"][0]["memory_layer"])
        self.assertNotIn("tokens", event_group["items"][0])

    def test_serving_pack_hides_summary_cross_session_lineage_by_default(self) -> None:
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
        self.assertEqual("profile", item["memory_layer"])
        self.assertEqual("cross_session", item["session_continuity"])
        self.assertEqual(1, pack["counts"]["memory_layer"]["profile"])
        for field in [
            "source_session_ids",
            "source_entity_count",
            "source_roles",
            "source_hook_types",
            "source_codex_events",
            "source_entity_hashes",
            "extraction_phase",
            "final_session_boundary",
            "source_record_type",
            "segment_origin",
            "derived_from_context_events",
        ]:
            self.assertNotIn(field, item)

    def test_current_profile_entity_serving_pack_hides_provenance_by_default(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "entity",
                "text": "preference: repo path = /opt/github-services/TemporalStore",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "entity_type": "preference",
                "entity_name": "repo path",
                "profile_current_state_boost": 0.18,
                "source_session_ids": [f"session-{index}" for index in range(10)],
                "source_entity_hashes": [101, 202, 303],
                "source_roles": ["llm", "tool"],
                "source_codex_events": ["Stop", "PostToolUse"],
                "source_hook_types": ["hook_boundary"],
            }
        ]

        for compact_refs in [
            core_mod.compact_context_pack_refs,
            context_pack_mod.compact_context_pack_refs,
        ]:
            item = compact_refs(selected)[0]
            self.assertNotIn("source_session_ids", item)
            self.assertNotIn("source_entity_count", item)
            self.assertNotIn("source_roles", item)
            self.assertNotIn("source_codex_events", item)
            self.assertNotIn("source_hook_types", item)
            self.assertNotIn("source_entity_hashes", item)

            debug_item = compact_refs(selected, include_debug=True)[0]
            self.assertEqual([f"session-{index}" for index in range(8)], debug_item["source_session_ids"])
            self.assertEqual(3, debug_item["source_entity_count"])
            self.assertEqual(["assistant", "tool"], debug_item["source_roles"])
            self.assertEqual(["Stop", "PostToolUse"], debug_item["source_codex_events"])
            self.assertEqual(["hook_boundary"], debug_item["source_hook_types"])
            self.assertNotIn("source_entity_hashes", debug_item)

    def test_pending_async_event_serving_pack_uses_live_memory_layer_without_lineage(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "event",
                "text": "user: remember this live hook message",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "event_type": "pending_async",
                "classification": "PENDING_ASYNC_EXTRACTION",
                "extraction_phase": "pending_async",
                "source_record_type": "context_event",
                "segment_origin": "live_hook",
                "derived_from_context_events": True,
                "source_roles": ["user"],
                "source_hook_types": ["before_llm"],
                "source_codex_events": ["UserPromptSubmit"],
                "source_role_counts": {"user": 1},
            }
        ]

        for pack_groups in [
            core_mod.serving_ref_groups_for_pack,
            lambda refs: context_pack_mod.serving_ref_groups_for_pack(refs, include_debug=False),
        ]:
            groups = pack_groups(selected)
            item = groups[0]["items"][0]

            self.assertEqual("pending_async", item["memory_layer"])
            self.assertEqual("session", item["memory_scope"])
            self.assertNotIn("extraction_phase", item)
            self.assertNotIn("source_record_type", item)
            self.assertNotIn("segment_origin", item)
            self.assertNotIn("derived_from_context_events", item)
            self.assertNotIn("source_roles", item)
            self.assertNotIn("source_hook_types", item)
            self.assertNotIn("source_codex_events", item)
        self.assertEqual({"pending_async": 1}, core_mod.memory_layer_counts(selected))
        self.assertEqual({"pending_async": 1}, context_pack_mod._memory_layer_counts(selected))

    def test_profile_entity_serving_pack_keeps_profile_layer_without_lineage(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "entity",
                "context_class": "profile_entity",
                "text": "repo preference: use /opt/github-services for TemporalStore work",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "entity_type": "preference",
                "entity_name": "repo location",
                "source_session_ids": ["codex:thread-a", "codex:thread-b"],
                "source_roles": ["user", "llm"],
                "source_hook_types": ["before_llm", "stop"],
            }
        ]

        for pack_groups in [
            core_mod.serving_ref_groups_for_pack,
            lambda refs: context_pack_mod.serving_ref_groups_for_pack(refs, include_debug=False),
        ]:
            item = pack_groups(selected)[0]["items"][0]

            self.assertEqual("profile", item["memory_layer"])
            self.assertEqual("user_profile", item["memory_scope"])
            self.assertEqual("cross_session", item["session_continuity"])
            self.assertEqual("repo location", item["entity"])
            self.assertNotIn("source_session_ids", item)
            self.assertNotIn("source_roles", item)
            self.assertNotIn("source_hook_types", item)
        self.assertEqual({"profile": 1}, core_mod.memory_layer_counts(selected))
        self.assertEqual({"profile": 1}, context_pack_mod._memory_layer_counts(selected))

    def test_serving_pack_debug_refs_include_lifecycle_lineage(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        context_pack_mod = importlib.import_module("tools.matrixark_mcp_context_pack")
        selected = [
            {
                "ref_type": "summary",
                "text": "profile summary after stop boundary",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "source_record_type": "context_summary",
                "segment_origin": "batch_extract",
                "derived_from_context_events": True,
                "source_event_ids": [11, 22],
            }
        ]

        for compact_refs in [
            core_mod.compact_context_pack_refs,
            context_pack_mod.compact_context_pack_refs,
        ]:
            item = compact_refs(selected)[0]
            self.assertNotIn("extraction_phase", item)
            self.assertNotIn("final_session_boundary", item)
            self.assertNotIn("source_record_type", item)
            debug_item = compact_refs(selected, include_debug=True)[0]
            self.assertEqual("final", debug_item["extraction_phase"])
            self.assertTrue(debug_item["final_session_boundary"])
            self.assertEqual("context_summary", debug_item["source_record_type"])
            self.assertEqual([11, 22], debug_item["source_event_ids"])

    def test_hot_context_event_records_are_first_class_session_memory(self) -> None:
        records_mod = importlib.import_module("tools.matrixark_mcp_ingest_message_records")
        envelope = {
            "kind": "message",
            "messages": [
                {
                    "role": "user",
                    "content": "why is context event missing from serving memory?",
                }
            ],
            "scope": {
                "tenant_id": "tenant_hot",
                "user_id": "user_hot",
                "session_id": "codex:thread-hot",
            },
            "metadata": {
                "source_hook_type_counts": {"before_llm": 1},
                "source_codex_event_counts": {"UserPromptSubmit": 1},
                "source_memory_selection_policy_counts": {"selected_user_prompt": 1},
            },
            "ingestion_time_ms": 1780000000000,
        }
        extraction = {
            "classification": "PENDING_ASYNC_EXTRACTION",
            "event_type": "pending_async",
            "status": "observed",
        }

        event = records_mod.context_event_record(
            event_id_hash=101,
            node_hash=202,
            node_path=["tenant:tenant_hot", "user:user_hot", "session:codex:thread-hot"],
            text="user: why is context event missing from serving memory?",
            extraction=extraction,
            envelope=envelope,
            prior_context={},
            hook={"hook_type": "before_llm", "codex_event": "UserPromptSubmit"},
        )
        index_terms = records_mod.context_event_index_terms(
            extraction=extraction,
            text=event["text"],
            envelope=envelope,
        )
        indexes = records_mod.context_event_index_records(
            index_terms=index_terms,
            event_id_hash=101,
            node_hash=202,
            scope=envelope["scope"],
            updated_at_ms=envelope["ingestion_time_ms"],
        )

        self.assertEqual("context_event", event["record_type"])
        self.assertEqual("session", event["memory_scope"])
        self.assertEqual("same_session", event["session_continuity"])
        self.assertEqual("pending_async", event["extraction_phase"])
        self.assertEqual(envelope["scope"], event["scope"])
        self.assertEqual(envelope["scope"], event["access_scope"])
        self.assertEqual({"user": 1}, event["source_role_counts"])
        self.assertEqual({"before_llm": 1}, event["source_hook_type_counts"])
        self.assertEqual({"UserPromptSubmit": 1}, event["source_codex_event_counts"])
        self.assertEqual(1780000000000, event["updated_at_ms"])
        self.assertIn("memory_scope:session", index_terms)
        self.assertIn("session_continuity:same_session", index_terms)
        self.assertIn("source_role:user", index_terms)
        self.assertIn("hook_type:before_llm", index_terms)
        self.assertIn("codex_event:userpromptsubmit", index_terms)
        self.assertIn("memory_selection_policy:selected_user_prompt", index_terms)
        self.assertTrue(all(record["memory_scope"] == "session" for record in indexes))
        self.assertTrue(all(record["session_continuity"] == "same_session" for record in indexes))
        self.assertTrue(all(record["extraction_phase"] == "pending_async" for record in indexes))

    def test_shared_pack_builder_exposes_memory_layer_budget_and_pressure(self) -> None:
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
            source_role_budget_tokens={"tool": 32},
            source_role_budget_mode="auto",
            memory_layer_budget_tokens={"profile_entity": 48},
            memory_layer_budget_mode="auto",
            memory_selection_policy_budget_tokens={"selected_tool_evidence_only": 40},
            memory_selection_policy_budget_mode="auto",
            debug_refs=False,
        )
        source_role_policy = pack["recall_policy"]["source_role_budget_policy"]
        self.assertTrue(source_role_policy["enabled"])
        self.assertEqual("auto", source_role_policy["mode"])
        self.assertEqual(100, source_role_policy["remote_budget_tokens"])
        self.assertTrue(source_role_policy["derived"])
        self.assertTrue(source_role_policy["independent_caps"])
        self.assertTrue(source_role_policy["global_remote_budget_enforced"])
        self.assertEqual("independent_per_role_caps_under_global_remote_budget", source_role_policy["budget_semantics"])
        self.assertEqual({"tool": 32}, source_role_policy["budget_tokens"])
        memory_policy = pack["recall_policy"]["memory_layer_budget_policy"]
        self.assertTrue(memory_policy["enabled"])
        self.assertEqual("auto", memory_policy["mode"])
        self.assertEqual("fact", memory_policy["question_type"])
        self.assertEqual(100, memory_policy["remote_budget_tokens"])
        self.assertEqual({"profile_entity": 48}, memory_policy["budget_tokens"])
        selection_policy = pack["recall_policy"]["memory_selection_policy_budget_policy"]
        self.assertTrue(selection_policy["enabled"])
        self.assertEqual("auto", selection_policy["mode"])
        self.assertEqual(100, selection_policy["remote_budget_tokens"])
        self.assertTrue(selection_policy["independent_caps"])
        self.assertTrue(selection_policy["global_remote_budget_enforced"])
        self.assertEqual(
            "independent_per_memory_selection_policy_caps_under_global_remote_budget",
            selection_policy["budget_semantics"],
        )
        self.assertEqual({"selected_tool_evidence_only": 40}, selection_policy["budget_tokens"])
        layer_budget = pack["recall_policy"]["memory_layer_budget"]
        self.assertEqual(1, layer_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, layer_budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, layer_budget["by_ref_type"]["entity"]["refs"])
        self.assertEqual(1, layer_budget["by_entity_type"]["tool_evidence"]["refs"])
        self.assertEqual(1, layer_budget["by_source_role"]["tool"]["refs"])
        self.assertEqual(1, layer_budget["by_hook_type"]["hook_boundary"]["refs"])
        self.assertEqual(1, layer_budget["by_codex_event"]["PostToolUse"]["refs"])
        self.assertEqual(1, layer_budget["final_session_boundary_ref_count"])
        layer_pressure = pack["recall_policy"]["memory_layer_pressure"]
        self.assertEqual(1, layer_pressure["selected_refs"])
        self.assertEqual(8, layer_pressure["selected_tokens"])
        self.assertEqual(0, layer_pressure["dropped_refs"])
        self.assertEqual(0, layer_pressure["dropped_tokens"])
        self.assertFalse(layer_pressure["profile_memory_pressure"])
        self.assertFalse(layer_pressure["cross_session_pressure"])
        self.assertIn("by_memory_scope", layer_pressure["by_dimension"])
        self.assertEqual(
            1,
            layer_pressure["by_dimension"]["by_memory_scope"]["user_profile"]["selected_refs"],
        )

        metrics_mod.attach_python_retrieval_metrics(
            pack,
            {},
            stage_latencies_ms={},
            retrieval_scan_stats={},
            selected=selected,
            dropped_over_budget={},
            records=[],
        )
        metrics_budget = pack["retrieval_metrics"]["memory_layer_budget"]
        self.assertEqual(1, metrics_budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, metrics_budget["by_entity_type"]["tool_evidence"]["refs"])
        self.assertNotIn("by_source_role", metrics_budget)
        self.assertNotIn("by_hook_type", metrics_budget)
        self.assertNotIn("by_codex_event", metrics_budget)
        self.assertNotIn("source_message_counts_by_role", metrics_budget)
        metrics_pressure = pack["retrieval_metrics"]["memory_layer_pressure"]
        self.assertEqual(1, metrics_pressure["selected_refs"])
        self.assertNotIn("by_source_role", metrics_pressure.get("by_dimension", {}))
        self.assertNotIn("tool_source_message_pressure", metrics_pressure)

    def test_modular_budget_pack_caps_memory_selection_policy(self) -> None:
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        candidates = [
            {
                "ref_type": "entity",
                "ref_hash": 101,
                "text": "assistant decided to push the rollout after validation",
                "score": 0.99,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_memory_selection_policy_counts": {
                    "selected_assistant_decision_outcome_only": 1
                },
            },
            {
                "ref_type": "entity",
                "ref_hash": 202,
                "text": "tool evidence shows validation passed",
                "score": 0.8,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "source_memory_selection_policy_counts": {
                    "selected_tool_evidence_only": 1
                },
            },
        ]

        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            candidates,
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            max_selected_refs=2,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 2,
                "max_candidates": 2,
            },
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 1
            },
            budget_fill_policy="quality_first",
        )

        self.assertGreater(used_tokens, 0)
        self.assertEqual([202], [ref["ref_hash"] for ref in selected])
        self.assertEqual(["selected_tool_evidence_only"], selected[0]["budget_memory_selection_policies"])
        self.assertEqual(1, dropped["memory_selection_policy_budget"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 1},
            dropped["memory_selection_policy_budget_policy"]["budget_tokens"],
        )
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 0},
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"],
        )
        self.assertEqual(
            8,
            dropped["estimated_tokens"]["memory_selection_policy_budget"],
        )

        metadata_only_candidate = {
            "ref_type": "event",
            "ref_hash": 303,
            "text": "assistant pending decision should obey policy caps from metadata",
            "score": 0.95,
            "memory_scope": "session",
            "session_continuity": "same_session",
            "metadata": {
                "source_memory_selection_policy_counts": {
                    "selected_assistant_decision_outcome_only": 1,
                },
            },
        }
        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [metadata_only_candidate],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            max_selected_refs=1,
            min_score=0.0,
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 1,
            },
            budget_fill_policy="quality_first",
        )
        self.assertEqual([], selected)
        self.assertEqual(0, used_tokens)
        self.assertEqual(1, dropped["memory_selection_policy_budget"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 0},
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"],
        )

        metadata_pending_async = {
            "ref_hash": 404,
            "text": "fresh pending assistant decision should obey pending async layer caps",
            "score": 0.96,
            "metadata": {
                "ref_type": "event",
                "event_type": "pending_async",
                "memory_scope": "session",
                "session_continuity": "same_session",
            },
        }
        self.assertEqual("pending_async_event", core_mod.candidate_memory_layer_name(metadata_pending_async))
        self.assertEqual("pending_async_event", budget_mod.candidate_memory_layer_name(metadata_pending_async))
        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [metadata_pending_async],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            max_selected_refs=1,
            min_score=0.0,
            memory_layer_budget_tokens={"pending_async_event": 1},
            budget_fill_policy="quality_first",
        )
        self.assertEqual([], selected)
        self.assertEqual(0, used_tokens)
        self.assertEqual(1, dropped["memory_layer_budget"])
        self.assertEqual(
            {"pending_async_event": 0},
            dropped["memory_layer_budget_policy"]["selected_tokens_by_layer"],
        )

        metadata_role_candidate = {
            "ref_type": "entity",
            "ref_hash": 505,
            "text": "assistant decision from metadata role lineage should obey assistant caps",
            "score": 0.97,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "metadata": {
                "entity_type": "assistant_decision",
                "source_role_counts": {"llm": 2, "model": 1},
            },
        }
        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [metadata_role_candidate],
            [],
            max_context_tokens=40,
            auxiliary_quota=0,
            max_selected_refs=1,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 40,
                "max_sessions": 1,
                "max_candidates": 1,
            },
            source_role_budget_tokens={"assistant": 1},
            budget_fill_policy="quality_first",
        )
        self.assertEqual([], selected)
        self.assertEqual(0, used_tokens)
        self.assertEqual(1, dropped["source_role_budget"])
        self.assertEqual(
            {"assistant": 0},
            dropped["source_role_budget_policy"]["selected_tokens_by_role"],
        )

    def test_modular_budget_pack_force_fill_respects_memory_selection_policy_cap(self) -> None:
        budget_mod = importlib.import_module("tools.matrixark_mcp_budget_pack")
        assistant_candidate = {
            "ref_type": "summary",
            "ref_hash": 303,
            "text": "assistant decision rollout evidence should not bypass retrieval caps",
            "score": 0.99,
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
        }

        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [assistant_candidate],
            [],
            max_context_tokens=8,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 8,
                "max_sessions": 1,
                "max_candidates": 1,
            },
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 1
            },
            budget_fill_policy="force_fill",
        )

        self.assertEqual([], selected)
        self.assertEqual(0, used_tokens)
        self.assertEqual(1, dropped["cross_session_budget"])
        self.assertEqual(
            {"selected_assistant_decision_outcome_only": 0},
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"],
        )

        selected, used_tokens, dropped = budget_mod.select_token_budgeted_refs(
            [assistant_candidate],
            [],
            max_context_tokens=8,
            auxiliary_quota=0,
            min_score=0.0,
            cross_session_policy={
                "enabled": True,
                "budget_tokens": 8,
                "max_sessions": 1,
                "max_candidates": 1,
            },
            memory_selection_policy_budget_tokens={
                "selected_assistant_decision_outcome_only": 8
            },
            budget_fill_policy="force_fill",
        )

        self.assertEqual([303], [ref["ref_hash"] for ref in selected])
        self.assertEqual(
            used_tokens,
            dropped["memory_selection_policy_budget_policy"]["selected_tokens_by_policy"][
                "selected_assistant_decision_outcome_only"
            ],
        )
        self.assertEqual(
            ["selected_assistant_decision_outcome_only"],
            selected[0]["budget_memory_selection_policies"],
        )

    def test_shared_pack_builder_reports_profile_shadowed_dropped_budget(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_retrieve_metrics")
        selected = [
            {
                "ref_type": "entity",
                "ref_hash": 22,
                "text": "decision: shared repo path = /opt/github-services/TemporalStore-memory-next",
                "token_estimate": 9,
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "entity_type": "decision",
                "extraction_phase": "final",
                "source_session_ids": ["session-a", "session-b"],
                "source_entity_hashes": [11, 12],
                "profile_current_state_representative": True,
                "current_state_policy": "profile_entity_bridge_preferred_over_session_local_history",
                "current_state_source_session_count": 2,
                "current_state_source_entity_count": 2,
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
                    "entity_type": "tool_evidence",
                    "source_roles": ["tool"],
                    "source_hook_types": ["tool_result"],
                    "source_codex_events": ["PostToolUse"],
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
        self.assertTrue(serving_selected[0]["profile_current_state_representative"])
        for field in [
            "current_state_policy",
            "current_state_source_session_count",
            "current_state_source_entity_count",
            "source_entity_count",
        ]:
            self.assertNotIn(field, serving_selected[0])
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
        pressure = pack["recall_policy"]["memory_layer_pressure"]
        self.assertEqual(1, dropped_budget["stale_ref_count"])
        self.assertEqual(6, dropped_budget["stale_token_estimate"])
        self.assertEqual(1, dropped_budget["profile_shadowed_ref_count"])
        self.assertEqual(6, dropped_budget["profile_shadowed_token_estimate"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_drop_reason"]["stale"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_memory_scope"]["session"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_session_continuity"]["same_session"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_entity_type"]["tool_evidence"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_source_role"]["tool"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_hook_type"]["tool_result"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_codex_event"]["PostToolUse"])
        self.assertEqual({"refs": 1, "tokens": 6}, dropped_budget["by_profile_shadowed_reason"]["source_entity_lineage"])
        self.assertEqual(1, pressure["selected_refs"])
        self.assertEqual(9, pressure["selected_tokens"])
        self.assertEqual(1, pressure["dropped_refs"])
        self.assertEqual(6, pressure["dropped_tokens"])
        self.assertTrue(pressure["session_memory_pressure"])
        self.assertFalse(pressure["profile_memory_pressure"])
        self.assertFalse(pressure["cross_session_pressure"])
        self.assertIn("by_memory_scope", pressure["dropped_dimensions"])
        self.assertEqual(
            1,
            pressure["by_dimension"]["by_memory_scope"]["session"]["dropped_refs"],
        )

        metrics_mod.attach_python_retrieval_metrics(
            pack,
            {},
            stage_latencies_ms={},
            retrieval_scan_stats={},
            selected=selected,
            dropped_over_budget=serving_dropped,
            records=[],
        )
        metrics_dropped_budget = pack["retrieval_metrics"]["dropped_memory_layer_budget"]
        self.assertEqual(1, metrics_dropped_budget["by_memory_scope"]["session"]["refs"])
        self.assertEqual(1, metrics_dropped_budget["by_profile_shadowed_reason"]["source_entity_lineage"]["refs"])
        self.assertNotIn("by_source_role", metrics_dropped_budget)
        self.assertNotIn("by_hook_type", metrics_dropped_budget)
        self.assertNotIn("by_codex_event", metrics_dropped_budget)
        metrics_pressure = pack["retrieval_metrics"]["memory_layer_pressure"]
        self.assertEqual(1, metrics_pressure["dropped_refs"])
        self.assertNotIn("by_source_role", metrics_pressure.get("by_dimension", {}))
        self.assertNotIn("tool_source_message_pressure", metrics_pressure)

    def test_memory_layer_pressure_reports_pending_async_event_drops(self) -> None:
        builder_mod = importlib.import_module("tools.matrixark_mcp_retrieve_pack_builder")

        pressure = builder_mod.memory_layer_pressure_summary(
            {"total_selected_refs": 1, "by_memory_layer": {"pending_async_event": {"refs": 1, "tokens": 5}}},
            {"total_dropped_refs": 1, "by_memory_layer": {"pending_async_event": {"refs": 1, "tokens": 7}}},
        )

        self.assertTrue(pressure["pending_async_event_pressure"])
        self.assertIn("by_memory_layer", pressure["dropped_dimensions"])
        self.assertEqual(1, pressure["by_dimension"]["by_memory_layer"]["pending_async_event"]["dropped_refs"])

    def test_deadline_fallback_hides_memory_layer_lineage_by_default(self) -> None:
        deadline_mod = importlib.import_module("tools.matrixark_mcp_deadline_pack")

        class Adapter:
            def __init__(self) -> None:
                self.audit_records = []

            def append_audit(self, record):
                self.audit_records.append(record)

        adapter = Adapter()
        scope = {
            "account_id": "acct_deadline",
            "tenant_id": "tenant_deadline",
            "user_id": "user_deadline",
            "session_id": "session_now",
        }
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 1001,
                "scope": scope,
                "text": "assistant decided to push the local recovery gate after tests passed.",
                "memory_scope": "session",
                "session_continuity": "same_session",
                "event_type": "pending_async",
                "extraction_phase": "pending_async",
                "source_roles": ["llm"],
                "source_role_counts": {"llm": 1},
                "source_hook_types": ["hook_boundary"],
                "source_hook_type_counts": {"hook_boundary": 1},
                "source_codex_events": ["Stop"],
                "source_codex_event_counts": {"Stop": 1},
                "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 1},
            },
            {
                "record_type": "context_entity",
                "entity_hash": 2002,
                "scope": {
                    "account_id": "acct_deadline",
                    "tenant_id": "tenant_deadline",
                    "user_id": "user_deadline",
                },
                "entity_type": "assistant_decision",
                "entity_name": "local_recovery_gate",
                "state": "The user wants recovery and retrieval gates to fail closed.",
                "memory_scope": "user_profile",
                "session_continuity": "cross_session",
                "extraction_phase": "final",
                "final_session_boundary": True,
                "source_roles": ["model", "assistant"],
                "source_role_counts": {"model": 2, "assistant": 1},
                "source_hook_types": ["hook_boundary"],
                "source_hook_type_counts": {"hook_boundary": 3},
                "source_codex_events": ["Stop"],
                "source_codex_event_counts": {"Stop": 3},
                "source_memory_selection_policies": ["selected_assistant_decision_outcome_only"],
                "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 2},
                "source_session_ids": ["session_old", "session_now"],
                "source_event_ids": [1001],
                "source_entity_hashes": [10, 11],
            },
            {
                "record_type": "matrixark_async_pipeline_task",
                "task_hash": 3003,
                "event_id_hash": 1001,
                "scope": {
                    "account_id": "acct_deadline",
                    "tenant_id": "tenant_deadline",
                    "user_id": "user_deadline",
                },
                "status": "extraction_committed",
                "completed_stages": ["extraction"],
                "remaining_stages": ["summary", "compression", "embedding"],
                "trigger_policy": "threshold",
                "updated_at_ms": 100,
            },
        ]

        pack = deadline_mod.deadline_fallback_pack(
            adapter,
            query="what recovery gate did we decide?",
            scope=scope,
            question_type="current_state",
            max_context_tokens=96,
            local_budget={"items": [{"ref": "visible-buffer", "text": "local context"}], "token_estimate": 12, "safety_margin_tokens": 4},
            deadline_ms=1,
            elapsed_ms=2.0,
            records=records,
            reason="deadline_after_record_load",
            budget_source="test",
            source_role_budget_tokens={"assistant": 40},
            source_role_budget_mode="auto",
            memory_layer_budget_tokens={"profile_entity": 48},
            memory_layer_budget_mode="auto",
            memory_selection_policy_budget_tokens={"selected_assistant_decision_outcome_only": 56},
            memory_selection_policy_budget_mode="auto",
        )

        self.assertTrue(pack["partial"])
        self.assertLessEqual(pack["tokens"]["remote"], pack["tokens"]["remote_budget"])
        self.assertEqual(80, pack["tokens"]["remote_budget"])
        self.assertEqual(1, pack["counts"]["refs"]["entity"])
        self.assertEqual(0, pack["counts"]["refs"].get("event", 0))
        self.assertEqual(1, pack["counts"]["session_continuity"]["cross_session"])
        for field in [
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "async_pipeline_readiness",
            "pre_retrieval_summary_refresh",
        ]:
            self.assertNotIn(field, pack)
        audit_record = adapter.audit_records[0]
        budget = audit_record["memory_layer_budget"]
        self.assertEqual(1, budget["by_memory_scope"]["user_profile"]["refs"])
        self.assertEqual(1, budget["by_session_continuity"]["cross_session"]["refs"])
        self.assertEqual(1, budget["by_entity_type"]["assistant_decision"]["refs"])
        self.assertNotIn("pending_async_event", budget.get("by_memory_layer", {}))
        for field in [
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
            "by_memory_selection_policy",
        ]:
            self.assertNotIn(field, budget)
        self.assertEqual(1, budget["final_session_boundary_ref_count"])
        pressure = audit_record["memory_layer_pressure"]
        self.assertEqual(1, pressure["selected_refs"])
        self.assertEqual(pack["tokens"]["remote"], pressure["selected_tokens"])
        self.assertEqual(0, pressure["dropped_refs"])
        self.assertFalse(pressure["profile_memory_pressure"])
        self.assertFalse(pressure["cross_session_pressure"])
        self.assertEqual(1, pressure["by_dimension"]["by_memory_scope"]["user_profile"]["selected_refs"])
        self.assertNotIn("by_source_role", pressure["by_dimension"])
        self.assertNotIn("assistant_source_message_pressure", pressure)
        self.assertEqual(1, pressure["by_dimension"]["by_session_continuity"]["cross_session"]["selected_refs"])
        readiness = audit_record["async_pipeline_readiness"]
        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertEqual(1, readiness["task_count"])
        self.assertEqual(1, readiness["extraction_committed_task_count"])
        self.assertEqual(["compression", "embedding", "summary"], readiness["remaining_stages"])
        self.assertNotIn("memory_hierarchy", pack)
        self.assertNotIn("retrieval_metrics", pack)
        self.assertIn("async_pipeline_followup_pending", pack["warnings"])
        self.assertIn("pending_async_event_superseded_by_extracted_refs:1", pack["warnings"])
        entity_group = next(group for group in pack["groups"] if group["type"] == "entity")
        entity_item = entity_group["items"][0]
        self.assertEqual("user_profile", entity_item["memory_scope"])
        self.assertEqual("cross_session", pack["defaults"]["session_continuity"])
        self.assertNotIn("session_continuity", entity_item)
        for field in [
            "source_session_ids",
            "source_entity_count",
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
        ]:
            self.assertNotIn(field, entity_item)
        self.assertEqual(1, len(adapter.audit_records))
        self.assertEqual(budget, audit_record["memory_layer_budget"])
        self.assertEqual(pressure, audit_record["memory_layer_pressure"])
        self.assertEqual(readiness, audit_record["async_pipeline_readiness"])
        self.assertNotIn("memory_hierarchy", audit_record)
        policy_budget = audit_record["recall_policy_summary"]["memory_selection_policy_budget"]
        self.assertTrue(policy_budget["enabled"])
        self.assertEqual("auto", policy_budget["mode"])
        self.assertEqual(80, policy_budget["remote_budget_tokens"])
        self.assertEqual(56, policy_budget["budget_tokens"]["selected_assistant_decision_outcome_only"])
        self.assertEqual(
            1,
            policy_budget["selected_ref_count_by_policy"]["selected_assistant_decision_outcome_only"],
        )
        self.assertEqual(pressure, audit_record["recall_policy_summary"]["memory_layer_pressure"])
        self.assertEqual(readiness, audit_record["recall_policy_summary"]["async_pipeline_readiness"])
        self.assertEqual(
            1,
            audit_record["recall_policy_summary"]["session_continuity"]["cross_session_selected_ref_count"],
        )
        self.assertTrue(audit_record["partial_context_pack"])
        self.assertEqual(1, audit_record["selected_ref_counts"]["entity"])
        self.assertIn("retrieval_deadline_exceeded:deadline_after_record_load", audit_record["quality_warnings"])
        self.assertIn("async_pipeline_followup_pending", audit_record["quality_warnings"])
        self.assertIn("pending_async_event_superseded_by_extracted_refs:1", audit_record["quality_warnings"])

    def test_async_readiness_normalizes_llm_role_aliases_to_assistant(self) -> None:
        readiness_mod = importlib.import_module("tools.matrixark_mcp_async_readiness")
        scope = {
            "account_id": "acct_alias_readiness",
            "tenant_id": "tenant_alias_readiness",
            "user_id": "user_alias_readiness",
            "session_id": "session_alias_readiness",
        }
        readiness = readiness_mod.async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 1,
                    "scope": scope,
                    "status": "extraction_committed",
                    "remaining_stages": ["summary"],
                    "source_roles": ["llm", "model", "assistant"],
                    "memory_layers_written": {
                        "session_entities": 1,
                        "profile_entities": 1,
                        "same_session_entities": 1,
                        "cross_session_entities": 1,
                    },
                }
            ],
            {**scope, "_session_scope": "prefer"},
        )

        self.assertEqual({"assistant": 3}, readiness["pending_source_roles"])
        self.assertNotIn("llm", readiness["pending_source_roles"])
        self.assertNotIn("model", readiness["pending_source_roles"])
        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertIn("summary", readiness["remaining_stages"])
        layer_readiness = readiness["memory_layer_readiness"]
        self.assertIn("user_profile", layer_readiness["blocked_layers"])
        self.assertIn("cross_session", layer_readiness["blocked_layers"])
        self.assertIn("summary", layer_readiness["blocked_layers"])

    def test_async_readiness_uses_source_count_maps_before_list_fallback(self) -> None:
        readiness_mod = importlib.import_module("tools.matrixark_mcp_async_readiness")
        scope = {
            "account_id": "acct_count_readiness",
            "tenant_id": "tenant_count_readiness",
            "user_id": "user_count_readiness",
            "session_id": "session_count_readiness",
        }
        readiness = readiness_mod.async_pipeline_retrieval_readiness(
            [
                {
                    "record_type": "matrixark_async_pipeline_task",
                    "task_hash": 1,
                    "scope": scope,
                    "status": "extraction_committed",
                    "remaining_stages": ["summary"],
                    "source_role_counts": {"llm": 2, "model": 1, "assistant": 1, "tool": 2, "": 10},
                    "source_roles": ["user"],
                    "source_hook_type_counts": {"after_llm": 3, "hook_boundary": 1},
                    "source_hook_types": ["prompt_submit"],
                    "source_codex_event_counts": {"Stop": 2, "PostToolUse": 1},
                    "source_codex_events": ["UserPromptSubmit"],
                    "source_memory_selection_policy_counts": {
                        "selected_assistant_decision_outcome_only": 2,
                        "selected_tool_evidence_only": 1,
                    },
                    "source_memory_selection_policies": ["selected_user_prompt"],
                    "memory_layers_written": {
                        "session_entities": 1,
                        "profile_entities": 1,
                        "same_session_entities": 1,
                        "cross_session_entities": 1,
                    },
                }
            ],
            {**scope, "_session_scope": "prefer"},
        )

        self.assertEqual({"assistant": 4, "tool": 2}, readiness["pending_source_roles"])
        self.assertNotIn("user", readiness["pending_source_roles"])
        self.assertNotIn("llm", readiness["pending_source_roles"])
        self.assertNotIn("model", readiness["pending_source_roles"])
        self.assertEqual({"after_llm": 3, "hook_boundary": 1}, readiness["pending_source_hook_types"])
        self.assertNotIn("prompt_submit", readiness["pending_source_hook_types"])
        self.assertEqual({"PostToolUse": 1, "Stop": 2}, readiness["pending_source_codex_events"])
        self.assertNotIn("UserPromptSubmit", readiness["pending_source_codex_events"])
        self.assertEqual(
            {
                "selected_assistant_decision_outcome_only": 2,
                "selected_tool_evidence_only": 1,
            },
            readiness["pending_memory_selection_policies"],
        )
        self.assertNotIn("selected_user_prompt", readiness["pending_memory_selection_policies"])

    def test_compact_serving_pack_exposes_async_summary_readiness(self) -> None:
        core_mod = importlib.import_module("tools.matrixark_mcp_core")
        pack = {
            "context_pack_id": "pack-summary-stale",
            "selected_refs": [
                {
                    "ref_type": "entity",
                    "text": "assistant decision: keep profile summaries visible",
                    "memory_scope": "user_profile",
                    "session_continuity": "cross_session",
                    "source_roles": ["llm", "model", "tool"],
                    "source_role_counts": {"llm": 1, "model": 1, "tool": 1},
                    "source_hook_types": ["hook_boundary"],
                    "source_hook_type_counts": {"hook_boundary": 3},
                    "source_codex_events": ["Stop"],
                    "source_codex_event_counts": {"Stop": 3},
                    "source_session_ids": ["session_summary_1", "session_summary_2"],
                    "source_entity_hashes": [101, 102],
                    "extraction_context_event_ids": [201, 202, 203],
                    "source_memory_selection_policy_counts": {"selected_assistant_decision_outcome_only": 2},
                    "source_memory_selection_lossy_count": 1,
                    "source_memory_selection_complete_count": 2,
                    "source_memory_selection_dropped_text_chars": 120,
                    "source_memory_selection_dropped_line_count": 3,
                    "source_memory_selection_retained_text_ratio_avg": 0.75,
                    "source_memory_selection_retained_line_ratio_avg": 0.8,
                    "profile_promotion_policy": "always_when_profile_scope_available",
                    "profile_promotion_blocker": "",
                    "profile_revision": 3,
                    "previous_profile_revision": 2,
                    "previous_profile_updated_at_ms": 123456,
                    "supersedes_session_entity_hash": 301,
                    "supersedes_session_entity_hashes": [301, 302, 303],
                    "current_state_policy": "merged_profile_state",
                    "source_lineage": {"source_role_counts": {"assistant": 1}},
                }
            ],
            "context_pack_payload_policy": {"enable_debug_refs_with": "include_debug_refs=true"},
            "operational_visibility_policy": {"audit_mode": "sampled"},
            "recall_policy": {
                "session_continuity": {
                    "mode": "prefer",
                    "policy": "same-session first, profile entity bridge second",
                },
                "cross_session": {
                    "enabled": True,
                    "budget_tokens": 64,
                    "remote_budget_tokens": 100,
                    "computed_budget_tokens": 64,
                    "budget_floor_tokens": 256,
                    "budget_floor_applied": False,
                    "budget_floor_status": "remote_budget_too_small_for_profile_floor",
                    "max_sessions": 3,
                    "max_candidates": 24,
                },
                "shared_context": {"enabled": False},
                "async_pipeline_readiness": {
                    "task_count": 2,
                    "ready_for_retrieval": False,
                    "remaining_stages": ["summary", "compression"],
                    "remaining_stage_counts": {"summary": 1, "compression": 1},
                    "pending_memory_scopes": {"user_profile": 1},
                    "pending_session_continuities": {"cross_session": 1},
                    "freshness_warnings": ["profile_summary_stale"],
                },
                "memory_layer_budget": {
                    "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 7}},
                    "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 7}},
                    "by_source_role": {
                        "llm": {"refs": 1, "tokens": 3},
                        "model": {"refs": 1, "tokens": 4},
                        "tool": {"refs": 1, "tokens": 2},
                    },
                    "source_message_counts_by_role": {"llm": 1, "model": 1, "tool": 1},
                    "source_hook_counts_by_type": {"hook_boundary": 3},
                    "source_codex_event_counts_by_event": {"Stop": 3},
                },
                "memory_layer_pressure": {
                    "selected_refs": 1,
                    "selected_tokens": 7,
                    "dropped_refs": 1,
                    "dropped_tokens": 13,
                    "profile_memory_pressure": True,
                    "cross_session_pressure": True,
                    "assistant_source_message_pressure": True,
                },
            },
        }
        compact = core_mod.compact_context_pack_for_serving_flat(pack, include_debug=False)
        grouped_compact = core_mod.compact_context_pack_for_serving(pack, include_debug=False)
        debug_compact = core_mod.compact_context_pack_for_serving_flat(pack, include_debug=True)

        hidden_fragments = ("debug", "lineage")
        hidden_fields = {
            "current_state_policy",
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_session_ids",
            "source_entity_count",
            "source_lineage",
            "context_pack_payload_policy",
            "operational_visibility_policy",
            "memory_hierarchy",
        }

        def assert_no_default_debug_lineage(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    self.assertNotIn(key, hidden_fields)
                    lowered = str(key).lower()
                    self.assertFalse(any(fragment in lowered for fragment in hidden_fragments), key)
                    assert_no_default_debug_lineage(item)
            elif isinstance(value, list):
                for item in value:
                    assert_no_default_debug_lineage(item)

        for default_pack in [compact, grouped_compact]:
            assert_no_default_debug_lineage(default_pack)

        for field in [
            "async_pipeline_readiness",
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "pre_retrieval_summary_refresh",
            "by_source_role",
            "by_hook_type",
            "by_codex_event",
            "source_message_counts_by_role",
            "source_hook_counts_by_type",
            "source_codex_event_counts_by_event",
        ]:
            self.assertNotIn(field, compact)
        flat_item = compact["selected_refs"][0]
        for field in [
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_session_ids",
            "source_entity_count",
        ]:
            self.assertNotIn(field, flat_item)
        for field in [
            "async_pipeline_readiness",
            "memory_layer_budget",
            "dropped_memory_layer_budget",
            "memory_layer_pressure",
            "pre_retrieval_summary_refresh",
        ]:
            self.assertNotIn(field, grouped_compact)
        entity_item = grouped_compact["groups"][0]["items"][0]
        for field in [
            "source_roles",
            "source_role_counts",
            "source_hook_type_counts",
            "source_codex_event_counts",
            "source_session_ids",
            "source_entity_count",
        ]:
            self.assertNotIn(field, entity_item)
        self.assertNotIn("memory_hierarchy", compact)
        self.assertNotIn("memory_hierarchy", grouped_compact)
        readiness = debug_compact["async_pipeline_readiness"]
        self.assertFalse(readiness["ready_for_retrieval"])
        self.assertEqual(["summary", "compression"], readiness["remaining_stages"])
        self.assertEqual(["profile_summary_stale"], readiness["freshness_warnings"])
        self.assertEqual({"user_profile": 1}, readiness["pending_memory_scopes"])
        self.assertEqual({"cross_session": 1}, readiness["pending_session_continuities"])
        debug_item = debug_compact["selected_refs"][0]
        self.assertEqual(["assistant", "tool"], debug_item["source_roles"])
        self.assertEqual({"assistant": 2, "tool": 1}, debug_item["source_role_counts"])
        self.assertEqual({"hook_boundary": 3}, debug_item["source_hook_type_counts"])
        self.assertEqual({"Stop": 3}, debug_item["source_codex_event_counts"])
        self.assertEqual(["session_summary_1", "session_summary_2"], debug_item["source_session_ids"])
        self.assertEqual([201, 202, 203], debug_item["extraction_context_event_ids"])
        self.assertEqual(2, debug_item["source_entity_count"])
        self.assertEqual([301, 302, 303], debug_item["supersedes_session_entity_hashes"])
        self.assertEqual(3, debug_item["supersedes_session_entity_count"])
        self.assertEqual(1, debug_item["source_memory_selection_lossy_count"])
        self.assertEqual(2, debug_item["source_memory_selection_complete_count"])
        self.assertEqual(120, debug_item["source_memory_selection_dropped_text_chars"])
        self.assertEqual(3, debug_item["source_memory_selection_dropped_line_count"])
        self.assertEqual(0.75, debug_item["source_memory_selection_retained_text_ratio_avg"])
        self.assertEqual(0.8, debug_item["source_memory_selection_retained_line_ratio_avg"])
        self.assertEqual("always_when_profile_scope_available", debug_item["profile_promotion_policy"])
        self.assertEqual(3, debug_item["profile_revision"])
        self.assertEqual(2, debug_item["previous_profile_revision"])
        self.assertEqual(123456, debug_item["previous_profile_updated_at_ms"])
        self.assertEqual(301, debug_item["supersedes_session_entity_hash"])
        self.assertEqual("merged_profile_state", debug_item["current_state_policy"])
        self.assertNotIn("source_entity_hashes", debug_item)
        self.assertNotIn("source_lineage", debug_item)
        self.assertEqual({"user_profile": {"refs": 1, "tokens": 7}}, debug_compact["memory_layer_budget"]["by_memory_scope"])
        self.assertTrue(debug_compact["memory_layer_pressure"]["profile_memory_pressure"])
        self.assertTrue(debug_compact["memory_layer_pressure"]["cross_session_pressure"])
        self.assertIn("assistant_source_message_pressure", debug_compact["memory_layer_pressure"])
        hierarchy = debug_compact["memory_hierarchy"]
        self.assertEqual("user_profile", hierarchy["models"]["profile_entity"]["memory_scope"])
        self.assertEqual("context_profile_entity", hierarchy["models"]["profile_index"]["data_model"])
        self.assertEqual("same-session first, profile entity bridge second", hierarchy["retrieval_strategy"])
        self.assertEqual("prefer", hierarchy["session_scope_mode"])
        self.assertTrue(hierarchy["cross_session_enabled"])
        self.assertEqual(64, hierarchy["cross_session_budget_tokens"])
        self.assertEqual(100, hierarchy["cross_session_remote_budget_tokens"])
        self.assertEqual(256, hierarchy["cross_session_budget_floor_tokens"])
        self.assertFalse(hierarchy["cross_session_budget_floor_applied"])
        self.assertEqual(
            "remote_budget_too_small_for_profile_floor",
            hierarchy["cross_session_budget_floor_status"],
        )
        self.assertIn("profile_entity_bridge", hierarchy["selected_ref_flow"])
        self.assertIn("summary_or_compression", hierarchy["selected_ref_flow"])
        self.assertNotIn("recall_policy", compact)

