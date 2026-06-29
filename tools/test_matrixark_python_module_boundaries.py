#!/usr/bin/env python3
"""Regression tests for the MatrixArk Python MCP module layout."""

from __future__ import annotations

import ast
import importlib
import sys
import tempfile
import unittest
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


    def test_mcp_entrypoint_reexports_split_modules(self) -> None:
        server_mod = importlib.import_module("tools.matrixark_mcp_server")
        metrics_mod = importlib.import_module("tools.matrixark_mcp_metrics")
        local_mod = importlib.import_module("tools.matrixark_mcp_local_adapter")
        temporal_mod = importlib.import_module("tools.matrixark_mcp_temporal_adapters")

        self.assertIs(server_mod.MatrixArkServiceMetrics, metrics_mod.MatrixArkServiceMetrics)
        self.assertIs(server_mod.MatrixArkLocalAdapter, local_mod.MatrixArkLocalAdapter)
        self.assertIs(server_mod.MatrixArkTemporalStoreDirectAdapter, temporal_mod.MatrixArkTemporalStoreDirectAdapter)
        self.assertIs(server_mod.MatrixArkTemporalStoreRustAdapter, temporal_mod.MatrixArkTemporalStoreRustAdapter)

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



if __name__ == "__main__":
    unittest.main()
