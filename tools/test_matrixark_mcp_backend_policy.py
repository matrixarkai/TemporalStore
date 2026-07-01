#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import threading
import unittest

import matrixark_mcp_server as mcp

try:
    from tools import matrixark_mcp_core as mcp_core
except ModuleNotFoundError:  # Direct execution with PYTHONPATH=tools.
    import matrixark_mcp_core as mcp_core




class _NativeAppendClient:
    def __init__(self) -> None:
        self.calls = []

    def get_string(self, key: str) -> str:
        return "0"

    def matrixark_batch_append_records(self, entries, *, count_key=None, count_value=None) -> None:
        self.calls.append({"entries": list(entries), "count_key": count_key, "count_value": count_value})


class _CandidateCacheClient:
    def __init__(self, records: list[dict]) -> None:
        self.records = records
        self.batch_hget_calls = 0

    def get_string(self, key: str) -> str:
        if key.endswith(":record_count"):
            return str(len(self.records))
        return ""

    def batch_hget(self, entries) -> list[dict]:
        self.batch_hget_calls += 1
        rows = []
        for index, entry in enumerate(entries):
            if index >= len(self.records):
                continue
            rows.append({"key": entry["key"], "field": entry["field"], "value": json.dumps(self.records[index])})
        return rows


class _NativeIndexClient:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], str] = {}
        self.batch_hget_entries: list[list[dict]] = []

    def get_string(self, key: str) -> str:
        if key.endswith(":record_count"):
            return self.store.get((key, "__string__"), "0")
        return self.store.get((key, "__string__"), "")

    def put_string(self, key: str, value: str) -> None:
        self.store[(key, "__string__")] = value

    def hget(self, key: str, field: str) -> str:
        return self.store.get((key, field), "")

    def hset(self, key: str, field: str, value: str) -> None:
        self.store[(key, field)] = value

    def batch_hget(self, entries) -> list[dict]:
        self.batch_hget_entries.append(list(entries))
        return [
            {"key": entry["key"], "field": entry["field"], "value": self.store.get((entry["key"], entry["field"]), "")}
            for entry in entries
        ]

    def matrixark_batch_append_records(self, entries, *, count_key=None, count_value=None) -> None:
        for entry in entries:
            self.hset(str(entry["key"]), str(entry["field"]), str(entry["value"]))
        if count_key is not None and count_value is not None:
            self.put_string(str(count_key), str(count_value))

class _FailingWarmupClient:
    def hset(self, key: str, field: str, value: str) -> None:
        raise RuntimeError("Slot not found for deploy_ns/deploy_table")

    def hget(self, key: str, field: str) -> str:
        return ""


def _direct_adapter_for_readiness(*, metaserver: str, client: object | None = None) -> mcp.MatrixArkTemporalStoreDirectAdapter:
    adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
    adapter._metaserver = metaserver
    adapter._namespace = "deploy_ns"
    adapter._table = "deploy_table"
    adapter._storage_prefix = "matrixark:test"
    adapter._client = client or _FailingWarmupClient()
    return adapter


class MatrixArkMcpBackendPolicyTest(unittest.TestCase):
    def setUp(self) -> None:
        self._old_profile = mcp.MATRIXARK_MCP_PROFILE
        self._old_allow_local = mcp.MATRIXARK_ALLOW_LOCAL_BACKEND
        self._old_require_ready = mcp.MATRIXARK_REQUIRE_BACKEND_READY
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()

    def tearDown(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = self._old_profile
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = self._old_allow_local
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = self._old_require_ready
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()

    def _args(self, backend: str) -> argparse.Namespace:
        return argparse.Namespace(backend=backend)

    def test_production_profile_rejects_local_storage(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = False
        with self.assertRaises(mcp.MatrixArkError):
            mcp.validate_mcp_backend_policy(self._args("local"))
        with self.assertRaises(mcp.MatrixArkError):
            mcp.validate_mcp_backend_policy(self._args("temporalstore-local"))

    def test_production_profile_allows_native_backends(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = False
        mcp.validate_mcp_backend_policy(self._args("temporalstore-direct"))
        mcp.validate_mcp_backend_policy(self._args("temporalstore-rust"))

    def test_debug_override_allows_local_storage(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "benchmark"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = True
        mcp.validate_mcp_backend_policy(self._args("local"))

    def test_backend_readiness_default_policy(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "benchmark"
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = ""
        self.assertTrue(mcp.backend_ready_required("temporalstore-rust"))
        self.assertTrue(mcp.backend_ready_required("temporalstore-direct"))
        self.assertFalse(mcp.backend_ready_required("local"))
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = "1"
        self.assertTrue(mcp.backend_ready_required("local"))

    def test_context_serving_records_share_stable_placement_route(self) -> None:
        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        node_hash = 44
        records = [
            {"record_type": "context_event", "event_id_hash": 1, "scope": scope, "node_hash": node_hash, "updated_at_ms": 1780000000000, "text": "event"},
            {"record_type": "context_entity", "entity_hash": 2, "scope": scope, "node_hash": node_hash, "state": "entity"},
            {"record_type": "context_segment", "segment_hash": 3, "scope": scope, "node_hash": node_hash, "text": "segment"},
            {"record_type": "context_embedding", "ref_hash": 4, "scope": scope, "node_hash": node_hash, "embedding": [0.1]},
            {"record_type": "resource_chunk", "chunk_hash": 5, "scope": scope, "node_hash": node_hash, "text": "chunk"},
            {"record_type": "skill_section", "section_hash": 6, "scope": scope, "node_hash": node_hash, "text": "skill"},
            {"record_type": "context_index", "index_name": "source_type:message", "ref_hash": 7, "scope": scope, "node_hash": node_hash},
        ]

        materialized = [
            record
            for record in mcp_core.materialize_serving_record_batch(records)
            if record.get("record_type") != "context_debug_record"
        ]

        placement_keys = {record.get("placement_key") for record in materialized}
        self.assertEqual(placement_keys, {"context:t=11|u=22|s=33|:node=44"})
        for record in materialized:
            route = record.get("storage_route")
            self.assertIsInstance(route, dict)
            self.assertEqual(route.get("placement_key"), record.get("placement_key"))
            self.assertEqual(route.get("routing_key"), record.get("placement_key"))
            self.assertEqual(route.get("partition_key"), record.get("placement_key"))
            self.assertEqual(route.get("colocation_group"), "matrixark_context")
            self.assertEqual(route.get("placement_hash"), record.get("placement_hash"))

    def test_direct_append_prefers_native_matrixark_batch_append_records(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-append"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
        adapter._retrieval_candidate_cache = {}
        adapter._retrieval_candidate_cache_lock = threading.RLock()
        adapter._entry_count_cache = None
        adapter._legacy_index_mode = False
        adapter._records_lock = threading.RLock()
        adapter._write_retries = 0
        adapter._write_backoff_s = 0.0
        adapter._write_throttle_s = 0.0

        adapter._append_many_materialized(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 123,
                    "tenant_hash": 1,
                    "scope_key": "scope",
                    "updated_at_ms": 1780000000000,
                    "text": "native batch append works",
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 123 + mcp_core.CONTEXT_TIMELINE_FANOUT,
                    "tenant_hash": 1,
                    "scope_key": "scope",
                    "updated_at_ms": 1780000000000,
                    "text": "same millisecond collision slot",
                },
            ]
        )

        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["count_key"], "matrixark:test:native-append:record_count")
        self.assertEqual(call["count_value"], "1")
        keys = {entry["key"] for entry in call["entries"]}
        self.assertIn("matrixark:test:native-append:records:000000", keys)
        self.assertTrue(any("context_event_by_ingestion_time" in key for key in keys))
        time_index_entries = [entry for entry in call["entries"] if "context_event_by_ingestion_time" in entry["key"]]
        self.assertEqual(len(time_index_entries), 2)
        time_index_payloads = [json.loads(entry["value"]) for entry in time_index_entries]
        self.assertEqual({payload["record_type"] for payload in time_index_payloads}, {"context_event_ref"})
        self.assertEqual({payload["ref_hash"] for payload in time_index_payloads}, {123, 123 + mcp_core.CONTEXT_TIMELINE_FANOUT})
        self.assertEqual({payload["timestamp_key_ms"] for payload in time_index_payloads}, {1780000000000})
        self.assertEqual(len({payload["context_event_key"] for payload in time_index_payloads}), 2)
        self.assertTrue(all("text" not in payload for payload in time_index_payloads))

    def test_direct_retrieval_records_reuses_candidate_cache_by_count_and_scope(self) -> None:
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 1,
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "scope": {"tenant_id": "tenant_a", "user_id": "user_a"},
                "text": "approval happened",
            },
            {
                "record_type": "context_pack_audit",
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "scope": {"tenant_id": "tenant_a", "user_id": "user_a"},
            },
            {
                "record_type": "context_event",
                "event_id_hash": 2,
                "tenant_id": "tenant_b",
                "user_id": "user_b",
                "scope": {"tenant_id": "tenant_b", "user_id": "user_b"},
                "text": "wrong scope",
            },
        ]
        client = _CandidateCacheClient(records)
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:candidate-cache"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
        adapter._retrieval_candidate_cache = {}
        adapter._retrieval_candidate_cache_lock = threading.RLock()
        adapter._entry_count_cache = None
        adapter._legacy_index_mode = False
        adapter._records_lock = threading.RLock()
        adapter._metrics_lock = threading.RLock()

        scope = {"tenant_id": "tenant_a", "user_id": "user_a"}
        first = adapter.retrieval_records(scope=scope)
        second = adapter.retrieval_records(scope=scope)

        self.assertEqual(client.batch_hget_calls, 1)
        self.assertFalse(first["scan_stats"]["candidate_cache_hit"])
        self.assertTrue(second["scan_stats"]["candidate_cache_hit"])
        self.assertEqual([record["event_id_hash"] for record in second["records"]], [1])
        self.assertEqual(second["scan_stats"]["dropped_type"], 1)
        self.assertEqual(second["scan_stats"]["dropped_scope"], 1)

    def test_direct_retrieval_uses_native_compact_index_prefilter(self) -> None:
        client = _NativeIndexClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-index"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
        adapter._retrieval_candidate_cache = {}
        adapter._retrieval_candidate_cache_lock = threading.RLock()
        adapter._entry_count_cache = None
        adapter._legacy_index_mode = False
        adapter._records_lock = threading.RLock()
        adapter._metrics_lock = threading.RLock()
        adapter._write_retries = 0
        adapter._write_backoff_s = 0.0
        adapter._write_throttle_s = 0.0

        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        adapter.append_many(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 101,
                    "scope": scope,
                    "node_hash": 44,
                    "updated_at_ms": 1780000000000,
                    "text": "Alice approved the GPU budget.",
                },
                {
                    "record_type": "context_embedding",
                    "embedding_type": "event_text",
                    "ref_type": "event",
                    "ref_hash": 101,
                    "scope": scope,
                    "node_hash": 44,
                    "embedding": [0.1, 0.2],
                },
                {
                    "record_type": "context_index",
                    "index_name": "source_type:message",
                    "ref_type": "event",
                    "ref_hash": 101,
                    "scope": scope,
                    "node_hash": 44,
                    "updated_at_ms": 1780000000000,
                },
            ]
        )

        result = adapter.retrieval_records(
            scope=scope,
            record_types={"context_event", "context_embedding", "context_index"},
            secondary_index_groups=[{"source_type:message"}],
        )

        stats = result["scan_stats"]
        self.assertTrue(stats["native_pushdown"])
        self.assertEqual(stats["execution_mode"], "native_secondary_index_prefilter")
        self.assertEqual(stats["native_index_ref_hash_count"], 1)
        self.assertGreaterEqual(stats["native_locations"], 1)
        self.assertEqual({record["record_type"] for record in result["records"]}, {"context_event", "context_embedding", "context_index"})
        broad_record_loads = [
            entries
            for entries in client.batch_hget_entries
            if entries and all(str(entry["key"]).startswith(f"{adapter._record_hash_key}:") for entry in entries)
        ]
        self.assertTrue(broad_record_loads)
        self.assertTrue(all(len(entries) < adapter._shard_size for entries in broad_record_loads))

    def test_direct_retrieval_candidate_cache_is_shared_across_adapters(self) -> None:
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 1,
                "tenant_id": "tenant_a",
                "user_id": "user_a",
                "scope": {"tenant_id": "tenant_a", "user_id": "user_a"},
                "text": "approval happened",
            }
        ]
        scope = {"tenant_id": "tenant_a", "user_id": "user_a"}

        def make_adapter(client: _CandidateCacheClient) -> mcp.MatrixArkTemporalStoreDirectAdapter:
            adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
            adapter._client = client
            adapter._storage_prefix = "matrixark:test:candidate-cache-shared"
            adapter._record_hash_key = f"{adapter._storage_prefix}:records"
            adapter._index_key = f"{adapter._storage_prefix}:record_index"
            adapter._count_key = f"{adapter._storage_prefix}:record_count"
            adapter._shard_size = 1024
            adapter._index_cache = None
            adapter._records_cache = None
            adapter._retrieval_candidate_cache = {}
            adapter._retrieval_candidate_cache_lock = threading.RLock()
            adapter._entry_count_cache = None
            adapter._legacy_index_mode = False
            adapter._records_lock = threading.RLock()
            adapter._metrics_lock = threading.RLock()
            return adapter

        first_client = _CandidateCacheClient(records)
        second_client = _CandidateCacheClient(records)
        first = make_adapter(first_client).retrieval_records(scope=scope)
        second = make_adapter(second_client).retrieval_records(scope=scope)

        self.assertEqual(first_client.batch_hget_calls, 1)
        self.assertEqual(second_client.batch_hget_calls, 0)
        self.assertFalse(first["scan_stats"]["candidate_cache_hit"])
        self.assertTrue(second["scan_stats"]["candidate_cache_hit"])
        self.assertEqual(second["scan_stats"]["candidate_cache_scope"], "process_global")
        self.assertEqual([record["event_id_hash"] for record in second["records"]], [1])

    def test_direct_readiness_reports_metaserver_failure(self) -> None:
        adapter = _direct_adapter_for_readiness(metaserver="127.0.0.1:1")
        result = adapter._run_backend_readiness_gate(reason="unit-metaserver-down", timeout_ms=1)

        self.assertEqual(result["status"], "topology_not_ready")
        self.assertIn("metaserver unreachable", result["error"])
        self.assertFalse(result["checks"]["metaserver_reachable"]["ok"])
        self.assertFalse(result["checks"]["namespace_table_opened"])
        self.assertFalse(result["checks"]["slot_coverage_verified_by_warmup_hset_hget"])
        self.assertTrue(result["attempt_log"])

    def test_direct_readiness_reports_slot_failure(self) -> None:
        adapter = _direct_adapter_for_readiness(metaserver="")
        result = adapter._run_backend_readiness_gate(reason="unit-slot-missing", timeout_ms=1)

        self.assertEqual(result["status"], "topology_not_ready")
        self.assertIn("Slot not found", result["error"])
        self.assertTrue(result["checks"]["namespace_table_opened"])
        self.assertFalse(result["checks"]["slot_coverage_verified_by_warmup_hset_hget"])
        self.assertEqual(result["topology"]["namespace"], "deploy_ns")
        self.assertEqual(result["topology"]["table"], "deploy_table")


if __name__ == "__main__":
    unittest.main()
