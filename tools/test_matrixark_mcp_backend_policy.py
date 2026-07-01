#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
import threading
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

try:
    from tools import matrixark_mcp_local_adapter as mcp_local
    from tools import matrixark_mcp_core as mcp_core
    from tools.run_matrixark_cpp_rust_scale_report import comparison, selected_ref_count, summarize_retrieval_metrics
except ModuleNotFoundError:  # Direct execution with PYTHONPATH=tools.
    import matrixark_mcp_local_adapter as mcp_local
    import matrixark_mcp_core as mcp_core
    from run_matrixark_cpp_rust_scale_report import comparison, selected_ref_count, summarize_retrieval_metrics




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


class _NativeContextPackClient:
    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.batch_hget_calls = 0

    def get_string(self, key: str) -> str:
        if key.endswith(":record_count"):
            return "7"
        return ""

    def batch_hget(self, entries) -> list[dict]:
        self.batch_hget_calls += 1
        raise AssertionError("native context pack path should not materialize records in Python")

    def matrixark_retrieve_context_pack(self, request) -> dict:
        if isinstance(request, str):
            request = json.loads(request)
        self.requests.append(dict(request))
        return {
            "context_pack_id": "native-pack-1",
            "selected_refs": [
                {
                    "ref_type": "event",
                    "ref_hash": 101,
                    "context_class": "event",
                    "text": "Alice approved the GPU budget.",
                    "score": 0.91,
                    "token_estimate": 6,
                }
            ],
            "used_remote_context_tokens": 6,
            "used_local_context_tokens": int(request.get("local_context_tokens") or 0),
            "total_prompt_context_tokens": 6 + int(request.get("local_context_tokens") or 0),
            "remote_context_budget_tokens": 1024,
            "dropped_refs": {"over_budget_count": 2, "low_score_count": 3},
            "retrieval_metrics": {
                "query_plan_ms": 1.5,
                "node_traversal_ms": 2.5,
                "index_prefilter_ms": 3.5,
                "candidate_fetch_ms": 4.5,
                "score_ms": 5.5,
                "pack_ms": 6.5,
                "audit_ms": 0.0,
                "selected_refs": 1,
                "scanned_records": 7,
                "cache_hit": True,
                "placement_partitions_touched": 2,
            },
            "quality_warnings": [],
        }


class _AuditCaptureAdapter(mcp_local.MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        self.appended: list[dict] = []
        self.audit_appended: list[dict] = []

    def append(self, record: dict) -> None:
        self.appended.append(record)

    def append_audit(self, record: dict) -> None:
        self.audit_appended.append(record)


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
        mcp_core._DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.clear()

    def tearDown(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = self._old_profile
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = self._old_allow_local
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = self._old_require_ready
        mcp_local.CONTEXT_TELEMETRY_WRITE_MODE = mcp_core.CONTEXT_TELEMETRY_WRITE_MODE
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
        mcp_core._DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.clear()

    def test_context_pack_visibility_defaults_to_inline_telemetry_without_audit_write(self) -> None:
        adapter = _AuditCaptureAdapter(Path("/tmp/matrixark-audit-capture.jsonl"))
        mcp_local.CONTEXT_TELEMETRY_WRITE_MODE = "inline"

        decision = adapter.append_context_pack_visibility(
            pack={
                "context_pack_id": "pack-inline",
                "selected_refs": [{"ref_type": "event"}],
                "dropped_refs": {"refs": []},
            },
            audit_record={"record_type": "context_pack_audit"},
            query="what changed?",
            scope={},
            audit_mode="telemetry_only",
            audit_sample_rate=1.0,
        )

        self.assertEqual(decision["audit_mode"], "telemetry_only")
        self.assertEqual(decision["telemetry_write_mode"], "inline")
        self.assertTrue(decision["telemetry_record"])
        self.assertFalse(decision["rich_replay_audit"])
        self.assertFalse(decision["serving_blocked_on_full_audit"])
        self.assertEqual(adapter.appended, [])
        self.assertEqual(adapter.audit_appended, [])

    def test_context_pack_visibility_full_audit_uses_async_audit_hook(self) -> None:
        adapter = _AuditCaptureAdapter(Path("/tmp/matrixark-audit-capture.jsonl"))
        mcp_local.CONTEXT_TELEMETRY_WRITE_MODE = "async"

        decision = adapter.append_context_pack_visibility(
            pack={
                "context_pack_id": "pack-full",
                "selected_refs": [{"ref_type": "event"}],
                "dropped_refs": {"refs": []},
            },
            audit_record={"record_type": "context_pack_audit"},
            query="what changed?",
            scope={},
            audit_mode="full",
            audit_sample_rate=1.0,
        )

        self.assertEqual(decision["audit_mode"], "full")
        self.assertEqual(decision["telemetry_write_mode"], "async")
        self.assertTrue(decision["rich_replay_audit"])
        self.assertEqual(adapter.appended, [])
        self.assertEqual([record["record_type"] for record in adapter.audit_appended], ["context_pack_telemetry", "context_pack_audit"])

    def test_scale_report_counts_compact_context_pack_groups(self) -> None:
        self.assertEqual(
            selected_ref_count(
                {
                    "context_pack_id": "pack",
                    "groups": [
                        {"type": "event", "n": 2, "items": [{"text": "a"}, {"text": "b"}]},
                        {"type": "resource_chunk", "n": 1, "items": [{"text": "c"}]},
                    ],
                }
            ),
            3,
        )
        self.assertEqual(
            selected_ref_count(
                {
                    "context_pack": {
                        "groups": {
                            "same_session:event": [{"text": "a"}],
                            "shared:resource_chunk": [{"text": "b"}, {"text": "c"}],
                        }
                    }
                }
            ),
            3,
        )

    def test_scale_report_blocks_selected_ref_drift(self) -> None:
        base = {
            "status": "passed",
            "raw_storage": {"write": {"record_qps": 100, "p95_ms": 1}, "read": {"qps": 100, "p95_ms": 1}},
            "ingest_messages": {"message_qps": 100},
            "ingest": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1},
            "retrieve": {"qps": 100, "p50_ms": 1, "p95_ms": 1, "p99_ms": 1, "selected_refs_avg": 4},
        }
        rust = json.loads(json.dumps(base))
        rust["retrieve"]["selected_refs_avg"] = 0

        result = comparison(base, rust)

        self.assertEqual(result["status"], "failed")
        self.assertTrue(
            any(row["metric"] == "selected_refs_avg" and not row["parity_passed"] for row in result["rows"])
        )

    def test_scale_report_compares_retrieval_stage_metrics(self) -> None:
        stage_metrics = summarize_retrieval_metrics(
            [
                {
                    "query_plan_ms": 1,
                    "node_traversal_ms": 2,
                    "index_prefilter_ms": 3,
                    "candidate_fetch_ms": 4,
                    "score_ms": 5,
                    "pack_ms": 6,
                    "audit_ms": 0.5,
                    "selected_refs": 2,
                    "scanned_records": 10,
                    "cache_hit": True,
                    "placement_partitions_touched": 1,
                }
            ]
        )
        base = {
            "status": "passed",
            "raw_storage": {"write": {"record_qps": 100, "p95_ms": 1}, "read": {"qps": 100, "p95_ms": 1}},
            "ingest_messages": {"message_qps": 100},
            "ingest": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1},
            "retrieve": {"qps": 100, "p50_ms": 1, "p95_ms": 1, "p99_ms": 1, "selected_refs_avg": 2, "stage_metrics": stage_metrics},
        }

        result = comparison(base, base)

        self.assertEqual(stage_metrics["stage_p95_ms"]["pack_ms"], 6.0)
        self.assertTrue(any(row["metric"] == "pack_p95_ms" for row in result["rows"]))
        self.assertTrue(any(row["metric"] == "cache_hit_rate" for row in result["rows"]))

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

        adapter.append_many(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 123,
                    "tenant_hash": 1,
                    "scope_key": "scope",
                    "node_hash": 44,
                    "storage_options": {"storage_family": "shared_store", "write_mode": "async"},
                    "updated_at_ms": 1780000000000,
                    "text": "native batch append works",
                },
                {
                    "record_type": "context_event",
                    "event_id_hash": 123 + mcp_core.CONTEXT_TIMELINE_FANOUT,
                    "tenant_hash": 1,
                    "scope_key": "scope",
                    "node_hash": 44,
                    "storage_options": {"storage_family": "shared_store", "write_mode": "async"},
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
        routed_entries = [entry for entry in call["entries"] if entry.get("storage_route", {}).get("placement_key")]
        self.assertTrue(routed_entries)
        for entry in routed_entries:
            route = entry["storage_route"]
            self.assertEqual(route["placement_key"], "context:scope:node=44")
            self.assertEqual(route["routing_key"], "context:scope:node=44")
            self.assertEqual(route["write_mode"], "async")
            self.assertTrue(route["background_write"])
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

    def test_direct_native_retrieve_matches_reference_logical_refs(self) -> None:
        scope = {"tenant_hash": 11, "user_hash": 22, "session_hash": 33}
        other_scope = {"tenant_hash": 11, "user_hash": 99, "session_hash": 33}
        records = [
            {
                "record_type": "context_event",
                "event_id_hash": 101,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
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
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 44,
                "vector": [0.1, 0.2],
            },
            {
                "record_type": "context_index",
                "index_name": "source_type:message",
                "ref_type": "event",
                "ref_hash": 101,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 44,
                "updated_at_ms": 1780000000000,
            },
            {
                "record_type": "context_event",
                "event_id_hash": 202,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 55,
                "updated_at_ms": 1780000000001,
                "text": "Same scope but wrong node.",
            },
            {
                "record_type": "context_index",
                "index_name": "source_type:message",
                "ref_type": "event",
                "ref_hash": 202,
                "scope": scope,
                "scope_key": mcp_core.canonical_scope_key(scope),
                "node_hash": 55,
                "updated_at_ms": 1780000000001,
            },
            {
                "record_type": "context_event",
                "event_id_hash": 303,
                "scope": other_scope,
                "scope_key": mcp_core.canonical_scope_key(other_scope),
                "node_hash": 44,
                "updated_at_ms": 1780000000002,
                "text": "Wrong scope should be filtered.",
            },
            {
                "record_type": "context_index",
                "index_name": "source_type:message",
                "ref_type": "event",
                "ref_hash": 303,
                "scope": other_scope,
                "scope_key": mcp_core.canonical_scope_key(other_scope),
                "node_hash": 44,
                "updated_at_ms": 1780000000002,
            },
        ]

        client = _NativeIndexClient()
        direct = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        direct._client = client
        direct._storage_prefix = "matrixark:test:native-reference-parity"
        direct._record_hash_key = f"{direct._storage_prefix}:records"
        direct._index_key = f"{direct._storage_prefix}:record_index"
        direct._count_key = f"{direct._storage_prefix}:record_count"
        direct._shard_size = 1024
        direct._index_cache = None
        direct._records_cache = None
        direct._retrieval_candidate_cache = {}
        direct._retrieval_candidate_cache_lock = threading.RLock()
        direct._entry_count_cache = None
        direct._legacy_index_mode = False
        direct._records_lock = threading.RLock()
        direct._metrics_lock = threading.RLock()
        direct._write_retries = 0
        direct._write_backoff_s = 0.0
        direct._write_throttle_s = 0.0
        direct.append_many(records)

        with tempfile.TemporaryDirectory() as tmpdir:
            reference = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "reference.jsonl")
            reference.append_many(records)
            kwargs = {
                "scope": scope,
                "record_types": {"context_event", "context_embedding", "context_index"},
                "secondary_index_groups": [{"source_type:message"}],
                "selected_node_hashes": {44},
            }
            direct_result = direct.retrieval_records(**kwargs)
            reference_result = reference.retrieval_records(**kwargs)
            mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
            direct_cached_table_result = direct.retrieval_records(**kwargs)

        def logical_refs(rows: list[dict]) -> set[tuple[str, int]]:
            refs: set[tuple[str, int]] = set()
            for row in rows:
                record_type = str(row.get("record_type") or "")
                if isinstance(row.get("ref_hashes"), list):
                    for ref_hash in row["ref_hashes"]:
                        refs.add((record_type, int(ref_hash)))
                    continue
                ref_hash = row.get("event_id_hash") or row.get("ref_hash") or row.get("chunk_hash") or row.get("section_hash")
                if ref_hash is not None:
                    refs.add((record_type, int(ref_hash)))
            return refs

        self.assertEqual(logical_refs(direct_result["records"]), logical_refs(reference_result["records"]))
        self.assertEqual(logical_refs(direct_result["records"]), {("context_event", 101), ("context_embedding", 101), ("context_index", 101)})
        self.assertTrue(direct_result["scan_stats"]["native_pushdown"])
        self.assertEqual(direct_result["scan_stats"]["execution_mode"], "native_placement_prefetch")
        self.assertEqual(direct_result["scan_stats"]["native_placement_nodes"], 1)
        self.assertGreaterEqual(direct_result["scan_stats"]["native_placement_locator_rows"], 1)
        self.assertGreaterEqual(direct_result["scan_stats"]["native_placement_locations"], 1)
        placement_lookups = [
            entries
            for entries in client.batch_hget_entries
            if entries and all("context_placement_lookup" in str(entry["key"]) for entry in entries)
        ]
        self.assertTrue(placement_lookups)
        self.assertEqual({str(entry["field"]) for entry in placement_lookups[0]}, {"44"})
        self.assertEqual(direct_result["scan_stats"]["dropped_scope"], reference_result["scan_stats"]["dropped_by_scope"])
        self.assertEqual(reference_result["scan_stats"]["dropped_by_node"], 2)
        self.assertEqual(direct_result["scan_stats"]["dropped_node"], 0)
        self.assertTrue(direct_cached_table_result["scan_stats"]["native_placement_candidate_cache_hit"])
        self.assertEqual(
            logical_refs(direct_cached_table_result["records"]),
            {("context_event", 101), ("context_embedding", 101), ("context_index", 101)},
        )
        direct.append_many(
            [
                {
                    "record_type": "context_event",
                    "event_id_hash": 404,
                    "scope": scope,
                    "scope_key": mcp_core.canonical_scope_key(scope),
                    "node_hash": 44,
                    "updated_at_ms": 1780000000003,
                    "text": "New append changes the watermark.",
                }
            ]
        )
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
        direct_after_append_result = direct.retrieval_records(**kwargs)
        self.assertFalse(direct_after_append_result["scan_stats"]["native_placement_candidate_cache_hit"])
        self.assertIn(("context_event", 404), logical_refs(direct_after_append_result["records"]))

    def test_direct_retrieve_uses_native_context_pack_api_when_available(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "max_context_tokens": 2048,
                "local_context_tokens": 128,
                "ranking": {"max_selected_refs": 8},
                "debug_context_pack": True,
                "include_retrieval_metrics": True,
            }
        )

        self.assertEqual(result["context_pack_id"], "native-pack-1")
        self.assertTrue(result["native_context_pack"])
        self.assertEqual(result["context_pack_assembly"], "native_cpp_direct")
        self.assertEqual(client.batch_hget_calls, 0)
        self.assertEqual(len(client.requests), 1)
        request = client.requests[0]
        self.assertEqual(request["storage_prefix"], "matrixark:test:native-pack")
        self.assertEqual(request["watermark_count"], 7)
        self.assertEqual(request["scope_key"], "t=11|u=22|s=33|")
        self.assertTrue(request["required_output"]["selected_refs"])
        self.assertTrue(request["required_output"]["dropped_summary"])
        self.assertTrue(request["required_output"]["retrieval_metrics"])
        self.assertEqual(result["retrieval_metrics"]["query_plan_ms"], 1.5)
        self.assertEqual(result["retrieval_metrics"]["placement_partitions_touched"], 2)
        self.assertEqual(
            result["recall_policy"]["backend_retrieval_pushdown"]["execution_mode"],
            "native_context_pack",
        )

    def test_direct_retrieve_compact_pack_can_include_native_retrieval_metrics(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "include_retrieval_metrics": True,
            }
        )

        self.assertIn("groups", result)
        self.assertNotIn("selected_refs", result)
        self.assertEqual(result["retrieval_metrics"]["score_ms"], 5.5)
        self.assertEqual(result["retrieval_metrics"]["scanned_records"], 7)

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
