#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

import matrixark_mcp_server as mcp

try:
    from tools import matrixark_mcp_local_adapter as mcp_local
    from tools import matrixark_mcp_core as mcp_core
    from tools.run_matrixark_cpp_rust_scale_report import (
        comparison,
        effective_storage_tuning_from_env,
        fallback_flags_from_backend,
        phase_scale_matrix_gate,
        production_policy_gate,
        selected_ref_count,
        storage_tuning_failures,
        summarize_retrieval_metrics,
        timeout_count,
        validate_cpp_runtime_host,
        validate_rust_runtime_path,
    )
    from tools.validate_storage_lifecycle_parity import REPORT_PAIR_CORPUS, _load_json, validate_report_pair
except ModuleNotFoundError:  # Direct execution with PYTHONPATH=tools.
    import matrixark_mcp_local_adapter as mcp_local
    import matrixark_mcp_core as mcp_core
    from run_matrixark_cpp_rust_scale_report import (
        comparison,
        effective_storage_tuning_from_env,
        fallback_flags_from_backend,
        phase_scale_matrix_gate,
        production_policy_gate,
        selected_ref_count,
        storage_tuning_failures,
        summarize_retrieval_metrics,
        timeout_count,
        validate_cpp_runtime_host,
        validate_rust_runtime_path,
    )
    from validate_storage_lifecycle_parity import REPORT_PAIR_CORPUS, _load_json, validate_report_pair




_SHARED_CORRECTNESS_EVIDENCE = {
    "scope_filtering": True,
    "placement_filtering": True,
    "compact_secondary_index_prefilter": True,
    "stale_superseded_exclusion": True,
    "shared_resource_skill_quota": True,
    "cross_session_quota_rerank": True,
}


class MatrixArkRustProxyPoolPolicyTest(unittest.TestCase):
    def test_rust_bridge_defaults_to_separate_read_write_pack_lanes(self) -> None:
        old_env = {
            key: os.environ.get(key)
            for key in (
                "MATRIXARK_RUST_PROXY_WRITE_LANES",
                "MATRIXARK_RUST_PROXY_READ_LANES",
                "MATRIXARK_RUST_PROXY_PACK_LANES",
                "MATRIXARK_RUST_PROXY_CONTROL_LANES",
                "MATRIXARK_RUST_PROXY_SHARED_PROCESS",
            )
        }
        for key in old_env:
            os.environ.pop(key, None)
        try:
            client = mcp.MatrixArkRustCliClient(
                cli_path="matrixark_record_log",
                metaserver="127.0.0.1:18000",
                namespace="ns",
                table="table",
                request_timeout_ms=10000,
                io_timeout_ms=10000,
            )
            snapshot = client.metrics_snapshot()
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertTrue(snapshot["shared_process_mode"])
        self.assertEqual(snapshot["lane_pool"], {"write": 1, "read": 1, "pack": 1, "control": 1})
        self.assertEqual(snapshot["max_inflight"], 1)
        self.assertFalse(snapshot["write_pool_enabled"])
        self.assertFalse(snapshot["read_pool_enabled"])
        self.assertFalse(snapshot["pack_pool_enabled"])
        self.assertEqual(client._lane_group_for_op("matrixark_batch_append_records"), "write")
        self.assertEqual(client._lane_group_for_op("matrixark_batch_append_raw_ingestion_records"), "write")
        self.assertEqual(client._lane_group_for_op("batch_hget"), "read")
        self.assertEqual(client._lane_group_for_op("matrixark_retrieve_context_pack"), "pack")
        self.assertEqual(client._lane_group_for_op("readiness"), "control")

    def test_rust_bridge_can_opt_into_separate_read_write_pack_lanes(self) -> None:
        old_env = {
            key: os.environ.get(key)
            for key in (
                "MATRIXARK_RUST_PROXY_WRITE_LANES",
                "MATRIXARK_RUST_PROXY_READ_LANES",
                "MATRIXARK_RUST_PROXY_PACK_LANES",
                "MATRIXARK_RUST_PROXY_CONTROL_LANES",
                "MATRIXARK_RUST_PROXY_SHARED_PROCESS",
            )
        }
        os.environ["MATRIXARK_RUST_PROXY_SHARED_PROCESS"] = "0"
        for key in ("MATRIXARK_RUST_PROXY_WRITE_LANES", "MATRIXARK_RUST_PROXY_READ_LANES", "MATRIXARK_RUST_PROXY_PACK_LANES", "MATRIXARK_RUST_PROXY_CONTROL_LANES"):
            os.environ.pop(key, None)
        try:
            client = mcp.MatrixArkRustCliClient(
                cli_path="matrixark_rust_proxy",
                metaserver="127.0.0.1:18000",
                namespace="ns",
                table="table",
                request_timeout_ms=10000,
                io_timeout_ms=10000,
            )
            snapshot = client.metrics_snapshot()
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertFalse(snapshot["shared_process_mode"])
        self.assertEqual(snapshot["lane_pool"], {"write": 4, "read": 4, "pack": 2, "control": 1})
        self.assertEqual(snapshot["max_inflight"], 11)
        self.assertTrue(snapshot["write_pool_enabled"])
        self.assertTrue(snapshot["read_pool_enabled"])
        self.assertTrue(snapshot["pack_pool_enabled"])


class _NativeAppendClient:
    def __init__(self) -> None:
        self.calls = []

    def get_string(self, key: str) -> str:
        return "0"

    def matrixark_batch_append_records(self, entries, *, count_key=None, count_value=None, append_options=None) -> None:
        self.calls.append(
            {
                "entries": list(entries),
                "count_key": count_key,
                "count_value": count_value,
                "append_options": dict(append_options or {}),
            }
        )


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

    def matrixark_batch_append_records(self, entries, *, count_key=None, count_value=None, append_options=None) -> None:
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
                "append_queue_wait_ms": 0.25,
                "append_engine_ms": 0.75,
                "selected_refs": 1,
                "dropped_refs": 21,
                "scanned_records": 7,
                "candidate_cache_hit": True,
                "placement_partitions_touched": 2,
                "placement_fetch_count": 8,
                "index_postings_read": 9,
                "compact_index_bucket_used": True,
                "compact_index_bucket_count": 3,
                "timeout_count": 0,
                "fallback_flags": ["native_context_pack"],
                "correctness_evidence": {
                    "scope_filtering": True,
                    "placement_filtering": True,
                    "compact_secondary_index_prefilter": True,
                    "stale_superseded_exclusion": True,
                    "shared_resource_skill_quota": True,
                    "cross_session_quota_rerank": True,
                },
                "drop_counters": {
                    "scope": 1,
                    "placement": 2,
                    "index_filter": 3,
                    "stale": 4,
                    "token_budget": 5,
                    "score_threshold": 6,
                },
            },
            "quality_warnings": [],
        }


class _FailingNativeContextPackClient:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.read_all_calls = 0

    def get_string(self, key: str) -> str:
        if key.endswith(":record_count"):
            return "7"
        return ""

    def batch_hget(self, entries) -> list[dict]:
        self.read_all_calls += 1
        return []

    def matrixark_retrieve_context_pack(self, request) -> dict:
        if self.mode == "raise":
            raise RuntimeError("native offline")
        if self.mode == "raw_tables":
            return {
                "context_pack_id": "bad-native-pack",
                "selected_refs": [],
                "candidate_records": [{"record_type": "context_event", "text": "raw table"}],
            }
        return {"unexpected": True}


class _AuditCaptureAdapter(mcp_local.MatrixArkLocalAdapter):
    def __post_init__(self) -> None:
        self.appended: list[dict] = []
        self.audit_appended: list[dict] = []

    def append(self, record: dict) -> None:
        self.appended.append(record)

    def append_audit(self, record: dict) -> None:
        self.audit_appended.append(record)




class _NativeAppendClient:
    def __init__(self) -> None:
        self.calls = []

    def get_string(self, key: str) -> str:
        return "0"

    def matrixark_batch_append_records(self, entries, *, count_key=None, count_value=None) -> None:
        self.calls.append({"entries": list(entries), "count_key": count_key, "count_value": count_value})

class _FailingWarmupClient:
    def hset(self, key: str, field: str, value: str) -> None:
        raise RuntimeError("Slot not found for deploy_ns/deploy_table")

    def hget(self, key: str, field: str) -> str:
        return ""

class _NativeCandidateScanClient:
    def __init__(self) -> None:
        self.calls = []

    def matrixark_scan_candidates(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "records": [
                {"record_type": "resource_chunk", "scope": kwargs["scope"], "text": "native filtered pdf"}
            ],
            "scan_stats": {
                "execution_mode": "rust_proxy_native_candidate_prefilter",
                "native_prefix_scan": True,
                "native_secondary_index_prefilter": True,
                "scanned_records": 9,
                "returned_records": 1,
                "secondary_index_dropped_candidate_count": 3,
            },
        }


class _FailingNativeCandidateScanClient:
    def matrixark_scan_candidates(self, **kwargs):
        raise RuntimeError("native scan unavailable")


class _NativeContextPackClient:
    def __init__(self) -> None:
        self.calls = []

    def matrixark_retrieve_context_pack(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "context_pack": {
                "context_pack_id": "native-pack-1",
                "selected_refs": [{"ref_type": "resource_chunk", "text": "native packed evidence"}],
                "used_context_tokens": 3,
                "recall_policy": {
                    "native_context_pack": {"enabled": True},
                    "native_response_contract": {
                        "raw_records_returned_to_python": False,
                        "python_hot_path_records": 0,
                    },
                },
            },
        }


class _BadNativeContextPackClient:
    def matrixark_retrieve_context_pack(self, **kwargs):
        return {
            "native_pack_assembly": True,
            "records": [{"record_type": "context_event", "text": "raw record should not cross Python boundary"}],
            "context_pack": {"context_pack_id": "bad", "selected_refs": []},
        }


def _direct_adapter_for_readiness(*, metaserver: str, client: object | None = None) -> mcp.MatrixArkTemporalStoreDirectAdapter:
    adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
    adapter._metaserver = metaserver
    adapter._namespace = "deploy_ns"
    adapter._table = "deploy_table"
    adapter._storage_prefix = "matrixark:test"
    adapter._client = client or _FailingWarmupClient()
    return adapter


class MatrixArkMcpBackendPolicyTest(unittest.TestCase):
    def _storage_tuning(self) -> dict[str, object]:
        return {
            "TS_CONTEXT_PAGE_TARGET_BYTES": 65536,
            "TS_BLOCK_SEGMENT_TARGET_BYTES": 1073741824,
            "TS_STORAGE_ZONE_SIZE": 10485760,
            "TS_STREAM_MAX_BLOB_SIZE": 10485760,
            "TS_COMPACTION_WATERMARK_BYTES": 268435456,
            "TS_COLD_SCAN_NO_CACHE_FILL": True,
            "TS_PAGE_INDEX_CACHE_BYTES": 67108864,
            "TS_BLOCK_INDEX_CACHE_BYTES": 67108864,
            "effective_block_segment_target_bytes": 10485760,
        }

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

    def test_phase_scale_matrix_tracks_required_post_phase_sweeps(self) -> None:
        args = argparse.Namespace(
            events=1000,
            retrieve_workers=4,
            skip_context_pipeline=False,
            phase_name="phase-4",
            phase_scale_events="1000,10000,100000",
            phase_retrieve_workers="4,8,16,32",
            phase_resource_imports="large_pdf,large_csv,repo_directory",
            phase_contextmemory_features="resources,skills,cross_session_retrieval,compact_indexes,audit_light_telemetry",
            completed_scale_events="10000,100000",
            completed_retrieve_workers="8,16,32",
            completed_resource_imports="large_pdf,large_csv,repo_directory",
            completed_contextmemory_features="resources,skills",
            require_phase_scale_matrix=True,
        )

        gate = phase_scale_matrix_gate({"config": {"events": 1000, "retrieve_workers": 4}}, args)

        self.assertEqual(gate["status"], "passed")
        self.assertEqual(gate["open_required_cases"], [])
        self.assertEqual(gate["phase"], "phase-4")
        self.assertEqual(gate["full_contextmemory_pipeline"]["status"], "passed")

    def test_phase_scale_matrix_can_fail_closed_for_missing_scale_evidence(self) -> None:
        args = argparse.Namespace(
            events=1000,
            retrieve_workers=4,
            skip_context_pipeline=False,
            phase_name="phase-4",
            phase_scale_events="1000,10000,100000",
            phase_retrieve_workers="4,8,16,32",
            phase_resource_imports="large_pdf,large_csv,repo_directory",
            phase_contextmemory_features="resources,skills,cross_session_retrieval,compact_indexes,audit_light_telemetry",
            completed_scale_events="",
            completed_retrieve_workers="",
            completed_resource_imports="",
            completed_contextmemory_features="",
            require_phase_scale_matrix=True,
        )

        gate = phase_scale_matrix_gate({"config": {"events": 1000, "retrieve_workers": 4}}, args)

        self.assertEqual(gate["status"], "failed")
        open_cases = {(item["group"], item["case"]) for item in gate["open_required_cases"]}
        self.assertIn(("event_ingestion", 10000), open_cases)
        self.assertIn(("event_ingestion", 100000), open_cases)
        self.assertIn(("resource_imports", "large_pdf"), open_cases)
        self.assertEqual(gate["full_contextmemory_pipeline"]["status"], "incomplete")

    def test_scale_report_effective_storage_tuning_reads_public_knobs(self) -> None:
        names = [
            "TS_CONTEXT_PAGE_TARGET_BYTES",
            "TS_BLOCK_SEGMENT_TARGET_BYTES",
            "TS_STORAGE_ZONE_SIZE",
            "TS_STREAM_MAX_BLOB_SIZE",
            "TS_COMPACTION_WATERMARK_BYTES",
            "TS_COLD_SCAN_NO_CACHE_FILL",
            "TS_PAGE_INDEX_CACHE_BYTES",
            "TS_BLOCK_INDEX_CACHE_BYTES",
            "TEMPORALSTORE_STORAGE_ZONE_SIZE",
            "TEMPORALSTORE_STREAM_MAX_BLOB_SIZE",
        ]
        old_env = {name: os.environ.get(name) for name in names}
        try:
            os.environ.update(
                {
                    "TS_CONTEXT_PAGE_TARGET_BYTES": "131072",
                    "TS_BLOCK_SEGMENT_TARGET_BYTES": "1073741824",
                    "TS_STORAGE_ZONE_SIZE": "33554432",
                    "TS_STREAM_MAX_BLOB_SIZE": "67108864",
                    "TS_COMPACTION_WATERMARK_BYTES": "536870912",
                    "TS_COLD_SCAN_NO_CACHE_FILL": "false",
                    "TS_PAGE_INDEX_CACHE_BYTES": "2097152",
                    "TS_BLOCK_INDEX_CACHE_BYTES": "4194304",
                }
            )

            tuning = effective_storage_tuning_from_env()

            self.assertEqual(tuning["TS_CONTEXT_PAGE_TARGET_BYTES"], 131072)
            self.assertEqual(tuning["TS_BLOCK_SEGMENT_TARGET_BYTES"], 1073741824)
            self.assertEqual(tuning["TS_STORAGE_ZONE_SIZE"], 33554432)
            self.assertEqual(tuning["TS_STREAM_MAX_BLOB_SIZE"], 67108864)
            self.assertEqual(tuning["TS_COMPACTION_WATERMARK_BYTES"], 536870912)
            self.assertFalse(tuning["TS_COLD_SCAN_NO_CACHE_FILL"])
            self.assertEqual(tuning["TS_PAGE_INDEX_CACHE_BYTES"], 2097152)
            self.assertEqual(tuning["TS_BLOCK_INDEX_CACHE_BYTES"], 4194304)
            self.assertEqual(tuning["effective_block_segment_target_bytes"], 67108864)
        finally:
            for name, value in old_env.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    def test_storage_tuning_failures_detect_backend_omission_and_drift(self) -> None:
        tuning = self._storage_tuning()
        report = {
            "config": {"effective_storage_tuning": dict(tuning)},
            "backends": {
                "cpp": {"status": "passed", "effective_storage_tuning": dict(tuning)},
                "rust": {
                    "status": "passed",
                    "effective_storage_tuning": {
                        **tuning,
                        "TS_STREAM_MAX_BLOB_SIZE": 20971520,
                    },
                },
            },
        }

        failures = storage_tuning_failures(report)

        self.assertIn(
            "rust effective_storage_tuning.TS_STREAM_MAX_BLOB_SIZE drift: backend=20971520 config=10485760",
            failures,
        )
        del report["backends"]["cpp"]["effective_storage_tuning"]["TS_PAGE_INDEX_CACHE_BYTES"]
        failures = storage_tuning_failures(report)
        self.assertIn("cpp missing effective_storage_tuning.TS_PAGE_INDEX_CACHE_BYTES", failures)

    def test_storage_lifecycle_read_sequence_is_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_drift = json.loads(json.dumps(corpus["cpp"]))
        cpp_drift["storage_read_sequence"] = [
            "logical_key_timestamp_range",
            "page_read",
            "decode_records",
        ]
        failures = validate_report_pair(cpp_drift, corpus["rust"])
        self.assertTrue(any("cpp storage_read_sequence drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_read_sequence"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_read_sequence`", failures)

    def test_storage_lifecycle_write_sequence_is_required_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_drift = json.loads(json.dumps(corpus["cpp"]))
        cpp_drift["storage_write_sequence"] = [
            "append_record",
            "route_shard_slot",
            "flush_page_block_segment",
            "publish_append_watermark",
        ]
        failures = validate_report_pair(cpp_drift, corpus["rust"])
        self.assertTrue(any("cpp storage_write_sequence drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_write_sequence"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_write_sequence`", failures)

    def test_storage_lifecycle_cold_scan_sequence_is_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_drift = json.loads(json.dumps(corpus["cpp"]))
        cpp_drift["storage_cold_scan_sequence"] = [
            "timestamp_page_index_scan",
            "page_read",
            "bounded_decode",
            "hot_cache_promotion",
        ]
        failures = validate_report_pair(cpp_drift, corpus["rust"])
        self.assertTrue(any("cpp storage_cold_scan_sequence drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_cold_scan_sequence"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_cold_scan_sequence`", failures)

    def test_storage_lifecycle_phases_are_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_drift = json.loads(json.dumps(corpus["cpp"]))
        cpp_drift["storage_lifecycle_phases"] = [
            "prepare",
            "reclaim",
            "evict",
            "expire",
            "page_gc",
            "block_gc",
            "compact",
            "index_gc",
            "delayed_destroy",
            "follower_cursor_safety",
            "watermark_progress",
        ]
        failures = validate_report_pair(cpp_drift, corpus["rust"])
        self.assertTrue(any("cpp storage_lifecycle_phases drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_lifecycle_phases"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_lifecycle_phases`", failures)

    def test_storage_cache_layers_are_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_drift = json.loads(json.dumps(corpus["cpp"]))
        cpp_drift["storage_cache_layers"] = [
            "memory_object_cache",
            "page_index_cache",
            "block_index_cache",
            "disk_cache",
            "shared_store_read_through",
        ]
        failures = validate_report_pair(cpp_drift, corpus["rust"])
        self.assertTrue(any("cpp storage_cache_layers drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_cache_layers"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_cache_layers`", failures)

    def test_storage_cache_semantics_are_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        cpp_drift = json.loads(json.dumps(corpus["cpp"]))
        cpp_drift["storage_cache_semantics"] = [
            "lookup_hot_to_cold",
            "refill_from_durable_on_miss",
            "invalidate_on_append_watermark",
            "invalidate_on_compaction_watermark",
            "cold_scan_promote",
        ]
        failures = validate_report_pair(cpp_drift, corpus["rust"])
        self.assertTrue(any("cpp storage_cache_semantics drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_cache_semantics"]
        failures = validate_report_pair(corpus["cpp"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_cache_semantics`", failures)

    def test_storage_reclaim_metrics_and_scope_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        required_reclaim_metrics = [
            "tombstone_records",
            "stale_page_tombstones",
            "stale_block_tombstones",
            "stale_pages_rewritten",
            "stale_pages_skipped",
            "stale_blocks_rewritten",
            "stale_blocks_skipped",
            "reclaimable_bytes",
            "compaction_reclaimed_bytes",
            "physical_reclaimed_bytes",
            "physical_reclaim_errors",
        ]
        for metric in required_reclaim_metrics:
            rust_missing = json.loads(json.dumps(corpus["rust"]))
            del rust_missing["storage_lifecycle_metrics"][metric]
            failures = validate_report_pair(corpus["cpp"], rust_missing)
            self.assertIn(f"rust metrics missing `{metric}`", failures)

        cpp_bad_scope = json.loads(json.dumps(corpus["cpp"]))
        cpp_bad_scope["storage_reclaim_scope"] = {
            "owner": "matrixark_context_gc",
            "matrixark_context_gc_role": "owns_physical_reclaim",
            "physical_reclaim_context_specific": True,
        }
        failures = validate_report_pair(cpp_bad_scope, corpus["rust"])
        self.assertTrue(any("cpp storage_reclaim_scope drift" in failure for failure in failures))

        rust_missing_scope = json.loads(json.dumps(corpus["rust"]))
        del rust_missing_scope["storage_reclaim_scope"]
        failures = validate_report_pair(corpus["cpp"], rust_missing_scope)
        self.assertIn("rust report missing required top-level `storage_reclaim_scope`", failures)

    def test_storage_lifecycle_gap_fill_metrics_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["cpp"], corpus["rust"]), [])

        lifecycle_gap_metrics = {
            "StorageManager": [
                "storage_manager_prepare_count",
                "storage_manager_reclaim_count",
                "storage_manager_watermark_progress_count",
            ],
            "PageGc": ["storage_manager_page_gc_count"],
            "eviction": ["storage_manager_evict_count", "cache_evictions"],
            "expiration": ["storage_manager_expire_count"],
            "page_compaction": ["storage_manager_compaction_count", "compaction_reclaimed_bytes"],
            "stream_blob_rollover": ["stream_rollover_count", "segment_open_count", "segment_sealed_count"],
            "zones": ["storage_zone_total_bytes", "storage_zone_used_bytes", "storage_zone_stale_bytes"],
            "index_reclaim": [
                "storage_manager_index_gc_count",
                "page_index_rebuild_count",
                "block_index_rebuild_count",
                "object_index_rebuild_count",
            ],
        }

        for metrics in lifecycle_gap_metrics.values():
            for metric in metrics:
                cpp_missing = json.loads(json.dumps(corpus["cpp"]))
                del cpp_missing["storage_lifecycle_metrics"][metric]
                failures = validate_report_pair(cpp_missing, corpus["rust"])
                self.assertIn(f"cpp metrics missing `{metric}`", failures)

    def test_production_policy_gate_blocks_perf_claims_before_correct_refs(self) -> None:
        report = {
            "config": {
                "events": 1000,
                "dataset": "matrixark-scale-synthetic",
                "messages_per_ingest": 20,
                "batch_size": 20,
                "retrieve_workers": 4,
                "retrieve_queries": 16,
                "max_context_tokens": 12000,
                "storage_options": {"storage_family": "shared_store"},
                "topology": {"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "table": "deploy_table"},
                "embedding_provider": "hash",
                "embedding_model": "matrixark-local-token-hash-v1",
                "reader_provider": "deterministic",
                "reader_model": "matrixark-deterministic-reader",
                "judge_provider": "deterministic",
                "judge_model": "matrixark-deterministic-judge",
                "effective_storage_tuning": self._storage_tuning(),
            },
            "comparison": {"phase0_correctness": {"status": "failed"}},
            "backends": {
                "cpp": {
                    "status": "passed",
                    "effective_storage_tuning": self._storage_tuning(),
                    "retrieve": {
                        "stage_metrics": {
                            "selected_refs_max": 0,
                            "broad_scan_used_count": 1,
                            "python_pack_fallback_count": 1,
                            "stage_p95_ms": {"audit_ms": 9},
                        }
                    },
                },
                "rust": {
                    "status": "passed",
                    "effective_storage_tuning": self._storage_tuning(),
                    "retrieve": {"stage_metrics": {"selected_refs_max": 2, "stage_p95_ms": {"audit_ms": 0}}},
                },
            },
        }

        gate = production_policy_gate(report)

        self.assertEqual(gate["status"], "failed")
        blocker_names = {item["name"] for item in gate["blockers"]}
        self.assertIn("correctness_before_latency", blocker_names)
        self.assertIn("cpp_selected_refs_non_empty", blocker_names)
        self.assertIn("cpp_placement_index_driven", blocker_names)

    def test_production_policy_gate_fails_selected_refs_when_backend_not_passed(self) -> None:
        report = {
            "config": {
                "events": 10000,
                "dataset": "matrixark-scale-synthetic",
                "messages_per_ingest": 20,
                "batch_size": 20,
                "retrieve_workers": 16,
                "retrieve_queries": 128,
                "max_context_tokens": 12000,
                "storage_options": {"storage_family": "shared_store"},
                "topology": {"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "table": "deploy_table"},
                "embedding_provider": "hash",
                "embedding_model": "matrixark-local-token-hash-v1",
                "reader_provider": "deterministic",
                "reader_model": "matrixark-deterministic-reader",
                "judge_provider": "deterministic",
                "judge_model": "matrixark-deterministic-judge",
                "effective_storage_tuning": self._storage_tuning(),
            },
            "comparison": {"phase0_correctness": {"status": "failed"}},
            "backends": {
                "cpp": {
                    "status": "backend_startup_failed",
                    "effective_storage_tuning": self._storage_tuning(),
                    "retrieve": {"stage_metrics": {"selected_refs_max": 0, "stage_p95_ms": {"audit_ms": 0}}},
                },
                "rust": {
                    "status": "topology_not_ready",
                    "effective_storage_tuning": self._storage_tuning(),
                    "retrieve": {"stage_metrics": {"selected_refs_max": 0, "stage_p95_ms": {"audit_ms": 0}}},
                },
            },
        }

        gate = production_policy_gate(report)

        blocker_names = {item["name"] for item in gate["blockers"]}
        self.assertIn("cpp_selected_refs_non_empty", blocker_names)
        self.assertIn("rust_selected_refs_non_empty", blocker_names)

    def test_cpp_linux_so_preflight_rejects_windows_python(self) -> None:
        original = validate_cpp_runtime_host.__globals__["_is_windows_host"]
        validate_cpp_runtime_host.__globals__["_is_windows_host"] = lambda: True
        try:
            with self.assertRaisesRegex(RuntimeError, "invalid_host_platform"):
                validate_cpp_runtime_host("C:\\repo\\output-ubuntu22\\release\\sdk\\lib\\libbcache2.so")
        finally:
            validate_cpp_runtime_host.__globals__["_is_windows_host"] = original

    def test_rust_parity_preflight_requires_proxy_or_explicit_compat(self) -> None:
        with tempfile.TemporaryDirectory(prefix="matrixark-rust-cli-policy-") as tmpdir:
            release_dir = Path(tmpdir) / "target" / "release"
            release_dir.mkdir(parents=True)
            compat = release_dir / "matrixark_record_log"
            compat.write_text("", encoding="utf-8")
            proxy = release_dir / "matrixark_rust_proxy"
            proxy.write_text("", encoding="utf-8")

            args = argparse.Namespace(
                rust_cli=str(compat),
                allow_rust_record_log_compat=False,
                allow_rust_debug_cli=False,
            )
            with self.assertRaisesRegex(RuntimeError, "matrixark_rust_proxy"):
                validate_rust_runtime_path(args)

            args.allow_rust_record_log_compat = True
            validate_rust_runtime_path(args)

            args.rust_cli = str(proxy)
            args.allow_rust_record_log_compat = False
            validate_rust_runtime_path(args)

    def test_rust_parity_preflight_rejects_debug_cli_by_default(self) -> None:
        with tempfile.TemporaryDirectory(prefix="matrixark-rust-cli-policy-") as tmpdir:
            debug_dir = Path(tmpdir) / "target" / "debug"
            debug_dir.mkdir(parents=True)
            proxy = debug_dir / "matrixark_rust_proxy"
            proxy.write_text("", encoding="utf-8")
            args = argparse.Namespace(
                rust_cli=str(proxy),
                allow_rust_record_log_compat=False,
                allow_rust_debug_cli=False,
            )

            with self.assertRaisesRegex(RuntimeError, "debug artifacts"):
                validate_rust_runtime_path(args)

            args.allow_rust_debug_cli = True
            validate_rust_runtime_path(args)

    def test_production_policy_gate_passes_native_index_driven_context_path(self) -> None:
        metrics = {
            "selected_refs_max": 3,
            "broad_scan_used_count": 0,
            "python_pack_fallback_count": 0,
            "raw_candidate_tables_returned_count": 0,
            "stage_p95_ms": {"audit_ms": 0},
        }
        report = {
            "config": {
                "events": 1000,
                "dataset": "matrixark-scale-synthetic",
                "messages_per_ingest": 20,
                "batch_size": 20,
                "retrieve_workers": 4,
                "retrieve_queries": 16,
                "max_context_tokens": 12000,
                "storage_options": {"storage_family": "shared_store"},
                "topology": {"metaserver": "127.0.0.1:18000", "namespace": "deploy_ns", "table": "deploy_table"},
                "embedding_provider": "hash",
                "embedding_model": "matrixark-local-token-hash-v1",
                "reader_provider": "deterministic",
                "reader_model": "matrixark-deterministic-reader",
                "judge_provider": "deterministic",
                "judge_model": "matrixark-deterministic-judge",
                "effective_storage_tuning": self._storage_tuning(),
            },
            "comparison": {"phase0_correctness": {"status": "passed"}},
            "backends": {
                "cpp": {
                    "status": "passed",
                    "effective_storage_tuning": self._storage_tuning(),
                    "retrieve": {"stage_metrics": dict(metrics)},
                },
                "rust": {
                    "status": "passed",
                    "effective_storage_tuning": self._storage_tuning(),
                    "retrieve": {"stage_metrics": dict(metrics)},
                },
            },
        }

        gate = production_policy_gate(report)

        self.assertEqual(gate["status"], "passed")
        self.assertEqual(gate["blockers"], [])

    def test_production_policy_gate_blocks_perf_claim_without_reader_judge_model_config(self) -> None:
        metrics = {
            "selected_refs_max": 3,
            "broad_scan_used_count": 0,
            "python_pack_fallback_count": 0,
            "raw_candidate_tables_returned_count": 0,
            "stage_p95_ms": {"audit_ms": 0},
        }
        report = {
            "config": {
                "events": 1000,
                "messages_per_ingest": 20,
                "retrieve_workers": 4,
                "retrieve_queries": 16,
                "max_context_tokens": 12000,
                "storage_options": {"storage_family": "shared_store"},
            },
            "comparison": {"phase0_correctness": {"status": "passed"}},
            "backends": {
                "cpp": {"status": "passed", "retrieve": {"stage_metrics": dict(metrics)}},
                "rust": {"status": "passed", "retrieve": {"stage_metrics": dict(metrics)}},
            },
        }

        gate = production_policy_gate(report)

        self.assertEqual(gate["status"], "failed")
        blocker_names = {item["name"] for item in gate["blockers"]}
        self.assertIn("same_dataset_storage_topology_budget_batch_models", blocker_names)

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
            "retrieve": {
                "qps": 100,
                "p50_ms": 1,
                "p95_ms": 1,
                "p99_ms": 1,
                "selected_refs_avg": 4,
                "stage_metrics": {
                    "selected_refs_avg": 4,
                    "selected_refs_max": 4,
                    "correctness_evidence": dict(_SHARED_CORRECTNESS_EVIDENCE),
                    "selected_ref_signatures_by_query": {"0": ["event:1", "resource_chunk:2"]},
                },
            },
        }
        rust = json.loads(json.dumps(base))
        rust["retrieve"]["stage_metrics"]["selected_ref_signatures_by_query"] = {"0": ["event:1"]}

        result = comparison(base, rust)

        self.assertEqual(result["status"], "failed")
        self.assertTrue(
            any(failure["reason"] == "selected_ref_set_mismatch" for failure in result["phase0_correctness"]["failures"])
        )

    def test_scale_report_allows_selected_ref_ordering_drift_for_same_refs(self) -> None:
        base = {
            "status": "passed",
            "raw_storage": {"write": {"record_qps": 100, "p95_ms": 1}, "read": {"qps": 100, "p95_ms": 1}},
            "ingest_messages": {"message_qps": 100},
            "ingest": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1},
            "retrieve": {
                "qps": 100,
                "p50_ms": 1,
                "p95_ms": 1,
                "p99_ms": 1,
                "selected_refs_avg": 2,
                "stage_metrics": {
                    "selected_refs_avg": 2,
                    "selected_refs_max": 2,
                    "correctness_evidence": dict(_SHARED_CORRECTNESS_EVIDENCE),
                    "selected_ref_signatures_by_query": {"0": ["entity:7", "event:1"]},
                },
            },
        }
        rust = json.loads(json.dumps(base))
        rust["retrieve"]["stage_metrics"]["selected_ref_signatures_by_query"] = {"0": ["event:1", "entity:7"]}

        result = comparison(base, rust)

        self.assertEqual(result["phase0_correctness"]["status"], "passed")
        self.assertTrue(result["phase0_correctness"]["backend_values"]["cpp"]["correctness_evidence"]["selected_ref_parity"])
        self.assertTrue(result["phase0_correctness"]["backend_values"]["rust"]["correctness_evidence"]["selected_ref_parity"])

    def test_scale_report_rejects_empty_selected_ref_parity(self) -> None:
        base = {
            "status": "passed",
            "raw_storage": {"write": {"record_qps": 100, "p95_ms": 1}, "read": {"qps": 100, "p95_ms": 1}},
            "ingest_messages": {"message_qps": 100},
            "ingest": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1},
            "retrieve": {
                "qps": 100,
                "p50_ms": 1,
                "p95_ms": 1,
                "p99_ms": 1,
                "selected_refs_avg": 0,
                "stage_metrics": {
                    "selected_refs_avg": 0,
                    "selected_refs_max": 0,
                    "correctness_evidence": dict(_SHARED_CORRECTNESS_EVIDENCE),
                    "selected_ref_signatures_by_query": {"0": []},
                },
            },
        }
        rust = json.loads(json.dumps(base))

        result = comparison(base, rust)

        self.assertEqual(result["phase0_correctness"]["status"], "failed")
        self.assertFalse(result["phase0_correctness"]["backend_values"]["cpp"]["correctness_evidence"]["selected_ref_parity"])
        self.assertFalse(result["phase0_correctness"]["backend_values"]["rust"]["correctness_evidence"]["selected_ref_parity"])
        selected_row = next(row for row in result["rows"] if row["metric"] == "selected_refs_avg")
        self.assertFalse(selected_row["parity_passed"])
        self.assertIn(">= 1", selected_row["parity_threshold"])

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
                    "append_queue_wait_ms": 0.25,
                    "append_engine_ms": 0.75,
                    "selected_refs": 2,
                    "dropped_refs": 1,
                    "scanned_records": 10,
                    "candidate_cache_hit": True,
                    "index_postings_read": 3,
                    "placement_partitions_touched": 1,
                    "timeout_count": 2,
                    "fallback_flags": [
                        "native_context_pack",
                        "native_prefilter_no_match_broad_scan_blocked",
                    ],
                    "correctness_evidence": dict(_SHARED_CORRECTNESS_EVIDENCE),
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
        self.assertEqual(stage_metrics["stage_p95_ms"]["append_queue_wait_ms"], 0.25)
        self.assertEqual(stage_metrics["stage_p95_ms"]["append_engine_ms"], 0.75)
        self.assertEqual(stage_metrics["index_postings_read_avg"], 3.0)
        self.assertEqual(stage_metrics["timeout_count"], 2)
        self.assertEqual(
            stage_metrics["fallback_flags_total"]["native_prefilter_no_match_broad_scan_blocked"],
            1,
        )
        self.assertTrue(any(row["metric"] == "pack_p95_ms" for row in result["rows"]))
        self.assertTrue(any(row["metric"] == "native_timeout_count" for row in result["rows"]))
        self.assertTrue(any(row["metric"] == "append_engine_p95_ms" for row in result["rows"]))
        self.assertTrue(any(row["metric"] == "cache_hit_rate" for row in result["rows"]))
        self.assertTrue(result["status_labels"]["feature_correct"])
        self.assertTrue(result["status_labels"]["performance_candidate"])
        self.assertTrue(result["status_labels"]["production_performance_parity"])
        self.assertEqual(result["rust_vs_cpp_parity"]["feature_parity"]["status"], "passed")
        self.assertTrue(result["rust_vs_cpp_parity"]["feature_parity"]["passed"])
        self.assertEqual(result["rust_vs_cpp_parity"]["performance_parity"]["status"], "passed")
        self.assertTrue(result["rust_vs_cpp_parity"]["performance_parity"]["passed"])
        self.assertTrue(result["rust_vs_cpp_parity"]["production_performance_parity"]["passed"])

    def test_scale_report_requires_shared_correctness_evidence(self) -> None:
        base = {
            "status": "passed",
            "raw_storage": {"write": {"record_qps": 100, "p95_ms": 1}, "read": {"qps": 100, "p95_ms": 1}},
            "ingest_messages": {"message_qps": 100},
            "ingest": {"p50_ms": 1, "p95_ms": 1, "p99_ms": 1},
            "retrieve": {
                "qps": 100,
                "p50_ms": 1,
                "p95_ms": 1,
                "p99_ms": 1,
                "selected_refs_avg": 2,
                "stage_metrics": {
                    "selected_refs_avg": 2,
                    "selected_refs_max": 2,
                    "correctness_evidence": {
                        **_SHARED_CORRECTNESS_EVIDENCE,
                        "scope_filtering": False,
                        "compact_secondary_index_prefilter": False,
                    },
                    "selected_ref_signatures_by_query": {"0": ["event:1", "entity:7"]},
                },
            },
        }
        rust = json.loads(json.dumps(base))

        result = comparison(base, rust)

        self.assertEqual(result["status"], "failed")
        missing = [
            failure["requirement"]
            for failure in result["phase0_correctness"]["failures"]
            if failure["reason"] == "missing_correctness_evidence"
        ]
        self.assertIn("scope_filtering", missing)
        self.assertIn("compact_secondary_index_prefilter", missing)

    def test_scale_report_counts_timeouts_and_fallback_flags(self) -> None:
        self.assertEqual(timeout_count(["request timed out", "Slot not found", "timeout waiting for response"]), 2)
        flags = fallback_flags_from_backend(
            {
                "status": "failed",
                "retrieve": {"partial_context_packs": 1, "stage_metrics": {"samples": 0}},
                "errors": {"ingest": ["memory fallback used"], "retrieve": []},
                "backend_metrics": {"result": {"embedding_fallback_used": True}},
            }
        )

        self.assertTrue(flags["memory_fallback"])
        self.assertTrue(flags["hash_embedding_fallback"])
        self.assertTrue(flags["partial_context_pack"])
        self.assertTrue(flags["native_metrics_missing"])

    def _args(self, backend: str) -> argparse.Namespace:
        return argparse.Namespace(backend=backend)

    def test_context_pack_access_view_omits_hashes(self) -> None:
        access = mcp.MatrixArkMcpServer._serving_access(
            {
                "_matrixark_auth": {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "deeproute",
                    "session_id": "debug-message-pdf-session",
                    "agent_name": "codex",
                    "api_key_id": "dev",
                    "role": "dev_admin",
                    "mode": "dev",
                    "scope_key": "t=1|u=2|s=3|",
                    "tenant_hash": 1,
                    "user_hash": 2,
                    "session_hash": 3,
                }
            }
        )
        self.assertEqual("acct_local", access.get("account_id"))
        self.assertEqual("tenant_codex", access.get("tenant_id"))
        self.assertEqual("deeproute", access.get("user_id"))
        self.assertEqual("debug-message-pdf-session", access.get("session_id"))
        self.assertNotIn("scope_key", access)
        self.assertNotIn("tenant_hash", access)
        self.assertNotIn("user_hash", access)
        self.assertNotIn("session_hash", access)

    def test_message_event_secondary_indexes_are_not_written_per_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            adapter.ingest(
                {
                    "messages": [{"role": "user", "content": "Alice approved the GPU request."}],
                    "scope": {
                        "account_id": "acct_local",
                        "tenant_id": "tenant_codex",
                        "user_id": "codex_user",
                        "session_id": "secondary-index-cap",
                    },
                }
            )
            records = adapter.read_all()
            event_indexes = [
                record
                for record in records
                if record.get("record_type") == "context_index" and record.get("data_model") == "context_event"
            ]
            self.assertEqual([], event_indexes)

    def test_secondary_index_budget_caps_total_operation_terms(self) -> None:
        budget = mcp.new_secondary_index_budget(3)
        first = mcp.take_secondary_index_terms(
            [
                "source_type:resource",
                "resource_type:pdf",
                "keyword:gpu",
                "keyword:budget",
            ],
            budget,
        )
        second = mcp.take_secondary_index_terms(
            [
                "event_type:approval",
                "entity_type:owner",
            ],
            budget,
        )

        self.assertEqual(first, ["source_type:resource", "resource_type:pdf", "keyword:gpu"])
        self.assertEqual(second, [])
        self.assertEqual(
            mcp.secondary_index_budget_summary(budget),
            {
                "index_total_cap": 3,
                "index_emitted_count": 3,
                "index_dropped_by_total_cap_count": 3,
            },
        )

    def test_approval_state_entity_name_uses_stable_subject_not_full_state(self) -> None:
        self.assertEqual(
            mcp.canonical_entity_name("approval_state", "Project Aurora GPU procurement after Q3 budget review"),
            "Project Aurora GPU procurement",
        )
        self.assertEqual(
            mcp.canonical_entity_name("approval_state", "the Project Aurora GPU procurement after finance review"),
            "Project Aurora GPU procurement",
        )
        self.assertEqual(
            mcp.canonical_entity_name("approval_state", "attachment is required before vendor selection"),
            "attachment",
        )
        self.assertEqual(
            mcp.canonical_entity_name("approval_state", "attachment as a blocker before vendor selection"),
            "attachment",
        )

    def test_openai_compatible_batch_extraction_uses_model_entities(self) -> None:
        extraction_globals = mcp.one_pass_memory_extraction.__globals__
        old_call = extraction_globals["openai_compatible_json_call"]
        self.addCleanup(lambda: extraction_globals.__setitem__("openai_compatible_json_call", old_call))

        def fake_json_call(*, system: str, user: str) -> dict:
            return {
                "classification": "BATCH_MEMORY",
                "event_type": "approval_state",
                "batch_summary": "Project Aurora GPU procurement is approved with Bob as owner.",
                "entities": [
                    {
                        "entity_type": "approval_state",
                        "entity_name": "Project Aurora GPU procurement",
                        "state": "Approved by Alice after Q3 budget review.",
                        "confidence": 0.93,
                        "operator": "LLM_MERGE",
                    },
                    {
                        "entity_type": "resource_owner",
                        "entity_name": "Project Aurora procurement owner",
                        "state": "Bob owns procurement and vendor coordination.",
                        "confidence": 0.9,
                    },
                ],
                "segments": [
                    {
                        "topic": "project_aurora_gpu",
                        "coordinate_tuples": [[0, 1]],
                        "message_indexes": [0, 1],
                        "saliency_score": 0.95,
                        "summary_text": "Approval and owner for Project Aurora GPU procurement.",
                    }
                ],
                "indexes": ["event_type:approval_state", "entity_type:approval_state"],
            }

        extraction_globals["openai_compatible_json_call"] = fake_json_call
        result = mcp.one_pass_memory_extraction(
            {
                "kind": "message",
                "messages": [
                    {"role": "user", "content": "Alice approved Project Aurora GPU procurement."},
                    {"role": "assistant", "content": "Bob owns procurement follow-up."},
                ],
                "scope": {},
                "metadata": {},
                "source_event_ids": ["evt-1", "evt-2"],
                "extraction_provider": "openai-compatible",
                "understanding_provider": "openai-compatible",
            },
            prior_context={"level": "", "refs": [], "messages": [], "summaries": []},
        )

        self.assertEqual(result["mode"], "matrixark_one_pass_schema_openai_compatible")
        self.assertEqual(result["segment_provider"]["provider"], "openai_compatible")
        self.assertEqual(result["entities"][0]["extracted_by"], "openai_compatible")
        self.assertEqual(result["entities"][0]["entity_name"], "Project Aurora GPU procurement")
        self.assertEqual(result["entities"][0]["state"], "Approved by Alice after Q3 budget review.")
        self.assertNotEqual(result["entities"][0]["entity_name"], result["entities"][0]["state"])

    def test_openai_compatible_resource_fact_extraction_uses_model_facts(self) -> None:
        extraction_globals = mcp.extract_resource_facts.__globals__
        old_call = extraction_globals["openai_compatible_json_call"]
        self.addCleanup(lambda: extraction_globals.__setitem__("openai_compatible_json_call", old_call))

        def fake_json_call(*, system: str, user: str) -> dict:
            return {
                "facts": [
                    {
                        "event_type": "resource_decision",
                        "entity_type": "resource_decision",
                        "entity_name": "Project Aurora GPU approval",
                        "value": "Alice approved the GPU purchase after finance review.",
                        "confidence": 0.91,
                    }
                ]
            }

        extraction_globals["openai_compatible_json_call"] = fake_json_call
        chunk = SimpleNamespace(
            text="Alice approved the GPU purchase after finance review.",
            metadata={"heading": "Approval Packet"},
            chunk_hash="chunk-123",
            source_ref="gpu.pdf#page=1",
        )
        facts = mcp.extract_resource_facts(
            chunk,
            chunk_metadata={"heading": "Approval Packet"},
            envelope={"extraction_provider": "openai-compatible", "understanding_provider": "openai-compatible"},
            raw_uri="/tmp/gpu.pdf",
            resource_version="v1",
        )

        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["mode"], "matrixark_resource_schema_openai_compatible")
        self.assertEqual(facts[0]["extraction_provider"], "openai_compatible")
        self.assertEqual(facts[0]["entity_name"], "Project Aurora GPU approval")
        self.assertEqual(facts[0]["value"], "Alice approved the GPU purchase after finance review.")
        self.assertNotEqual(facts[0]["entity_name"], facts[0]["value"])

    def test_context_pack_serving_refs_drop_debug_index_and_hash_fields(self) -> None:
        ref = {
            "ref_type": "resource_chunk",
            "ref_hash": 123,
            "node_hash": 456,
            "node_path": ["tenant", "shared", "resources"],
            "score": 0.812345,
            "matched_index_terms": ["keyword:gpu", "resource_type:pdf"],
            "keyword_score": 2,
            "embedding_score": 0.91,
            "text": "Alice approved the GPU purchase after finance review.",
            "source_ref": "docs/gpu.pdf#page=2",
            "raw_uri": "s3://bucket/docs/gpu.pdf",
            "resource_type": "pdf",
            "metadata": {
                "unit_kind": "pdf_page",
                "page": 2,
                "keywords": ["gpu", "approval", "finance"],
                "content_hash": "abcdef",
                "relative_path": "docs/gpu.pdf",
            },
        }
        compact = mcp.compact_context_pack_refs([ref])[0]

        self.assertEqual(compact["ref_type"], "resource_chunk")
        self.assertEqual(compact["text"], ref["text"])
        self.assertEqual(compact["source_ref"], "docs/gpu.pdf#page=2")
        self.assertNotIn("score", compact)
        self.assertNotIn("token_estimate", compact)
        self.assertNotIn("raw_uri", compact)
        self.assertNotIn("ref_hash", compact)
        self.assertNotIn("node_hash", compact)
        self.assertNotIn("node_path", compact)
        self.assertNotIn("matched_index_terms", compact)
        self.assertNotIn("keyword_score", compact)
        self.assertNotIn("embedding_score", compact)
        self.assertNotIn("keywords", compact.get("metadata", {}))
        self.assertNotIn("content_hash", compact.get("metadata", {}))
        self.assertEqual(compact["metadata"], {"unit_kind": "pdf_page", "relative_path": "docs/gpu.pdf", "page": 2})

    def test_context_pack_serving_top_level_omits_operational_cache_fields(self) -> None:
        compact = mcp.compact_context_pack_for_serving({
            "context_pack_id": "pack-123",
            "context_pack_assembly": "native_cpp_direct",
            "context_pack_cache_hit": False,
            "cache_hit": False,
            "cache_hit_used": False,
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "scan_count": 42,
            "selected_refs": [],
            "remote_context_refs": [],
        })

        self.assertEqual(compact["pack_id"], "pack-123")
        for field in [
            "assembly",
            "cache_hit",
            "cache_hit_used",
            "context_pack_id",
            "context_pack_assembly",
            "context_pack_cache_hit",
            "native_pack_assembly",
            "raw_records_returned",
            "python_hot_path_records",
            "scan_count",
        ]:
            self.assertNotIn(field, compact)

    def test_context_pack_serving_omits_duplicate_and_operational_fields(self) -> None:
        ref = {"ref_type": "event", "text": "Alice approved the GPU request."}
        compact = mcp.compact_context_pack_for_serving({
            "context_pack_id": "pack-123",
            "selected_refs": [ref],
            "remote_context_refs": [ref],
            "selected_ref_counts": {"event": 1},
            "context_sources_order": ["local_context", "matrixark_remote_context"],
            "primary_candidate_count": 10,
            "auxiliary_candidate_count": 2,
            "budget_source": "agent_provided_max_context_tokens",
            "local_context_safety_margin_tokens": 70,
            "remote_context_budget_tokens": 1330,
            "request_deadline_ms": 120000,
            "request_elapsed_ms": 25.15,
            "used_context_tokens": 12,
            "used_remote_context_tokens": 12,
            "used_local_context_tokens": 0,
            "total_prompt_context_tokens": 12,
            "local_context_policy": {
                "local_context_count": 0,
                "local_context_tokens": 0,
                "safety_margin_tokens": 70,
                "remote_is_additive_only_within_remaining_budget": True,
            },
            "recall_policy": {
                "query_plan": {"query_type": "fact", "temporal_window": {"mode": "latest"}},
                "tree_traversal": {"enabled": True, "selected_node_count": 3},
            },
            "dropped_refs": {"duplicate": 3},
            "insufficient_context": False,
            "partial_context_pack": False,
            "packing_policy": "question_type_aware:fact",
            "requested_max_context_tokens": 1000,
            "context_pack_cache_hit": True,
            "cache_hit": True,
            "cache_hit_used": True,
            "context_pack_assembly": "native_cpp_direct",
            "assembly": "native_cpp_direct",
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "scan_count": 50,
            "selected_ref_count": 1,
            "dropped_ref_count": 0,
        })

        self.assertEqual(compact["selected_refs"], [{"ref_type": "event", "text": "Alice approved the GPU request."}])
        self.assertNotIn("remote_context_refs", compact)
        for field in [
            "selected_ref_counts",
            "context_sources_order",
            "primary_candidate_count",
            "auxiliary_candidate_count",
            "budget_source",
            "local_context_safety_margin_tokens",
            "remote_context_budget_tokens",
            "request_deadline_ms",
            "request_elapsed_ms",
            "used_remote_context_tokens",
            "used_local_context_tokens",
            "total_prompt_context_tokens",
            "local_context_policy",
            "recall_policy",
            "dropped_refs",
            "insufficient_context",
            "partial_context_pack",
            "packing_policy",
            "requested_max_context_tokens",
            "context_pack_cache_hit",
            "cache_hit",
            "cache_hit_used",
            "context_pack_assembly",
            "assembly",
            "native_pack_assembly",
            "raw_records_returned",
            "python_hot_path_records",
            "scan_count",
            "selected_ref_count",
            "dropped_ref_count",
        ]:
            self.assertNotIn(field, compact)
        self.assertEqual(compact.get("recall"), {"temporal": "latest"})

    def test_context_pack_serving_dropped_policy_omits_numeric_knobs(self) -> None:
        compact = mcp.compact_dropped_refs_for_context_pack({
            "cross_session_budget": 2,
            "cross_session_policy": {
                "enabled": True,
                "mode": "prefer",
                "decision": "always_consider_same_user_cross_session_when_session_scope_prefer",
                "budget_ratio": 0.2,
                "budget_tokens": 1536,
                "max_budget_tokens": 1536,
                "max_candidates": 24,
                "max_sessions": 3,
                "parallelism": 4,
                "selected_ref_count": 0,
                "selected_tokens": 0,
            },
        })

        policy = compact["cross_session_policy"]
        self.assertTrue(policy["enabled"])
        self.assertEqual(policy["mode"], "prefer")
        self.assertIn("decision", policy)
        self.assertNotIn("budget_ratio", policy)
        self.assertNotIn("budget_tokens", policy)
        self.assertNotIn("max_budget_tokens", policy)
        self.assertNotIn("max_candidates", policy)
        self.assertNotIn("max_sessions", policy)
        self.assertNotIn("parallelism", policy)
        self.assertNotIn("selected_ref_count", policy)
        self.assertNotIn("selected_tokens", policy)

    def test_replay_returns_compact_context_pack_scope_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            adapter.append_many([
                {
                    "record_type": "context_pack_audit",
                    "context_pack_id": "pack-1",
                    "query": "gpu approval",
                    "selected_refs": [{"ref_type": "resource_chunk", "text_preview": "gpu approved", "ref_hash": 1}],
                    "dropped_refs": {
                        "over_budget": 2,
                        "refs": [{"ref_type": "event", "ref_hash": 99, "text_preview": "debug"}],
                        "reason_descriptions": {"over_budget": "verbose explanation"},
                    },
                    "recall_policy": {
                        "query_plan": {"query_type": "fact", "secondary_filters": {"keyword": ["gpu"]}},
                        "tree_traversal": {"enabled": True, "selected_node_count": 2, "trace": ["verbose"]},
                        "secondary_index_filter": {"enabled": True, "matched_candidate_count": 3},
                        "rerank": {"enabled": True, "mode": "question_type_token_efficiency"},
                    },
                    "layer_scores": [{"verbose": True}],
                    "used_remote_context_tokens": 42,
                    "created_at_ms": 10,
                },
                {"record_type": "context_pack_audit", "context_pack_id": "pack-2", "query": "other", "created_at_ms": 11},
                {"record_type": "context_pack_telemetry", "context_pack_id": "pack-1", "selected_ref_count": 1, "dropped_ref_count": 2},
                {"record_type": "context_event", "context_pack_id": "unrelated", "event_id_hash": 7, "text": "not replayed"},
            ])

            replay = adapter.replay({"pack_id": "pack-1"})
            self.assertEqual(replay["replay_payload_policy"], "compact_context_pack_scope")
            self.assertTrue(replay["events"])
            self.assertTrue(all(row.get("context_pack_id") == "pack-1" for row in replay["events"]))
            audit = next(row for row in replay["events"] if row.get("record_type") == "context_pack_audit")
            self.assertNotIn("recall_policy", audit)
            self.assertNotIn("layer_scores", audit)
            self.assertNotIn("refs", audit.get("dropped_refs", {}))
            self.assertNotIn("reason_descriptions", audit.get("dropped_refs", {}))
            self.assertEqual(audit["dropped_refs"]["over_budget"], 2)
            self.assertTrue(audit["dropped_refs"]["dropped_ref_detail_available_in_audit"])
            self.assertIn("recall_policy_summary", audit)

    def test_serving_records_store_compact_scope_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = mcp.enrich_scope_with_identity(
                {"session_id": "debug-message-pdf-session"},
                {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "codex_user",
                    "session_id": "",
                    "agent_name": "codex",
                    "mode": "dev",
                },
            )
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_hash": 1,
                        "node_hash": 10,
                        "summary_text": "summary",
                        "node_path": ["tenant:tenant_codex", "user:codex_user"],
                        "depth": 2,
                        "scope": scope,
                        "created_at_ms": 1780000000000,
                        "updated_at_ms": 1780000000000,
                    },
                    {
                        "record_type": "context_embedding",
                        "embedding_type": "node_l0",
                        "ref_type": "node",
                        "ref_hash": 10,
                        "node_hash": 10,
                        "vector": [0.1],
                        "dim": 1,
                        "model": "matrixark-local-token-hash-v1",
                        "scope": scope,
                    },
                    {
                        "record_type": "resource_manifest",
                        "resource_hash": 2,
                        "raw_uri": "local://policy.pdf",
                        "access_scope": mcp.registry_access_scope(scope),
                        "scope": scope,
                    },
                    {
                        "record_type": "resource_chunk",
                        "chunk_hash": 3,
                        "node_hash": 10,
                        "text": "chunk",
                        "access_scope": mcp.registry_access_scope(scope),
                        "scope": scope,
                    },
                ]
            )

            compacted = adapter.read_all()
            self.assertTrue(compacted)
            for record in compacted:
                if record.get("record_type") == "context_debug_record":
                    continue
                self.assertEqual(scope["scope_key"], record.get("scope_key"))
                self.assertNotIn("scope", record)
                self.assertNotIn("_explicit_scope_keys", str(record))
                if record.get("record_type") == "context_summary":
                    self.assertNotIn("created_at_ms", record)
                    self.assertNotIn("depth", record)
                    self.assertEqual(1780000000000, record.get("updated_at_ms"))
                if record.get("record_type") == "context_embedding":
                    self.assertNotIn("dim", record)
                    self.assertNotIn("model", record)
                    self.assertEqual(mcp.stable_hash("embedding_model:matrixark-local-token-hash-v1"), record.get("model_hash"))
            self.assertTrue(
                mcp.scope_matches(mcp.candidate_access_scope(compacted[0]), scope),
                "compacted scope_key should still satisfy scoped retrieval",
            )

    def test_context_events_do_not_duplicate_summaries_or_embeddings(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            result = adapter.ingest(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": "Alice approved the Project Aurora GPU request after finance review.",
                        }
                    ],
                    "scope": {
                        "account_id": "acct_local",
                        "tenant_id": "tenant_codex",
                        "user_id": "codex_user",
                        "session_id": "summary-split-test",
                    },
                }
            )
            self.assertEqual("accepted", result["status"])
            records = adapter.read_all()
            events = [record for record in records if record.get("record_type") == "context_event"]
            summaries = [record for record in records if record.get("record_type") == "context_summary"]
            embeddings = [record for record in records if record.get("record_type") == "context_embedding"]

            self.assertTrue(events)
            self.assertTrue(summaries)
            self.assertTrue(embeddings)
            self.assertTrue(any(record.get("summary_type") == "session_l0" for record in summaries))
            self.assertTrue(any(record.get("embedding_type") == "session_l0" for record in embeddings))
            self.assertTrue(any(record.get("embedding_type") == "event_text" for record in embeddings))
            for event in events:
                self.assertNotIn("summary_text", event)
                self.assertNotIn("summary_embedding", event)
                self.assertNotIn("vector", event)
                self.assertNotIn("embedding", event)
            for summary in summaries:
                self.assertIn("summary_text", summary)
                self.assertNotIn("vector", summary)
            for embedding in embeddings:
                self.assertIn("vector", embedding)
                self.assertNotIn("summary_text", embedding)

    def test_context_node_topology_records_store_compact_scope_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = mcp.enrich_scope_with_identity(
                {"session_id": "debug-message-pdf-session"},
                {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "codex_user",
                    "session_id": "",
                    "agent_name": "codex",
                    "mode": "dev",
                },
            )

            adapter.ensure_context_node_path(
                node_path=["tenant:tenant_codex", "user:codex_user", "session:debug-message-pdf-session"],
                scope=scope,
                updated_at_ms=1780000000000,
            )

            topology_records = [
                record
                for record in adapter.read_all()
                if record.get("record_type") in {"context_node", "context_child_ref"}
            ]
            self.assertTrue(topology_records)
            for record in topology_records:
                self.assertEqual(scope["scope_key"], record.get("scope_key"))
                self.assertEqual(1780000000000, record.get("updated_at_ms"))
                self.assertNotIn("created_at_ms", record)
                self.assertNotIn("depth", record)
                self.assertNotIn("scope", record)
                for duplicate_field in (
                    "account_id",
                    "tenant_id",
                    "user_id",
                    "session_id",
                    "tenant_hash",
                    "user_hash",
                    "session_hash",
                ):
                    self.assertNotIn(duplicate_field, record)
                if record.get("record_type") == "context_node":
                    self.assertNotIn("node_name", record)
                if record.get("record_type") == "context_child_ref":
                    self.assertNotIn("parent_path", record)
                    self.assertNotIn("child_path", record)
                    self.assertNotIn("child_name", record)

    def test_parent_summary_uses_child_summaries_and_state_not_recursive_raw_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "events.jsonl")
            scope = mcp.enrich_scope_with_identity(
                {"session_id": "parent-summary-session"},
                {
                    "account_id": "acct_local",
                    "tenant_id": "tenant_codex",
                    "user_id": "codex_user",
                    "session_id": "",
                    "agent_name": "codex",
                    "mode": "dev",
                },
            )
            parent_path = ["tenant:tenant_codex", "user:codex_user"]
            child_path = [*parent_path, "session:parent-summary-session"]
            parent_hash = mcp.stable_hash("/".join(parent_path))
            child_hash = mcp.stable_hash("/".join(child_path))
            child_summary_hash = mcp.stable_hash("child-summary")
            entity_hash = mcp.stable_hash("gpu-approval-entity")
            compression_hash = mcp.stable_hash("old-approval-compression")
            dirty_hash = mcp.stable_hash("parent-summary-dirty")

            adapter.ensure_context_node_path(
                node_path=child_path,
                scope=scope,
                updated_at_ms=1780000000000,
            )
            adapter.append_many(
                [
                    {
                        "record_type": "context_summary",
                        "summary_type": "session_l0",
                        "summary_hash": child_summary_hash,
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "summary_text": "Child summary says finance approved the GPU budget.",
                        "scope": scope,
                        "updated_at_ms": 1780000000100,
                    },
                    {
                        "record_type": "context_entity",
                        "entity_hash": entity_hash,
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "entity_type": "approval_state",
                        "entity_name": "gpu_purchase",
                        "state": "Alice approved the GPU purchase after finance review.",
                        "scope": scope,
                        "updated_at_ms": 1780000000200,
                    },
                    {
                        "record_type": "context_compression_event",
                        "compression_id_hash": compression_hash,
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "operator": "TIME_COMPRESS",
                        "summary_text": "Older compressed context covers earlier GPU review notes.",
                        "scope": scope,
                        "updated_at_ms": 1780000000300,
                    },
                    {
                        "record_type": "context_event",
                        "event_id_hash": mcp.stable_hash("raw-leaf-event"),
                        "node_hash": child_hash,
                        "node_path": child_path,
                        "text": "RAW_LEAF_SHOULD_NOT_APPEAR_IN_PARENT_SUMMARY",
                        "scope": scope,
                        "updated_at_ms": 1780000000400,
                    },
                    {
                        "record_type": "context_summary_dirty",
                        "dirty_hash": dirty_hash,
                        "node_hash": parent_hash,
                        "node_path": parent_path,
                        "dirty_reason": "child_update",
                        "source_ref_type": "summary",
                        "source_summary_hash": child_summary_hash,
                        "scope": scope,
                        "status": "pending",
                        "updated_at_ms": 1780000000500,
                    },
                ]
            )

            result = adapter.refresh_dirty_node_summaries(scope=scope, limit=4, refreshed_at_ms=1780000000600)
            self.assertEqual("ok", result["status"])
            self.assertEqual(1, result["refreshed_count"])

            records = adapter.read_all()
            parent_summaries = [
                record
                for record in records
                if record.get("record_type") == "context_summary"
                and record.get("node_hash") == parent_hash
                and record.get("dirty_hash") == dirty_hash
            ]
            self.assertTrue(parent_summaries)
            combined_summary_text = " ".join(str(record.get("summary_text", "")) for record in parent_summaries)
            self.assertIn("Child summary says finance approved the GPU budget", combined_summary_text)
            self.assertIn("Alice approved the GPU purchase", combined_summary_text)
            self.assertIn("Older compressed context", combined_summary_text)
            self.assertNotIn("RAW_LEAF_SHOULD_NOT_APPEAR", combined_summary_text)

            for summary in parent_summaries:
                policy = summary["summary_generation_policy"]
                self.assertEqual("child_summaries_plus_state", policy["source_policy"])
                self.assertFalse(policy["raw_recursive_leaf_event_scan"])
                self.assertEqual([], summary["source_event_ids"])
                self.assertIn(child_summary_hash, summary["source_summary_hashes"])
                self.assertIn(entity_hash, summary["source_entity_hashes"])
                self.assertIn(compression_hash, summary["source_operator_hashes"])

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
        mcp.validate_mcp_backend_policy(self._args("temporalstore-rust-direct"))

    def test_debug_override_does_not_restore_jsonl_storage(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "benchmark"
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = True
        with self.assertRaises(mcp.MatrixArkError):
            mcp.validate_mcp_backend_policy(self._args("local"))

    def test_backend_readiness_default_policy(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "benchmark"
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = ""
        self.assertTrue(mcp.backend_ready_required("temporalstore-rust"))
        self.assertTrue(mcp.backend_ready_required("temporalstore-rust-direct"))
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

    def test_native_context_pack_default_policy(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "dev"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        self.assertTrue(mcp.native_context_pack_required("temporalstore-rust"))
        self.assertTrue(mcp.native_context_pack_required("temporalstore-rust-direct"))
        self.assertTrue(mcp.native_context_pack_required("temporalstore-direct"))
        self.assertFalse(mcp.native_context_pack_required("local"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = "0"
        self.assertFalse(mcp.native_context_pack_required("temporalstore-rust"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = "1"
        self.assertTrue(mcp.native_context_pack_required("local"))


    def test_python_hot_cache_default_policy(self) -> None:
        mcp.MATRIXARK_ALLOW_PYTHON_HOT_CACHE = ""
        mcp.MATRIXARK_MCP_PROFILE = "dev"
        self.assertFalse(mcp.python_hot_cache_allowed(backend_label="temporalstore-direct"))
        self.assertFalse(mcp.python_hot_cache_allowed(backend_label="temporalstore-rust"))
        self.assertTrue(mcp.python_hot_cache_allowed(backend_label="local"))
        mcp.MATRIXARK_MCP_PROFILE = "production"
        self.assertFalse(mcp.python_hot_cache_allowed(backend_label="temporalstore-direct"))
        self.assertTrue(mcp.python_hot_cache_allowed(backend_label="local"))
        mcp.MATRIXARK_ALLOW_PYTHON_HOT_CACHE = "1"
        self.assertTrue(mcp.python_hot_cache_allowed(backend_label="temporalstore-direct"))

    def test_native_candidate_prefilter_default_policy(self) -> None:
        mcp.MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = ""
        mcp.MATRIXARK_MCP_PROFILE = "dev"
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust"))
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust-direct"))
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-direct"))
        self.assertFalse(mcp.native_candidate_prefilter_required_for_backend("local"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = "0"
        self.assertFalse(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust"))
        mcp.MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER = "1"
        self.assertTrue(mcp.native_candidate_prefilter_required_for_backend("temporalstore-rust"))


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

        self.assertEqual(len(client.calls), 2)
        raw_call = client.calls[0]
        self.assertEqual(raw_call["count_key"], "matrixark:test:native-append:raw_ingestion:record_count")
        self.assertEqual(raw_call["count_value"], "2")
        self.assertEqual(raw_call["append_options"]["append_path"], "matrixark_raw_ingestion_temporalstore_log")
        self.assertEqual(raw_call["append_options"]["raw_storage_backend"], "temporalstore")
        self.assertEqual(raw_call["append_options"]["source"], "matrixark_live_ingestion_dual_write")
        self.assertEqual({entry["key"] for entry in raw_call["entries"]}, {"matrixark:test:native-append:raw_ingestion:records:000000"})
        raw_payloads = [json.loads(entry["value"]) for entry in raw_call["entries"]]
        self.assertEqual([payload["text"] for payload in raw_payloads], ["native batch append works", "same millisecond collision slot"])
        self.assertTrue(all("placement_key" not in payload for payload in raw_payloads))

        call = client.calls[1]
        self.assertEqual(call["count_key"], "matrixark:test:native-append:record_count")
        self.assertEqual(call["count_value"], "1")
        self.assertEqual(call["append_options"]["append_path"], "native_append_queue")
        self.assertTrue(call["append_options"]["coalesce_writes"])
        self.assertEqual(call["append_options"]["route_by"], "placement_key")
        self.assertTrue(call["append_options"]["persist_from_storage_options"])
        self.assertEqual(call["append_options"]["hset_lowering"], "forbidden_for_parity")
        self.assertEqual(call["append_options"]["audit_hot_path"], "inline_counters_only")
        self.assertEqual(call["append_options"]["full_context_pack_audit"], "sample_or_enqueue_async_policy_enabled")
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

    def test_direct_append_dual_writes_raw_and_serving_records(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:dual-write"
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

        raw_record = {
            "record_type": "context_event",
            "event_id_hash": 991,
            "tenant_hash": 7,
            "scope_key": "tenant=7",
            "node_hash": 9,
            "updated_at_ms": 1780000001000,
            "text": "raw must stay canonical",
            "internal_extraction": {"classification": "memory", "status": "observed"},
        }
        adapter.append(raw_record)

        self.assertEqual(len(client.calls), 2)
        raw_call, serving_call = client.calls
        self.assertEqual(raw_call["count_key"], "matrixark:test:dual-write:raw_ingestion:record_count")
        self.assertEqual(raw_call["count_value"], "1")
        self.assertEqual(raw_call["append_options"]["append_path"], "matrixark_raw_ingestion_temporalstore_log")
        self.assertEqual(raw_call["append_options"]["raw_storage_backend"], "temporalstore")
        self.assertEqual(raw_call["entries"][0]["key"], "matrixark:test:dual-write:raw_ingestion:records:000000")
        raw_payload = json.loads(raw_call["entries"][0]["value"])
        self.assertEqual(raw_payload["text"], "raw must stay canonical")
        self.assertIn("internal_extraction", raw_payload)
        self.assertNotIn("placement_key", raw_payload)

        self.assertEqual(serving_call["count_key"], "matrixark:test:dual-write:record_count")
        self.assertEqual(serving_call["count_value"], "1")
        serving_payloads = [json.loads(entry["value"]) for entry in serving_call["entries"] if entry["key"].endswith(":records:000000")]
        self.assertEqual(len(serving_payloads), 1)
        self.assertEqual(serving_payloads[0]["record_type"], "context_event")
        self.assertEqual(serving_payloads[0]["placement_key"], "context:tenant=7:node=9")

    def test_direct_append_supports_matrixkv_raw_backend_option(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:matrixkv-raw"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._raw_ingestion_prefix = "matrixkv:raw:messages"
        adapter._raw_record_hash_key = f"{adapter._raw_ingestion_prefix}:records"
        adapter._raw_count_key = f"{adapter._raw_ingestion_prefix}:record_count"
        adapter._raw_storage_backend = "matrixkv"
        adapter._raw_entry_count_cache = None
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

        adapter.append(
            {
                "record_type": "context_event",
                "event_id_hash": 992,
                "tenant_hash": 8,
                "scope_key": "tenant=8",
                "node_hash": 10,
                "updated_at_ms": 1780000002000,
                "text": "matrixkv raw option",
            }
        )

        self.assertEqual(len(client.calls), 2)
        raw_call = client.calls[0]
        self.assertEqual(raw_call["count_key"], "matrixkv:raw:messages:record_count")
        self.assertEqual(raw_call["append_options"]["append_path"], "matrixark_raw_ingestion_matrixkv_log")
        self.assertEqual(raw_call["append_options"]["raw_storage_backend"], "matrixkv")
        self.assertEqual(raw_call["entries"][0]["key"], "matrixkv:raw:messages:records:000000")

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
        self.assertGreaterEqual(stats["native_index_postings_found"], 1)
        self.assertTrue(stats["native_index_posting_buckets"])
        self.assertFalse(stats["broad_scan_used"])
        self.assertGreaterEqual(stats["native_locations"], 1)
        self.assertEqual({record["record_type"] for record in result["records"]}, {"context_event", "context_embedding", "context_index"})
        lookup_values = [
            value
            for (key, field), value in client.store.items()
            if ":context_index_lookup:" in key and field == "source_type:message"
        ]
        self.assertTrue(lookup_values)
        lookup_payload = json.loads(lookup_values[0])
        self.assertEqual(lookup_payload["ref_hashes"], [101])
        self.assertTrue(lookup_payload["posting_buckets"])
        broad_record_loads = [
            entries
            for entries in client.batch_hget_entries
            if entries and all(str(entry["key"]).startswith(f"{adapter._record_hash_key}:") for entry in entries)
        ]
        self.assertTrue(broad_record_loads)
        self.assertTrue(all(len(entries) < adapter._shard_size for entries in broad_record_loads))

    def test_direct_retrieval_blocks_broad_scan_without_explicit_fallback(self) -> None:
        client = _NativeIndexClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:phase2-no-broad"
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

        result = adapter.retrieval_records(
            scope={"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
            secondary_index_groups=[{"source_type:message"}],
            selected_node_hashes={44},
        )

        self.assertEqual(result["records"], [])
        stats = result["scan_stats"]
        self.assertEqual(stats["execution_mode"], "native_prefilter_no_match_broad_scan_blocked")
        self.assertFalse(stats["native_pushdown"])
        self.assertFalse(stats["broad_scan_fallback_allowed"])
        self.assertFalse(stats["broad_scan_used"])
        self.assertTrue(stats["broad_scan_blocked"])
        self.assertEqual(stats["scanned"], 0)

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
                "resource_version": "rv1",
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
        self.assertEqual(
            direct_result["scan_stats"]["native_candidate_cache_key_shape"],
            "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
        )
        self.assertEqual(direct_result["scan_stats"]["native_candidate_cache_payload"], "compact_struct")
        self.assertEqual(direct_result["scan_stats"]["native_resource_version_watermark"], "rv1")
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
                    "resource_version": "rv2",
                    "updated_at_ms": 1780000000003,
                    "text": "New append changes the watermark.",
                }
            ]
        )
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
        direct_after_append_result = direct.retrieval_records(**kwargs)
        self.assertFalse(direct_after_append_result["scan_stats"]["native_placement_candidate_cache_hit"])
        self.assertEqual(direct_after_append_result["scan_stats"]["native_resource_version_watermark"], "rv1|rv2")
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
        self.assertEqual(request["append_watermark"], 7)
        self.assertEqual(request["index_posting_watermark"], 7)
        self.assertEqual(request["resource_version_watermark"], "")
        self.assertEqual(request["skill_status_watermark"], "")
        self.assertEqual(request["scope_key"], "t=11|u=22|s=33|")
        self.assertEqual(request["placement_node_hash"], request["start_node_hash"])
        self.assertEqual(request["placement_key"], f"context:{request['scope_key']}:node={request['start_node_hash']}")
        self.assertFalse(request["include_superseded"])
        self.assertFalse(request["include_superseded_resources"])
        self.assertEqual(request["shared_resource_max_refs"], 4)
        self.assertEqual(request["skill_max_refs"], 4)
        self.assertEqual(request["cross_session_max_refs"], 4)
        self.assertTrue(request["cross_session_rerank"])
        self.assertTrue(request["same_session_priority"])
        self.assertEqual(
            request["required_native_apis"],
            [
                "health",
                "readiness",
                "metrics",
                "matrixark_batch_append_records",
                "matrixark_retrieve_context_pack",
                "compact_secondary_index_lookup",
                "placement_key_candidate_fetch",
            ],
        )
        self.assertEqual(
            request["normal_path_stages"],
            [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
        )
        self.assertEqual(request["normalization_requirements"]["scope_key"], "canonical")
        self.assertEqual(request["normalization_requirements"]["placement_key"], "context:{scope_key}:node={node_hash}")
        self.assertEqual(request["execution_plan_requirements"]["phase"], "phase4_native_score_rerank_pack")
        self.assertEqual(request["execution_plan_requirements"]["candidate_fetch"], "selected_node_placement_partitions_only")
        self.assertEqual(
            request["execution_plan_requirements"]["candidate_cache"],
            "scope_key+node_hash+record_type+append_watermark+resource_version_watermark",
        )
        self.assertEqual(request["execution_plan_requirements"]["candidate_cache_payload"], "compact_structs_not_json_strings")
        self.assertEqual(
            request["execution_plan_requirements"]["scoring"],
            "native_embedding_similarity_temporal_decay_business_boost_same_session_boost",
        )
        self.assertEqual(
            request["execution_plan_requirements"]["quotas"],
            "native_shared_resource_quota_cross_session_quota_current_session_priority",
        )
        self.assertEqual(request["execution_plan_requirements"]["rerank"], "native_score_fusion_then_budget_aware_rerank")
        self.assertEqual(request["execution_plan_requirements"]["token_budget_pack"], "native_budget_pack_with_selected_refs_and_dropped_summary")
        self.assertEqual(request["execution_plan_requirements"]["python_role"], "dispatcher_only_no_candidate_materialization_no_hot_path_pack")
        self.assertEqual(request["execution_plan_requirements"]["pack_assembly"], "native_score_rank_budget_pack_selected_refs_dropped_summary")
        self.assertEqual(request["execution_plan_requirements"]["write_path"], "native_batch_append_records_append_queue_coalesced_persistence")
        self.assertEqual(request["execution_plan_requirements"]["write_route"], "placement_key_partition_route_before_persistence")
        self.assertEqual(request["execution_plan_requirements"]["write_coalescing"], "native_append_queue_coalesces_by_record_key_field")
        self.assertEqual(request["execution_plan_requirements"]["durability"], "storage_options_select_async_sync_shared_store_or_raft")
        self.assertEqual(request["execution_plan_requirements"]["retrieval_hot_path_audit"], "inline_counters_only_no_full_audit_blocking")
        self.assertEqual(request["execution_plan_requirements"]["context_pack_audit"], "sample_or_enqueue_async_policy_enabled")
        self.assertEqual(request["execution_plan_requirements"]["full_replay_audit_default"], "disabled")
        self.assertEqual(request["execution_plan_requirements"]["secondary_index"], "compact_postings_by_scope_index_time_bucket")
        self.assertEqual(request["execution_plan_requirements"]["broad_prefix_scan"], "disabled_unless_explicit_debug_fallback")
        self.assertEqual(request["execution_plan_requirements"]["health_readiness_metrics"], "native_backend_must_expose_health_readiness_metrics")
        self.assertEqual(
            request["execution_plan_requirements"]["normal_path"],
            "query_understanding_scope_filter_l0_l1_traversal_compact_index_placement_fetch_native_score_rerank_pack",
        )
        self.assertIn("scope", request["required_output"]["drop_counters"])
        self.assertIn("score_threshold", request["required_output"]["drop_counters"])
        self.assertTrue(request["required_output"]["broad_scan_used"])
        self.assertTrue(request["required_output"]["normal_path_stages"])
        self.assertTrue(request["required_output"]["health_readiness_metrics"])
        self.assertTrue(request["required_output"]["candidate_cache_key_shape"])
        self.assertTrue(request["required_output"]["native_pack_assembly"])
        self.assertFalse(request["required_output"]["raw_candidate_tables"])
        self.assertFalse(request["required_output"]["python_pack_fallback"])
        self.assertTrue(request["required_output"]["selected_refs"])
        self.assertTrue(request["required_output"]["dropped_summary"])
        self.assertTrue(request["required_output"]["retrieval_metrics"])
        self.assertEqual(result["retrieval_metrics"]["query_plan_ms"], 1.5)
        self.assertEqual(result["retrieval_metrics"]["placement_partitions_touched"], 2)
        self.assertEqual(result["retrieval_metrics"]["placement_fetch_count"], 8)
        self.assertEqual(result["retrieval_metrics"]["index_postings_read"], 9)
        self.assertTrue(result["retrieval_metrics"]["compact_index_bucket_used"])
        self.assertEqual(result["retrieval_metrics"]["compact_index_bucket_count"], 3)
        self.assertTrue(result["retrieval_metrics"]["candidate_cache_hit"])
        self.assertEqual(result["retrieval_metrics"]["append_queue_wait_ms"], 0.25)
        self.assertEqual(result["retrieval_metrics"]["append_engine_ms"], 0.75)
        self.assertEqual(result["retrieval_metrics"]["timeout_count"], 0)
        self.assertEqual(result["retrieval_metrics"]["fallback_flags"], ["native_context_pack"])
        self.assertEqual(result["retrieval_metrics"]["dropped_refs"], 21)
        self.assertTrue(result["retrieval_metrics"]["native_pack_assembly"])
        self.assertFalse(result["retrieval_metrics"]["python_pack_fallback"])
        self.assertFalse(result["retrieval_metrics"]["broad_scan_used"])
        self.assertEqual(result["retrieval_metrics"]["broad_scan_policy"], "explicit_fallback_or_debug_only")
        self.assertEqual(result["retrieval_metrics"]["normal_path_stages"], request["normal_path_stages"])
        self.assertEqual(result["retrieval_metrics"]["health_readiness_metrics"], {"health": True, "readiness": True, "metrics": True})
        self.assertFalse(result["retrieval_metrics"]["raw_candidate_tables_returned"])
        self.assertEqual(result["retrieval_metrics"]["drop_counters"]["scope"], 1)
        self.assertEqual(result["retrieval_metrics"]["drop_counters"]["score_threshold"], 6)
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["scope_filtering"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["placement_filtering"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["compact_secondary_index_prefilter"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["stale_superseded_exclusion"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["shared_resource_skill_quota"])
        self.assertTrue(result["retrieval_metrics"]["correctness_evidence"]["cross_session_quota_rerank"])
        self.assertEqual(
            result["recall_policy"]["backend_retrieval_pushdown"]["execution_mode"],
            "native_context_pack",
        )

    def test_direct_retrieve_derives_native_hash_scope_from_plain_ids(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = None
        scope = {
            "account_id": "acct_scale",
            "tenant_id": "tenant_scale",
            "user_id": "user_scale",
            "session_id": "session_scale",
        }

        adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": scope,
                "max_context_tokens": 2048,
                "ranking": {"max_selected_refs": 8},
            }
        )

        request = client.requests[0]
        expected_hashes = mcp_core.identity_hashes("acct_scale", "tenant_scale", "user_scale", "session_scale")
        self.assertEqual(request["tenant_hash"], expected_hashes["tenant_hash"])
        self.assertEqual(request["scope"]["tenant_hash"], expected_hashes["tenant_hash"])
        self.assertEqual(request["scope"]["user_hash"], expected_hashes["user_hash"])
        self.assertEqual(request["scope"]["session_hash"], expected_hashes["session_hash"])
        self.assertEqual(request["scope_key"], expected_hashes["scope_key"])
        self.assertEqual(request["scope_hash"], mcp_core.stable_hash(expected_hashes["scope_key"]))
        self.assertEqual(
            request["placement_key"],
            f"context:{expected_hashes['scope_key']}:node={request['start_node_hash']}",
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

    def test_native_retrieve_failure_blocks_python_broad_scan_by_default(self) -> None:
        client = _FailingNativeContextPackClient("raise")
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

        self.assertEqual(result["context_pack_assembly"], "native_context_pack_blocked")
        self.assertEqual(result["retrieval_metrics"]["native_api"], "matrixark_retrieve_context_pack")
        self.assertFalse(result["retrieval_metrics"]["python_pack_fallback"])
        self.assertFalse(result["retrieval_metrics"]["broad_scan_used"])
        self.assertTrue(result["retrieval_metrics"]["broad_scan_blocked"])
        self.assertEqual(result["retrieval_metrics"]["scanned_records"], 0)
        self.assertEqual(
            result["retrieval_metrics"]["normal_path_stages"],
            [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack",
            ],
        )
        self.assertEqual(result["retrieval_metrics"]["health_readiness_metrics"], {"health": True, "readiness": True, "metrics": True})
        self.assertEqual(client.read_all_calls, 0)
        self.assertIn("quality_warnings", result)

    def test_native_retrieve_raw_candidate_tables_are_rejected_without_python_pack(self) -> None:
        client = _FailingNativeContextPackClient("raw_tables")
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
            }
        )

        self.assertEqual(result["context_pack_assembly"], "native_context_pack_blocked")
        self.assertTrue(result["retrieval_metrics"]["raw_candidate_tables_returned"])
        self.assertFalse(result["retrieval_metrics"]["python_pack_fallback"])
        self.assertFalse(result["retrieval_metrics"]["broad_scan_used"])
        self.assertEqual(client.read_all_calls, 0)

    def test_native_retrieve_explicit_debug_fallback_can_leave_native_pack_path(self) -> None:
        client = _FailingNativeContextPackClient("raise")
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:native-pack"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._entry_count_cache = 0

        result = adapter.retrieve(
            {
                "query": "Who approved the GPU budget?",
                "scope": {"tenant_hash": 11, "user_hash": 22, "session_hash": 33},
                "allow_python_pack_fallback": True,
            }
        )

        self.assertNotEqual(result.get("context_pack_assembly"), "native_context_pack_blocked")

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

    def test_direct_append_time_index_uses_segment_parent_when_present(self) -> None:
        client = _NativeAppendClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._storage_prefix = "matrixark:test:segment-time"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._index_key = f"{adapter._storage_prefix}:record_index"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 1024
        adapter._index_cache = None
        adapter._records_cache = None
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
                    "event_id_hash": 456,
                    "node_hash": 999,
                    "parent_segment_hash": 777,
                    "updated_at_ms": 1780000000001,
                    "text": "segmented event",
                }
            ]
        )

        time_entry = next(entry for entry in client.calls[0]["entries"] if "context_event_by_ingestion_time" in entry["key"])
        self.assertIn("context_event_by_ingestion_time:context_segment:777", time_entry["key"])
        self.assertEqual(time_entry["field"], f"{1780000000001:020d}:456")
        time_payload = json.loads(time_entry["value"])
        self.assertEqual(time_payload["record_type"], "context_event")
        self.assertEqual(time_payload["event_id_hash"], 456)
        self.assertEqual(time_payload["text"], "segmented event")
        self.assertNotIn("event_time_key", time_payload)
        self.assertNotIn("ingestion_time_ms", time_payload)


    def test_direct_retrieval_records_prefilters_scope_type_and_secondary_indexes(self) -> None:
        policy_globals = mcp.MatrixArkTemporalStoreDirectAdapter._native_candidate_scan.__globals__["native_candidate_prefilter_required"].__globals__
        old_core_prefilter = policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"]
        policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"] = "0"
        self.addCleanup(lambda: policy_globals.__setitem__("MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER", old_core_prefilter))
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._records_cache = []
        adapter._backend_label = lambda: "temporalstore-direct"
        scope = {"account_id": "acct", "tenant_id": "tenant", "user_id": "user"}
        adapter.read_all = lambda: [
            {
                "record_type": "resource_chunk",
                "scope": scope,
                "node_hash": 101,
                "chunk_hash": 1001,
                "resource_type": "pdf",
                "text": "GPU approval policy",
            },
            {
                "record_type": "resource_chunk",
                "scope": scope,
                "node_hash": 102,
                "chunk_hash": 1002,
                "resource_type": "md",
                "text": "Markdown notes",
            },
            {
                "record_type": "context_event",
                "scope": scope,
                "node_hash": 103,
                "text": "Alice approved the request",
            },
            {
                "record_type": "context_pack_audit",
                "scope": scope,
                "context_pack_id": "audit-only",
            },
            {
                "record_type": "resource_chunk",
                "scope": {"account_id": "other", "tenant_id": "tenant", "user_id": "user"},
                "node_hash": 104,
                "chunk_hash": 1004,
                "resource_type": "pdf",
                "text": "wrong account",
            },
        ]

        result = adapter.retrieval_records(
            scope=scope,
            secondary_index_groups=[{"resource_type:pdf"}],
        )

        texts = {record.get("text") for record in result["records"]}
        self.assertIn("GPU approval policy", texts)
        self.assertIn("Alice approved the request", texts)
        self.assertNotIn("Markdown notes", texts)
        self.assertNotIn("wrong account", texts)
        stats = result["scan_stats"]
        self.assertTrue(stats["backend_pushdown"])
        self.assertTrue(stats["direct_backend_prefilter"])
        self.assertFalse(stats["native_pushdown"])
        self.assertEqual(stats["execution_mode"], "direct_backend_hot_cache_prefilter")
        self.assertGreaterEqual(stats["secondary_index_dropped_candidate_count"], 1)
        self.assertEqual(stats["dropped_by_type"], 1)
        self.assertEqual(stats["dropped_by_scope"], 1)

    def test_direct_retrieval_prefers_native_candidate_scan_when_available(self) -> None:
        client = _NativeCandidateScanClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-rust"
        scope = {"account_id": "acct", "tenant_id": "tenant", "user_id": "user"}

        result = adapter.retrieval_records(
            scope=scope,
            record_types={"resource_chunk"},
            secondary_index_groups=[{"resource_type:pdf"}],
            selected_node_hashes={123},
        )

        self.assertEqual(result["records"][0]["text"], "native filtered pdf")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0]["count_key"], "matrixark:test:record_count")
        self.assertEqual(client.calls[0]["record_hash_key"], "matrixark:test:records")
        self.assertEqual(client.calls[0]["secondary_index_groups"], [["resource_type:pdf"]])
        self.assertEqual(client.calls[0]["selected_node_hashes"], [123])
        stats = result["scan_stats"]
        self.assertTrue(stats["native_prefix_scan"])
        self.assertTrue(stats["native_secondary_index_prefilter"])
        self.assertEqual(stats["pack_assembly_location"], "python_reference_packer")

    def test_production_native_candidate_scan_failure_does_not_fallback_to_read_all(self) -> None:
        policy_globals = mcp.MatrixArkTemporalStoreDirectAdapter._native_candidate_scan.__globals__["native_candidate_prefilter_required"].__globals__
        old_core_profile = policy_globals["MATRIXARK_MCP_PROFILE"]
        old_core_prefilter = policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"]
        policy_globals["MATRIXARK_MCP_PROFILE"] = "production"
        policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"] = ""
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _FailingNativeCandidateScanClient()
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter.read_all = lambda: self.fail("read_all fallback must not run when native prefilter is required")

        try:
            with self.assertRaisesRegex(mcp.MatrixArkError, "backend-native candidate prefilter failed"):
                adapter.retrieval_records(
                    scope={"account_id": "acct"},
                    record_types={"resource_chunk"},
                    secondary_index_groups=[{"resource_type:pdf"}],
                )
        finally:
            policy_globals["MATRIXARK_MCP_PROFILE"] = old_core_profile
            policy_globals["MATRIXARK_REQUIRE_NATIVE_CANDIDATE_PREFILTER"] = old_core_prefilter

    def test_retrieve_prefers_same_session_but_allows_entity_bridge_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmpdir) / "matrixark.jsonl")
            same_scope = {
                "account_id": "acct",
                "tenant_id": "tenant",
                "user_id": "user",
                "session_id": "session_current",
                "tenant_hash": mcp.stable_hash("acct:tenant"),
                "user_hash": mcp.stable_hash(f"{mcp.stable_hash('acct:tenant')}:user:user"),
                "session_hash": mcp.stable_hash(f"{mcp.stable_hash('acct:tenant')}:session:session_current"),
                "_explicit_scope_keys": ["account_id", "tenant_id", "user_id", "session_id"],
            }
            same_scope["scope_key"] = mcp.scope_key_from_hashes(same_scope["tenant_hash"], same_scope["user_hash"], same_scope["session_hash"])
            prior_session_hash = mcp.stable_hash(f"{same_scope['tenant_hash']}:session:session_prior")
            prior_scope = {**same_scope, "session_id": "session_prior", "session_hash": prior_session_hash}
            prior_scope["scope_key"] = mcp.scope_key_from_hashes(prior_scope["tenant_hash"], prior_scope["user_hash"], prior_session_hash)
            adapter.append_many([
                {
                    "record_type": "context_event",
                    "event_id_hash": 101,
                    "node_hash": 11,
                    "node_path": ["memory", "approvals"],
                    "scope": same_scope,
                    "scope_key": same_scope["scope_key"],
                    "classification": "confirmation",
                    "event_type": "confirmation",
                    "text": "The current session is about the GPU request and approval follow-up.",
                    "updated_at_ms": 1000,
                },
                {
                    "record_type": "context_entity",
                    "entity_hash": 202,
                    "node_hash": 12,
                    "node_path": ["memory", "approvals"],
                    "scope": prior_scope,
                    "scope_key": prior_scope["scope_key"],
                    "entity_type": "approval_state",
                    "entity_name": "gpu_request",
                    "state": "Alice approved the GPU request after finance review.",
                    "updated_at_ms": 1100,
                },
                {
                    "record_type": "context_index",
                    "index_name": "event_type:confirmation",
                    "ref_hash": 101,
                    "node_hash": 11,
                    "scope": same_scope,
                    "scope_key": same_scope["scope_key"],
                },
                {
                    "record_type": "context_index",
                    "index_name": "entity_type:approval_state",
                    "ref_hash": 202,
                    "node_hash": 12,
                    "scope": prior_scope,
                    "scope_key": prior_scope["scope_key"],
                },
            ])

            pack = adapter.retrieve({
                "query": "Who approved the GPU request?",
                "scope": same_scope,
                "session_scope": "prefer",
                "max_context_tokens": 1000,
                "audit_mode": "off",
                "ranking": {"max_selected_refs": 8},
            })

            selected = pack["selected_refs"]
            self.assertTrue(any(ref.get("session_continuity") == "same_session" for ref in selected))
            self.assertTrue(any(ref.get("session_continuity") == "cross_session" and ref.get("ref_type") == "entity" for ref in selected))
            policy = (pack.get("recall_policy") or pack.get("recall", {})).get("session_continuity", {})
            if policy:
                self.assertEqual(policy["mode"], "prefer")
                self.assertGreaterEqual(policy["same_session_selected_ref_count"], 1)
                self.assertGreaterEqual(policy["entity_bridge_selected_ref_count"], 1)

            strict_pack = adapter.retrieve({
                "query": "Who approved the GPU request?",
                "scope": same_scope,
                "session_scope": "only",
                "max_context_tokens": 1000,
                "audit_mode": "off",
                "ranking": {"max_selected_refs": 8},
            })
            self.assertFalse(any(ref.get("session_continuity") == "cross_session" for ref in strict_pack["selected_refs"]))

    def test_service_backpressure_fallback_does_not_read_all_for_native_backend(self) -> None:
        class _NoReadAllNativeAdapter:
            def __init__(self) -> None:
                self.records_seen = None

            def _backend_label(self) -> str:
                return "temporalstore-direct"

            def read_all(self):
                raise AssertionError("native timeout fallback must not read all records")

            def deadline_fallback_pack(self, **kwargs):
                self.records_seen = len(kwargs["records"])
                return {
                    "context_pack_id": "timeout-partial",
                    "quality_warnings": ["timeout_partial"],
                    "records_seen": self.records_seen,
                }

        adapter = _NoReadAllNativeAdapter()
        server = mcp.MatrixArkMcpServer(adapter)
        old_limit = os.environ.get("MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT")
        os.environ["MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT"] = "5"
        try:
            pack = server._retrieve_timeout_fallback(
                {"query": "What did Alice approve?", "scope": {"account_id": "acct"}},
                deadline_ms=10,
                elapsed_ms=12.3,
                reason="service_backpressure",
            )
        finally:
            if old_limit is None:
                os.environ.pop("MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT", None)
            else:
                os.environ["MATRIXARK_BACKPRESSURE_FALLBACK_RECORD_LIMIT"] = old_limit

        self.assertEqual(pack["records_seen"], 0)
        self.assertEqual(adapter.records_seen, 0)

    def test_production_retrieve_dispatches_native_pack_before_query_embedding(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        pack = adapter.retrieve({
            "query": "What did Alice approve?",
            "scope": {"account_id": "acct"},
            "max_context_tokens": 1000,
            "audit_mode": "off",
        })

        self.assertEqual(pack["pack_id"], "native-pack-1")
        self.assertNotIn("context_pack_id", pack)
        self.assertEqual(len(client.calls), 1)
        self.assertNotIn("query_embedding", client.calls[0]["request"])
        self.assertEqual(client.calls[0]["request"]["query"], "What did Alice approve?")
        cross_session = client.calls[0]["request"]["cross_session"]
        self.assertTrue(cross_session["enabled"])
        self.assertEqual(cross_session["budget_ratio"], 0.12)
        self.assertEqual(cross_session["budget_tokens"], 114)
        self.assertEqual(cross_session["max_sessions"], 3)
        self.assertEqual(cross_session["max_candidates"], 24)
        self.assertEqual(cross_session["min_score"], 0.2)
        self.assertEqual(cross_session["decision"], "always_consider_same_user_cross_session_when_session_scope_prefer")
        self.assertEqual(cross_session["strategy"], "same_session_first_entity_bridge_then_bounded_cross_session")

    def test_direct_native_context_pack_accepts_explicit_cross_session_budget(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-direct"
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._observe_model_latency = lambda *args, **kwargs: None
        adapter.native_context_pack_required = lambda: True

        adapter.retrieve({
            "query": "What is my current storage preference?",
            "scope": {"account_id": "acct", "session_id": "s1"},
            "question_type": "current_state",
            "max_context_tokens": 2000,
            "cross_session": {"budget_tokens": 777, "max_sessions": 3, "parallelism": 2},
            "audit_mode": "off",
        })

        cross_session = client.calls[0]["request"]["cross_session"]
        self.assertTrue(cross_session["enabled"])
        self.assertEqual(cross_session["budget_tokens"], 380)
        self.assertEqual(cross_session["max_budget_ratio"], 0.2)
        self.assertEqual(cross_session["raw_evidence_min_score"], 0.45)
        self.assertEqual(cross_session["max_sessions"], 3)
        self.assertEqual(cross_session["parallelism"], 2)


    def test_cross_session_budget_is_cap_and_raw_events_need_high_confidence(self) -> None:
        policy = mcp.build_cross_session_policy(
            {"cross_session": {"budget_tokens": 900, "raw_evidence_min_score": 0.45}},
            {},
            question_type="current_state",
            session_scope="prefer",
            remote_budget_tokens=2000,
        )
        self.assertEqual(policy["budget_tokens"], 400)
        self.assertEqual(policy["max_budget_ratio"], 0.2)

        selected, used_tokens, dropped = mcp.select_token_budgeted_refs(
            [
                {
                    "ref_type": "event",
                    "ref_hash": 1,
                    "session_continuity": "cross_session",
                    "score": 0.40,
                    "scope": {"session_id": "old"},
                    "text": "Old raw event says Alice may approve the GPU request later.",
                },
                {
                    "ref_type": "entity",
                    "ref_hash": 2,
                    "session_continuity": "cross_session",
                    "score": 0.24,
                    "scope": {"session_id": "old"},
                    "text": "Current entity state: Alice approved the GPU request.",
                },
            ],
            [],
            max_context_tokens=1000,
            auxiliary_quota=0,
            question_type="current_state",
            min_score=0.2,
            cross_session_policy={
                **policy,
                "budget_tokens": 400,
                "raw_evidence_min_score": 0.45,
            },
        )

        self.assertEqual([item["ref_type"] for item in selected], ["entity"])
        self.assertGreater(used_tokens, 0)
        self.assertGreaterEqual(dropped["low_score"], 1)
        self.assertEqual(dropped["cross_session_policy"]["selected_ref_count"], 1)

    def test_shared_context_budget_caps_shared_resources_and_skills(self) -> None:
        shared_policy = mcp.build_shared_context_policy(
            {"shared_context": {"resource_budget_tokens": 8, "skill_budget_tokens": 8}},
            {},
            remote_budget_tokens=1000,
        )
        selected, used_tokens, dropped = mcp.select_token_budgeted_refs(
            [
                {
                    "ref_type": "resource_chunk",
                    "ref_hash": 1,
                    "score": 0.9,
                    "sharing_scope": "tenant_shared",
                    "text": "GPU approval policy",
                },
                {
                    "ref_type": "resource_chunk",
                    "ref_hash": 2,
                    "score": 0.88,
                    "sharing_scope": "tenant_shared",
                    "text": "finance review requires budget owner signoff",
                },
                {
                    "ref_type": "skill_section",
                    "ref_hash": 3,
                    "score": 0.87,
                    "sharing_scope": "tenant_shared",
                    "text": "Use replay debugger for selected refs",
                },
            ],
            [],
            max_context_tokens=1000,
            auxiliary_quota=0,
            question_type="fact",
            min_score=0.2,
            shared_context_policy=shared_policy,
        )

        self.assertGreater(used_tokens, 0)
        self.assertEqual(sum(1 for item in selected if item["ref_type"] == "resource_chunk"), 1)
        self.assertEqual(sum(1 for item in selected if item["ref_type"] == "skill_section"), 1)
        self.assertGreaterEqual(dropped["shared_resource_budget"], 1)
        self.assertEqual(dropped["shared_context_policy"]["resource_selected_ref_count"], 1)
        self.assertEqual(dropped["shared_context_policy"]["skill_selected_ref_count"], 1)

    def test_direct_native_context_pack_dispatches_single_backend_request(self) -> None:
        client = _NativeContextPackClient()
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = client
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        adapter._backend_label = lambda: "temporalstore-rust"

        pack = adapter.native_context_pack({
            "query": "gpu approval",
            "query_embedding": [0.1, 0.2],
            "scope": {"account_id": "acct"},
            "secondary_index_groups": [["resource_type:pdf"]],
            "max_context_tokens": 1000,
        })

        self.assertIsNotNone(pack)
        assert pack is not None
        self.assertEqual(pack["context_pack_id"], "native-pack-1")
        self.assertEqual(pack["context_pack_assembly"], "native_backend")
        self.assertEqual(pack["backend"], "temporalstore-rust")
        contract = pack["recall_policy"]["native_response_contract"]
        self.assertFalse(contract["raw_records_returned_to_python"])
        self.assertEqual(contract["python_hot_path_records"], 0)
        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["count_key"], "matrixark:test:record_count")
        self.assertEqual(call["record_hash_key"], "matrixark:test:records")
        self.assertEqual(call["request"]["query"], "gpu approval")
        self.assertEqual(call["request"]["secondary_index_groups"], [["resource_type:pdf"]])

    def test_native_context_pack_rejects_raw_record_payloads(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _BadNativeContextPackClient()
        adapter._count_key = "matrixark:test:record_count"
        adapter._record_hash_key = "matrixark:test:records"
        adapter._shard_size = 128
        with self.assertRaisesRegex(mcp.MatrixArkError, "must return a finished ContextPack"):
            adapter.native_context_pack({"query": "gpu", "scope": {}, "max_context_tokens": 1000})

    def test_direct_supports_native_context_pack_only_when_client_exposes_api(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _NativeContextPackClient()
        self.assertTrue(adapter.supports_native_context_pack())
        adapter._client = _NativeCandidateScanClient()
        self.assertFalse(adapter.supports_native_context_pack())

    def test_direct_supports_native_candidate_prefilter_when_client_exposes_api(self) -> None:
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _NativeCandidateScanClient()
        self.assertTrue(adapter.supports_native_candidate_prefilter())
        adapter._client = _NativeContextPackClient()
        self.assertFalse(adapter.supports_native_candidate_prefilter())

    def test_production_retrieve_fails_closed_without_native_context_pack(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = "production"
        mcp.MATRIXARK_REQUIRE_NATIVE_CONTEXT_PACK = ""
        adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
        adapter._client = _NativeCandidateScanClient()
        adapter._storage_prefix = "matrixark:test:pack-required"
        adapter._record_hash_key = f"{adapter._storage_prefix}:records"
        adapter._count_key = f"{adapter._storage_prefix}:record_count"
        adapter._shard_size = 256
        adapter._context_pack_cache_max_entries = 0
        adapter._context_pack_cache_ttl_s = 0
        adapter._context_pack_cache_lock = threading.RLock()
        adapter._context_pack_cache = {}
        adapter._retrieval_records_cache_generation = 0
        adapter._retrieval_records_cache_lock = threading.RLock()
        adapter._retrieval_records_cache = {}
        adapter.native_context_pack_required = lambda: True
        with self.assertRaisesRegex(mcp.MatrixArkError, "backend-native ContextPack assembly"):
            adapter.retrieve({
                "query": "What did Alice approve?",
                "scope": {"account_id": "acct"},
                "max_context_tokens": 1000,
            })

    def test_native_cpp_matrixark_append_does_not_lower_to_hset_loop(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "src/client/temporalstore_c_client.cc").read_text()
        start = source.index("int temporalstore_matrixark_batch_append_records")
        end = source.index("int temporalstore_smembers", start)
        body = source[start:end]
        implementation = (repo / "src/client/temporalstore_client.cc").read_text()
        impl_start = implementation.index("Status TemporalStoreClient::MatrixArkBatchAppendRecords")
        impl_end = implementation.index("Status TemporalStoreClient::SAdd", impl_start)
        impl_body = implementation[impl_start:impl_end]

        self.assertIn("MatrixArkBatchAppendRecords", body)
        self.assertIn("ExecuteRawBatch", implementation)
        self.assertIn("ExecuteRawBatch", impl_body)
        self.assertNotIn("->HSet(", body)
        self.assertNotIn("->PutString(", body)
        self.assertNotIn("->HSet(", impl_body)
        self.assertNotIn("->PutString(", impl_body)
        self.assertNotIn("PutString(count_key", impl_body)

    def test_rust_matrixark_append_uses_native_proxy_path(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "sdk/rust/temporalstore/src/bin/matrixark_rust_proxy.rs").read_text()
        implementation = (repo / "sdk/rust/temporalstore/src/bin/matrixark_record_log.rs").read_text()
        crate_implementation = (repo / "crates/temporalstore-rust/src/bin/matrixark_record_log.rs").read_text()

        self.assertIn("Production-facing alias for the MatrixArk Rust proxy", source)
        self.assertIn('"batch_hset" =>', implementation)
        self.assertIn('"matrixark_append_records" | "matrixark_batch_append_records" =>', implementation)
        self.assertNotIn('"batch_hset" | "matrixark_append_records"', implementation)
        self.assertIn("client\n                .matrixark_batch_append_records", implementation)
        self.assertIn('"native_append": true', implementation)
        self.assertIn("entries_compact", implementation)
        self.assertIn("expanded_hash_entries", implementation)
        self.assertNotIn("put_string(&serving_count_key", implementation)
        self.assertNotIn("value_contains_serving_context_record", implementation)

        self.assertIn('"scan_hash" =>', implementation)
        self.assertIn("client.hgetall", implementation)

        execute_start = crate_implementation.index("fn execute_record_log_request")
        crate_start = crate_implementation.index(
            '"matrixark_append_records" | "matrixark_batch_append_records" =>',
            execute_start,
        )
        crate_end = crate_implementation.index('"batch_hget" =>', crate_start)
        crate_append_body = crate_implementation[crate_start:crate_end]
        self.assertIn("execute_empty_batch_runtime", crate_append_body)
        self.assertIn("BatchExecuteRequest", crate_implementation)
        self.assertIn("rust_proxy_matrixark_batch_runtime_default", crate_append_body)
        self.assertIn("matrixark_batch_uses_forced_sync_durable_writes", crate_append_body)
        self.assertNotIn("execute_empty(&engine", crate_append_body)

    def test_rust_direct_sdk_bridge_has_production_binary(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cargo = (repo / "sdk/rust/temporalstore/Cargo.toml").read_text()
        direct_source = (repo / "sdk/rust/temporalstore/src/bin/matrixark_rust_direct_sdk.rs").read_text()
        implementation = (repo / "sdk/rust/temporalstore/src/bin/matrixark_record_log.rs").read_text()
        server = (repo / "tools/matrixark_mcp_server.py").read_text()

        self.assertIn('name = "matrixark_rust_direct_sdk"', cargo)
        self.assertIn("Production-facing alias for the MatrixArk Rust direct SDK bridge", direct_source)
        self.assertIn("matrixark_rust_sdk_mode_is_direct", implementation)
        self.assertIn("matrixark_rust_direct_sdk", implementation)
        self.assertIn("rust-direct-sdk-bridge", implementation)
        self.assertIn("--rust-direct-sdk", server)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK", server)

    def test_rust_cdylib_direct_binding_exposes_native_matrixark_api(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cargo = (repo / "sdk/rust/temporalstore/Cargo.toml").read_text()
        rust_lib = (repo / "sdk/rust/temporalstore/src/lib.rs").read_text()
        adapter = (repo / "tools/matrixark_mcp_temporal_adapters.py").read_text()
        server = (repo / "tools/matrixark_mcp_server.py").read_text()

        self.assertIn('crate-type = ["rlib", "cdylib"]', cargo)
        for symbol in [
            "temporalstore_rust_connect_json",
            "temporalstore_rust_close",
            "temporalstore_rust_hset",
            "temporalstore_rust_hget",
            "temporalstore_rust_hgetall_json",
            "temporalstore_rust_matrixark_batch_append_records_json",
            "temporalstore_rust_matrixark_scan_candidates_json",
            "temporalstore_rust_matrixark_retrieve_context_pack_json",
        ]:
            self.assertIn(symbol, rust_lib)
        self.assertIn("class MatrixArkRustCdylibClient", adapter)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", adapter)
        self.assertIn("rust_direct_cdylib_matrixark_batch_append_records", adapter)
        self.assertIn("rust_direct_cdylib_matrixark_retrieve_context_pack", adapter)
        self.assertIn("--rust-direct-lib", server)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB", server)

    def test_cpp_python_sdk_exposes_native_hash_scan(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "sdk/python/temporalstore/client.py").read_text()

        self.assertIn("has_hgetall", source)
        self.assertIn("def hgetall", source)
        self.assertIn("def scan_hash", source)

    def test_cpp_sdk_exposes_native_context_pack_boundary(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        header = (repo / "src/client/temporalstore_c_client.h").read_text()
        source = (repo / "src/client/temporalstore_c_client.cc").read_text()
        python_sdk = (repo / "sdk/python/temporalstore/client.py").read_text()

        self.assertIn("temporalstore_matrixark_retrieve_context_pack", header)
        self.assertIn("MatrixArkRetrieveContextPackNative", source)
        self.assertIn("raw_records_returned", source)
        self.assertIn("native_response_contract", source)
        self.assertIn("def matrixark_retrieve_context_pack", python_sdk)
        self.assertIn("has_matrixark_retrieve_context_pack", python_sdk)

    def test_cpp_sdk_exposes_native_candidate_prefilter_boundary(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        header = (repo / "src/client/temporalstore_c_client.h").read_text()
        source = (repo / "src/client/temporalstore_c_client.cc").read_text()
        python_sdk = (repo / "sdk/python/temporalstore/client.py").read_text()

        self.assertIn("temporalstore_matrixark_scan_candidates", header)
        self.assertIn("MatrixArkScanCandidatesNative", source)
        self.assertIn("native_candidate_prefilter", source)
        self.assertIn("def matrixark_scan_candidates", python_sdk)
        self.assertIn("has_matrixark_scan_candidates", python_sdk)

    def test_cpp_native_session_scope_defaults_to_cross_session_prefer(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "src/client/temporalstore_c_client.cc").read_text()

        self.assertIn('if (mode == "only" || mode == "strict")', source)
        self.assertIn('return "prefer";', source)
        self.assertIn('SessionScopeMode(query_scope) == "only"', source)
        self.assertIn('SessionScopeMode(*scope) == "prefer"', source)
        self.assertNotIn(' ? "prefer" : "only"', source)

    def test_cpp_native_pack_prioritizes_leaf_records_over_summaries(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "src/client/temporalstore_c_client.cc").read_text()

        self.assertIn("TypePriorityBoost", source)
        self.assertIn('record_type == "skill_section"', source)
        self.assertIn('record_type == "resource_chunk"', source)
        self.assertIn('record_type == "context_summary"', source)
        self.assertIn('score += TypePriorityBoost', source)

    def test_cpp_and_rust_native_pack_enforce_cross_session_budget(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        cpp_source = (repo / "src/client/temporalstore_c_client.cc").read_text()
        rust_source = (repo / "crates/temporalstore-rust/src/bin/matrixark_record_log.rs").read_text()

        for source in [cpp_source, rust_source]:
            self.assertIn("cross_session", source)
            self.assertIn("budget_tokens", source)
            self.assertIn("max_sessions", source)
            self.assertIn("max_candidates", source)
            self.assertIn("min_score", source)
            self.assertIn("entity_bridge_selected_ref_count", source)
            self.assertIn("same_session_first_entity_bridge_then_bounded_cross_session", source)

    def test_proxy_contract_exposes_matrixark_native_hot_path(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        openapi = (repo / "sdk/proxy/openapi.yaml").read_text()
        proxy_client = (repo / "sdk/python/temporalstore/proxy_client.py").read_text()
        adapter = (repo / "tools/matrixark_mcp_temporal_adapters.py").read_text()

        self.assertIn("/v1/matrixark/append_records", openapi)
        self.assertIn("/v1/matrixark/scan_candidates", openapi)
        self.assertIn("/v1/matrixark/retrieve_context_pack", openapi)
        self.assertIn("MatrixArkScanCandidatesRequest", openapi)
        self.assertIn("MatrixArkRetrieveContextPackRequest", openapi)
        self.assertIn("def matrixark_scan_candidates", proxy_client)
        self.assertIn("def matrixark_retrieve_context_pack", proxy_client)
        self.assertIn("MATRIXARK_TEMPORALSTORE_CPP_PROXY_ENDPOINT", adapter)
        self.assertIn("cpp_proxy_matrixark_batch_append_records", adapter)

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


class MatrixArkRustProxyAliasPolicyTest(unittest.TestCase):
    def test_rust_proxy_client_alias_keeps_cli_compatibility(self) -> None:
        self.assertIs(mcp.MatrixArkRustCliClient, mcp.MatrixArkRustProxyClient)

    def test_rust_proxy_reports_cpp_parity_hot_path_capabilities(self) -> None:
        client = mcp.MatrixArkRustProxyClient(
            proxy_path="/bin/true",
            metaserver="127.0.0.1:18000",
            namespace="deploy_ns",
            table="deploy_table",
            request_timeout_ms=1000,
            io_timeout_ms=1000,
        )

        metrics = client.metrics_snapshot()

        self.assertEqual(metrics["gateway_mode"], "rust_proxy")
        self.assertEqual(metrics["proxy_mode"], "rust_proxy_stdio")
        self.assertTrue(metrics["multiplexed_proxy_process"])
        self.assertEqual(metrics["sdk_mode"], "proxy")
        self.assertFalse(metrics["process_per_operation_enabled"])
        self.assertEqual(metrics["single_shot_mode"], "debug_only")
        self.assertTrue(metrics["supports_batch_append"])
        self.assertTrue(metrics["supports_prefix_scan"])
        self.assertEqual(metrics["prefix_scan_path"], "rust_proxy_scan_hash")
        self.assertTrue(metrics["supports_native_candidate_prefilter"])
        self.assertEqual(metrics["candidate_prefilter_path"], "rust_proxy_matrixark_scan_candidates")
        self.assertTrue(metrics["supports_native_pack_assembly"])
        self.assertEqual(metrics["native_pack_assembly_path"], "rust_proxy_matrixark_retrieve_context_pack")
        self.assertFalse(metrics["requires_c_sdk_hgetall_for_prefix_scan"])
        self.assertEqual(metrics["matrixark_append_write_path"], "rust_proxy_matrixark_batch_runtime_default")
        self.assertFalse(metrics["matrixark_batch_uses_forced_sync_durable_writes"])
        self.assertTrue(metrics["matrixark_native_batch_append_available"])
        self.assertTrue(metrics["matrixark_batch_append_uses_existing_batch_execute"])
        self.assertEqual(metrics["matrixark_batch_append_existing_batch_execute_source"], "temporalstore_matrixark_batch_append_records")
        self.assertFalse(metrics["matrixark_append_uses_per_record_hset"])
        self.assertFalse(metrics["matrixark_append_uses_generic_batch_hset_fallback"])
        self.assertTrue(metrics["separate_proxy_lanes"])
        self.assertGreater(metrics["max_inflight"], 1)
        self.assertGreaterEqual(metrics["write_lane_workers"], 1)
        self.assertGreaterEqual(metrics["read_lane_workers"], 1)
        self.assertGreaterEqual(metrics["retrieve_lane_workers"], 1)
        self.assertIn("write", metrics["lane_worker_counts"])
        self.assertIn("read", metrics["lane_worker_counts"])
        self.assertIn("retrieve", metrics["lane_worker_counts"])
        self.assertIn("proxy_queue_wait_ms_total", metrics)
        self.assertIn("serialization_ms_total", metrics)
        self.assertIn("rust_engine_ms_total", metrics)
        self.assertIn("scan_count_total", metrics)
        self.assertIn("cache_hits_total", metrics)
        self.assertIn("selected_refs_total", metrics)
        self.assertIn("dropped_refs_total", metrics)
        for lane in ("write", "read", "retrieve"):
            self.assertIn("queue_wait_ms_total", metrics["lane_metrics"][lane])

    def test_rust_proxy_metrics_record_native_hot_path_counts(self) -> None:
        client = mcp.MatrixArkRustProxyClient(
            proxy_path="/bin/true",
            metaserver="127.0.0.1:18000",
            namespace="deploy_ns",
            table="deploy_table",
            request_timeout_ms=1000,
            io_timeout_ms=1000,
        )

        client._record_call_metrics(
            "matrixark_retrieve_context_pack",
            {},
            {
                "ok": True,
                "rust_engine_time_ms": 7,
                "serialization_time_ms": 2,
                "scan_count": 11,
                "cache_hit": True,
                "selected_ref_count": 3,
                "dropped_ref_count": 5,
            },
            12.0,
            failed=False,
            lane="retrieve",
            wait_ms=4.0,
        )
        metrics = client.metrics_snapshot()

        self.assertEqual(metrics["rust_engine_ms_total"], 7)
        self.assertEqual(metrics["serialization_ms_total"], 2)
        self.assertEqual(metrics["scan_count_total"], 11)
        self.assertEqual(metrics["cache_hits_total"], 1)
        self.assertEqual(metrics["selected_refs_total"], 3)
        self.assertEqual(metrics["dropped_refs_total"], 5)
        self.assertEqual(metrics["lane_metrics"]["retrieve"]["queue_wait_ms_total"], 4)

    def test_rust_server_exposes_rust_proxy_argument(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "tools" / "matrixark_mcp_server.py").read_text()
        self.assertIn("--rust-proxy", source)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_PROXY", source)
        self.assertIn("single-shot CLI mode is debug-only", source)


if __name__ == "__main__":
    unittest.main()
