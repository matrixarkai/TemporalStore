#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

import argparse
import json
import os
import queue
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

import matrixark_mcp_server as mcp


def _scale_report_available() -> bool:
    """Whether `run_matrixark_rust_scale_report` can be imported under either layout."""
    import importlib.util

    for name in ("tools.run_matrixark_rust_scale_report", "run_matrixark_rust_scale_report"):
        try:
            if importlib.util.find_spec(name) is not None:
                return True
        except (ImportError, ValueError):
            continue
    return False


if not _scale_report_available():
    # SKIP, not an import error. `run_matrixark_rust_scale_report` is not present in this
    # repository and never has been in its history, while five files still import it -- this test
    # module, three `test_backend_policy_part*` modules and `test_ingest_envelope_schema`, which
    # import their fixtures from here.
    #
    # It is NOT recreated here on purpose. The symbols it owes include `CANONICAL_UBUNTU_REPO` and
    # `validate_runtime_host`, which name deployment infrastructure; writing a stand-in would be
    # guessing at scrubbed content and putting it in a public repository.
    #
    # The cost of leaving it as an ImportError was that six modules reported as ERRORS on every
    # run, indistinguishable from a genuine break, on a suite gate that is already red. A skip says
    # the same thing without spending a failure to say it.
    raise unittest.SkipTest(
        "run_matrixark_rust_scale_report is absent from this repository, so the backend-policy "
        "gates that import it cannot be exercised here")

try:
    from tools import matrixark_mcp_budget_pack as mcp_budget_pack
    from tools import matrixark_mcp_local_adapter as mcp_local
    from tools import matrixark_mcp_core as mcp_core
    from tools import matrixark_mcp_context_pack as mcp_context_pack
    from tools import matrixark_mcp_ingest_message_records as message_record_builders
    from tools import matrixark_mcp_retrieve_compression_scan as compression_scan
    from tools import matrixark_mcp_retrieve_index_terms as retrieve_index_terms
    from tools import matrixark_mcp_retrieve_planning as retrieve_planning
    from tools import matrixark_mcp_summary_runtime as mcp_summary_runtime
    from tools.matrixark_mcp_rust_proxy_process import rust_proxy_library_search_path
    from tools.run_matrixark_rust_scale_report import (
        comparison,
        default_lib_path,
        effective_storage_tuning_from_env,
        fallback_flags_from_backend,
        phase_scale_matrix_gate,
        production_policy_gate,
        selected_ref_count,
        storage_tuning_failures,
        summarize_retrieval_metrics,
        timeout_count,
        validate_runtime_host,
        validate_rust_runtime_path,
    )
    from tools.validate_storage_lifecycle_conformance import REPORT_PAIR_CORPUS, _load_json, validate_report_pair
except ModuleNotFoundError:  # Direct execution with PYTHONPATH=tools.
    import matrixark_mcp_budget_pack as mcp_budget_pack
    import matrixark_mcp_local_adapter as mcp_local
    import matrixark_mcp_core as mcp_core
    import matrixark_mcp_context_pack as mcp_context_pack
    import matrixark_mcp_ingest_message_records as message_record_builders
    import matrixark_mcp_retrieve_compression_scan as compression_scan
    import matrixark_mcp_retrieve_index_terms as retrieve_index_terms
    import matrixark_mcp_retrieve_planning as retrieve_planning
    import matrixark_mcp_summary_runtime as mcp_summary_runtime
    from matrixark_mcp_rust_proxy_process import rust_proxy_library_search_path
    from run_matrixark_rust_scale_report import (
        comparison,
        default_lib_path,
        effective_storage_tuning_from_env,
        fallback_flags_from_backend,
        phase_scale_matrix_gate,
        production_policy_gate,
        selected_ref_count,
        storage_tuning_failures,
        summarize_retrieval_metrics,
        timeout_count,
        validate_runtime_host,
        validate_rust_runtime_path,
    )
    from validate_storage_lifecycle_conformance import REPORT_PAIR_CORPUS, _load_json, validate_report_pair




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

        self.assertTrue(snapshot["shared_process_mode"])
        self.assertEqual(snapshot["lane_pool"], {"write": 1, "read": 1, "pack": 1, "control": 1})
        self.assertEqual(snapshot["max_inflight"], 4)
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
        self.assertEqual(snapshot["lane_pool"], {"write": 4, "read": 4, "pack": 8, "control": 1})
        self.assertEqual(snapshot["max_inflight"], 17)
        self.assertTrue(snapshot["write_pool_enabled"])
        self.assertTrue(snapshot["read_pool_enabled"])
        self.assertTrue(snapshot["pack_pool_enabled"])

    def test_rust_scale_keeps_shared_writer_and_enables_pack_lanes(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "tools" / "run_matrixark_rust_scale_report.py").read_text()
        allow_start = source.index("MATRIXARK_RUST_PROXY_ALLOW_ISOLATED_CLIENTS")
        allow_body = source[allow_start : source.index("else:", allow_start)]
        shared_body = source[source.index("else:", allow_start) : source.index("allow_c_api_bridge", allow_start)]

        self.assertIn('os.environ.setdefault("MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS", "1")', allow_body)
        self.assertIn('os.environ.setdefault("MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES", "1")', allow_body)
        self.assertIn('os.environ["MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS"] = "0"', shared_body)
        self.assertIn('os.environ["MATRIXARK_RUST_PROXY_DEDICATED_PACK_LANES"] = "1"', shared_body)
        self.assertIn('"MATRIXARK_RUST_PROXY_PACK_LANES"', source)


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

    def matrixark_retrieve_context_pack(self, request=None, **kwargs) -> dict:
        if request is None and kwargs:
            request = kwargs.get("request", {})
        if isinstance(request, str):
            request = json.loads(request)
        if request is None:
            request = {}
        self.requests.append(dict(request))
        context_pack = {
            "context_pack_id": "native-pack-1",
            "context_pack_assembly": "native_direct",
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
        return {
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "context_pack": context_pack,
        }


class _RustRecoveryParityClient(_NativeContextPackClient):
    sdk_mode = "proxy"
    metaserver = "127.0.0.1:18000"

    def __init__(self) -> None:
        super().__init__()
        self.metrics = {
            "gateway_mode": "rust_proxy",
            "proxy_mode": "rust_proxy_stdio",
            "sdk_mode": "proxy",
            "transport": "rust_proxy_stdio",
            "storage_family": "shared_store",
            "shared_store_read_throughs": 4,
            "page_store_reads": 6,
            "cache_hits_total": 2,
            "cache_misses_total": 1,
            "cache_warmup_page_refs": 3,
            "matrixark_context_records_total": 7,
        }

    def health(self) -> dict:
        return {"ok": True}

    def readiness(self) -> dict:
        return {"ok": True, "status": "ready"}

    def metrics_snapshot(self) -> dict:
        return dict(self.metrics)

    def metrics_prometheus(self) -> str:
        return ""


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

    def matrixark_batch_append_records(self, entries, *, count_key=None, count_value=None, append_options=None) -> None:
        self.calls.append(
            {
                "entries": list(entries),
                "count_key": count_key,
                "count_value": count_value,
                "append_options": append_options or {},
            }
        )



class _HashStoreClient:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}

    def get_string(self, key: str) -> str:
        return self.strings.get(key, "0")

    def put_string(self, key: str, value: str) -> None:
        self.strings[key] = value

    def hset(self, key: str, field: str, value: str) -> None:
        self.hashes.setdefault(key, {})[field] = value

    def hget(self, key: str, field: str) -> str:
        return self.hashes.get(key, {}).get(field, "")

    def batch_hset(self, entries) -> None:
        for entry in entries:
            self.hset(str(entry["key"]), str(entry["field"]), str(entry["value"]))

    def scan_hash(self, key: str):
        return {
            "records": [
                {"field": field, "value": value}
                for field, value in self.hashes.get(key, {}).items()
            ]
        }


def _direct_adapter_for_hash_store(client: _HashStoreClient) -> mcp.MatrixArkTemporalStoreDirectAdapter:
    adapter = mcp.MatrixArkTemporalStoreDirectAdapter.__new__(mcp.MatrixArkTemporalStoreDirectAdapter)
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.event_log = Path(tmpdir) / "unused.jsonl"
        mcp.MatrixArkLocalAdapter._init_local_runtime_state(adapter)
    adapter._client = client
    adapter._metaserver = ""
    adapter._namespace = "deploy_ns"
    adapter._table = "deploy_table"
    adapter._storage_prefix = "matrixark:test"
    adapter._record_hash_key = "matrixark:test:records"
    adapter._index_key = "matrixark:test:record_index"
    adapter._count_key = "matrixark:test:record_count"
    adapter._shard_size = mcp_core.DIRECT_RECORD_LOG_SHARD_SIZE
    adapter._index_cache = None
    adapter._records_cache = None
    adapter._entry_count_cache = None
    adapter._legacy_index_mode = False
    adapter._records_lock = threading.RLock()
    adapter._matrixark_proxy_mode = False
    adapter._matrixark_native_batch_append_available = False
    adapter._supported_storage_families = {"default", "local", "single_node", "shared_store"}
    adapter._write_retries = 0
    adapter._write_backoff_s = 0.0
    adapter._write_throttle_s = 0.0
    adapter._direct_write_queue_enabled = False
    adapter._raw_ingestion_prefix = f"{adapter._storage_prefix}:raw_ingestion"
    adapter._raw_record_hash_key = f"{adapter._raw_ingestion_prefix}:records"
    adapter._raw_count_key = f"{adapter._raw_ingestion_prefix}:record_count"
    adapter._raw_entry_count_cache = None
    adapter._pending_visibility_keys = set()
    adapter._audit_mode = "buffered"
    adapter._audit_buffer = []
    adapter._audit_flush_failures = 0
    adapter._metrics_lock = threading.RLock()
    adapter._metrics_started_at_ms = mcp.now_ms()
    adapter._commands_total = 0
    adapter._errors_total = 0
    adapter._timeouts_total = 0
    adapter._latency_sum_ms = 0.0
    adapter._latency_max_ms = 0.0
    adapter._latency_buckets = [0 for _ in mcp.MatrixArkServiceMetrics.LATENCY_BUCKETS_MS]
    adapter._records_written_total = 0
    adapter._records_read_total = 0
    return adapter


def _rust_adapter_for_recovery_parity(client: _RustRecoveryParityClient) -> mcp.MatrixArkTemporalStoreRustAdapter:
    adapter = mcp.MatrixArkTemporalStoreRustAdapter.__new__(mcp.MatrixArkTemporalStoreRustAdapter)
    with tempfile.TemporaryDirectory() as tmpdir:
        adapter.event_log = Path(tmpdir) / "unused-rust.jsonl"
        mcp.MatrixArkLocalAdapter._init_local_runtime_state(adapter)
    adapter._client = client
    adapter._retrieve_client = None
    adapter._summary_client = None
    adapter._retrieve_client_lock = threading.RLock()
    adapter._summary_client_lock = threading.RLock()
    adapter._dedicated_proxy_clients_enabled = False
    adapter._rust_direct_cdylib_enabled = False
    adapter._publish_visibility_after_flush = False
    adapter._metaserver = "127.0.0.1:18000"
    adapter._namespace = "deploy_ns"
    adapter._table = "deploy_table"
    adapter._storage_prefix = "matrixark:test:rust-recovery"
    adapter._record_hash_key = f"{adapter._storage_prefix}:records"
    adapter._index_key = f"{adapter._storage_prefix}:record_index"
    adapter._count_key = f"{adapter._storage_prefix}:record_count"
    adapter._shard_size = mcp_core.DIRECT_RECORD_LOG_SHARD_SIZE
    adapter._entry_count_cache = None
    adapter._records_cache = None
    adapter._index_cache = None
    adapter._retrieval_candidate_cache = {}
    adapter._retrieval_candidate_cache_lock = threading.RLock()
    adapter._records_lock = threading.RLock()
    adapter._audit_lock = threading.RLock()
    adapter._audit_buffer = []
    adapter._audit_mode = "buffered"
    adapter._audit_flush_failures = 0
    adapter._backend_ready = True
    adapter._disk_fallback_adapter = None
    adapter._disk_fallback_path = ""
    adapter._disk_fallback_enabled = False
    adapter._disk_fallback_recovery_enabled = False
    adapter._disk_fallback_recovery_attempted = False
    adapter._disk_fallback_recovery_in_progress = False
    adapter._disk_fallback_recovery_status = {"status": "not_attempted"}
    adapter._storage_family = "shared_store"
    adapter._storage_mode = "shared_store"
    adapter._replication_mode = "shared_store"
    return adapter


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


class _NativeEnvelopeContextPackClient:
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


class _EmptyNativeEnvelopeContextPackClient:
    def __init__(self) -> None:
        self.calls = []

    def matrixark_retrieve_context_pack(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "native_pack_assembly": True,
            "raw_records_returned": False,
            "python_hot_path_records": 0,
            "context_pack": {
                "context_pack_id": "native-empty-pack",
                "selected_refs": [],
                "used_context_tokens": 0,
                "retrieval_metrics": {
                    "memory_layer_budget": {
                        "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 3}},
                        "by_source_role": {"assistant": {"refs": 1, "tokens": 3}},
                        "source_message_counts_by_role": {"assistant": 2},
                    },
                    "async_pipeline_readiness": {
                        "ready_for_retrieval": False,
                        "pending_source_roles": {"assistant": 1},
                        "pending_memory_scopes": {"user_profile": 1},
                    },
                },
                "recall_policy": {
                    "memory_layer_budget": {
                        "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 3}},
                        "by_source_role": {"assistant": {"refs": 1, "tokens": 3}},
                        "source_message_counts_by_role": {"assistant": 2},
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


try:  # mixin
    from tools.test_backend_policy_part4 import _BackendPolicyPart4
except ImportError:
    from test_backend_policy_part4 import _BackendPolicyPart4

try:  # mixin
    from tools.test_backend_policy_part3 import _BackendPolicyPart3
except ImportError:
    from test_backend_policy_part3 import _BackendPolicyPart3

try:  # mixin
    from tools.test_backend_policy_part2 import _BackendPolicyPart2
except ImportError:
    from test_backend_policy_part2 import _BackendPolicyPart2

try:  # mixin
    from tools.test_backend_policy_part1 import _BackendPolicyPart1
except ImportError:
    from test_backend_policy_part1 import _BackendPolicyPart1

class MatrixArkMcpBackendPolicyTest(unittest.TestCase, _BackendPolicyPart4, _BackendPolicyPart3, _BackendPolicyPart2, _BackendPolicyPart1):
    def _storage_tuning(self) -> dict[str, object]:
        return {
            "TS_CONTEXT_PAGE_TARGET_BYTES": 65536,
            "TS_BLOCK_SLAB_TARGET_BYTES": 1073741824,
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
        self._old_local_jsonl_enabled = mcp_local.LOCAL_JSONL_ENABLED
        self._old_local_jsonl_include_bulky_fields = mcp_local.LOCAL_JSONL_INCLUDE_BULKY_FIELDS
        self._old_local_jsonl_max_bytes = mcp_local.LOCAL_JSONL_MAX_BYTES
        self._old_local_jsonl_retention_count = mcp_local.LOCAL_JSONL_RETENTION_COUNT
        self._old_local_jsonl_retention_age_ms = mcp_local.LOCAL_JSONL_RETENTION_AGE_MS
        self._old_recover_any_mode = os.environ.get("MATRIXARK_TEMPORALSTORE_RECOVER_LOCAL_STORE_ANY_MODE")
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
        mcp_core._DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.clear()

    def tearDown(self) -> None:
        mcp.MATRIXARK_MCP_PROFILE = self._old_profile
        mcp.MATRIXARK_ALLOW_LOCAL_BACKEND = self._old_allow_local
        mcp.MATRIXARK_REQUIRE_BACKEND_READY = self._old_require_ready
        mcp_local.CONTEXT_TELEMETRY_WRITE_MODE = mcp_core.CONTEXT_TELEMETRY_WRITE_MODE
        mcp_local.LOCAL_JSONL_ENABLED = self._old_local_jsonl_enabled
        mcp_local.LOCAL_JSONL_INCLUDE_BULKY_FIELDS = self._old_local_jsonl_include_bulky_fields
        mcp_local.LOCAL_JSONL_MAX_BYTES = self._old_local_jsonl_max_bytes
        mcp_local.LOCAL_JSONL_RETENTION_COUNT = self._old_local_jsonl_retention_count
        mcp_local.LOCAL_JSONL_RETENTION_AGE_MS = self._old_local_jsonl_retention_age_ms
        if self._old_recover_any_mode is None:
            os.environ.pop("MATRIXARK_TEMPORALSTORE_RECOVER_LOCAL_STORE_ANY_MODE", None)
        else:
            os.environ["MATRIXARK_TEMPORALSTORE_RECOVER_LOCAL_STORE_ANY_MODE"] = self._old_recover_any_mode
        mcp_core._DIRECT_RETRIEVAL_CANDIDATE_CACHE.clear()
        mcp_core._DIRECT_PLACEMENT_CANDIDATE_TABLE_CACHE.clear()

    def test_agent_hook_validation_accepts_codex_event_without_legacy_hook_type(self) -> None:
        hook = {
            "source": "codex",
            "hook_id": "hook-minimal-1",
            "observed_at_ms": 123456,
            "auto_captured": True,
            "trigger": "UserPromptSubmit",
            "codex_event": "UserPromptSubmit",
            "idempotency_key": "hook-minimal-1",
        }

        self.assertEqual(hook, mcp_core.validate_hook(hook))
        self.assertNotIn("hook_type", hook)

    def test_raw_ingestion_async_queue_persists_hook_write_debug(self) -> None:
        client = _HashStoreClient()
        adapter = _direct_adapter_for_hash_store(client)
        adapter._direct_write_queue_enabled = True
        adapter._direct_raw_ingestion_queue_enabled = True
        adapter._direct_write_queue_mode = "memory"
        adapter._direct_write_queue_autostart = False
        adapter._direct_write_queue_put_timeout_s = 0.1
        adapter._direct_write_queue_drain_max_batches = 64
        adapter._direct_write_queue = queue.Queue(maxsize=10)
        adapter._direct_write_enqueued_records = 0
        adapter._direct_write_enqueued_batches = 0

        record = {
            "record_type": "matrixark_ingest",
            "messages": [{"role": "user", "content": "debug me"}],
            "metadata": {"codex_event": "UserPromptSubmit"},
            "agent_hook": {"trigger": "UserPromptSubmit", "hook_id": "hook-1"},
        }

        adapter._append_raw_ingestion_records([record])

        self.assertEqual(adapter._direct_write_queue.qsize(), 1)
        queued_item = adapter._direct_write_queue.get_nowait()
        queued_record = queued_item["records"][0]
        self.assertEqual("raw_ingestion_event", queued_record["raw_record_type"])
        self.assertEqual("backfill_only", queued_record["raw_ingestion_visibility"])
        self.assertFalse(queued_record["serving_visible"])
        self.assertEqual("metadata_only_for_backfill_batching", queued_record["session_binding"])
        self.assertEqual(queued_record["matrixark_write_debug"]["write_path"], "async_memory_queue")
        self.assertEqual(queued_record["matrixark_write_debug"]["queue_mode"], "raw_ingestion")
        self.assertIn("queue_batch_id", queued_record["matrixark_write_debug"])

        adapter._flush_direct_write_items([queued_item])

        raw = client.hget("matrixark:test:raw_ingestion:records:000000", "00000000000000000000")
        stored = json.loads(raw)
        self.assertEqual("raw_ingestion_event", stored["raw_record_type"])
        self.assertEqual("backfill_only", stored["raw_ingestion_visibility"])
        self.assertFalse(stored["serving_visible"])
        self.assertEqual("metadata_only_for_backfill_batching", stored["session_binding"])
        debug = stored["matrixark_write_debug"]
        self.assertEqual(debug["write_path"], "async_queue_flush")
        self.assertEqual(debug["queue_mode"], "raw_ingestion")
        self.assertIn("queue_batch_id", debug)
        self.assertIn("flush_batch_id", debug)
        self.assertEqual(debug["persist_sequence"], 0)
        self.assertEqual(debug["persist_field"], "00000000000000000000")
        self.assertGreaterEqual(debug["queue_wait_ms"], 0)
        self.assertEqual(client.get_string("matrixark:test:raw_ingestion:record_count"), "1")

    def test_raw_agent_message_visibility_is_backfill_only_at_storage_boundary(self) -> None:
        client = _HashStoreClient()
        adapter = _direct_adapter_for_hash_store(client)
        adapter._direct_write_queue_enabled = False
        adapter._direct_raw_ingestion_queue_enabled = False

        adapter._append_raw_ingestion_records(
            [
                {
                    "record_type": "agent_message",
                    "messages": [{"role": "assistant", "content": "final answer summary"}],
                    "scope": {"session_id": "session-raw-agent"},
                    "metadata": {"codex_event": "PreviousAssistantBackfill"},
                    "agent_hook": {"hook_type": "after_llm"},
                }
            ]
        )

        raw = client.hget("matrixark:test:raw_ingestion:records:000000", "00000000000000000000")
        stored = json.loads(raw)
        self.assertEqual("agent_message", stored["record_type"])
        self.assertEqual("raw_agent_message", stored["raw_record_type"])
        self.assertEqual("backfill_only", stored["raw_ingestion_visibility"])
        self.assertFalse(stored["serving_visible"])
        self.assertEqual("metadata_only_for_backfill_batching", stored["session_binding"])
        self.assertEqual(client.get_string("matrixark:test:raw_ingestion:record_count"), "1")

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
            "TS_BLOCK_SLAB_TARGET_BYTES",
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
                    "TS_BLOCK_SLAB_TARGET_BYTES": "1073741824",
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
            self.assertEqual(tuning["TS_BLOCK_SLAB_TARGET_BYTES"], 1073741824)
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
                "native": {"status": "passed", "effective_storage_tuning": dict(tuning)},
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
        del report["backends"]["native"]["effective_storage_tuning"]["TS_PAGE_INDEX_CACHE_BYTES"]
        failures = storage_tuning_failures(report)
        self.assertIn("native missing effective_storage_tuning.TS_PAGE_INDEX_CACHE_BYTES", failures)

    def test_storage_lifecycle_read_sequence_is_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

        native_drift = json.loads(json.dumps(corpus["native"]))
        native_drift["storage_read_sequence"] = [
            "logical_key_timestamp_range",
            "page_read",
            "decode_records",
        ]
        failures = validate_report_pair(native_drift, corpus["rust"])
        self.assertTrue(any("native storage_read_sequence drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_read_sequence"]
        failures = validate_report_pair(corpus["native"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_read_sequence`", failures)

    def test_storage_lifecycle_write_sequence_is_required_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

        native_drift = json.loads(json.dumps(corpus["native"]))
        native_drift["storage_write_sequence"] = [
            "append_record",
            "route_shard_slot",
            "flush_page_block_segment",
            "publish_append_watermark",
        ]
        failures = validate_report_pair(native_drift, corpus["rust"])
        self.assertTrue(any("native storage_write_sequence drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_write_sequence"]
        failures = validate_report_pair(corpus["native"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_write_sequence`", failures)

    def test_storage_lifecycle_cold_scan_sequence_is_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

        native_drift = json.loads(json.dumps(corpus["native"]))
        native_drift["storage_cold_scan_sequence"] = [
            "timestamp_page_index_scan",
            "page_read",
            "bounded_decode",
            "hot_cache_promotion",
        ]
        failures = validate_report_pair(native_drift, corpus["rust"])
        self.assertTrue(any("native storage_cold_scan_sequence drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_cold_scan_sequence"]
        failures = validate_report_pair(corpus["native"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_cold_scan_sequence`", failures)

    def test_storage_lifecycle_phases_are_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

        native_drift = json.loads(json.dumps(corpus["native"]))
        native_drift["storage_lifecycle_phases"] = [
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
        failures = validate_report_pair(native_drift, corpus["rust"])
        self.assertTrue(any("native storage_lifecycle_phases drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_lifecycle_phases"]
        failures = validate_report_pair(corpus["native"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_lifecycle_phases`", failures)

    def test_storage_cache_layers_are_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

        native_drift = json.loads(json.dumps(corpus["native"]))
        native_drift["storage_cache_layers"] = [
            "memory_object_cache",
            "page_index_cache",
            "block_index_cache",
            "disk_cache",
            "shared_store_read_through",
        ]
        failures = validate_report_pair(native_drift, corpus["rust"])
        self.assertTrue(any("native storage_cache_layers drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_cache_layers"]
        failures = validate_report_pair(corpus["native"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_cache_layers`", failures)

    def test_storage_cache_semantics_are_exact_and_top_level(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

        native_drift = json.loads(json.dumps(corpus["native"]))
        native_drift["storage_cache_semantics"] = [
            "lookup_hot_to_cold",
            "refill_from_durable_on_miss",
            "invalidate_on_append_watermark",
            "invalidate_on_compaction_watermark",
            "cold_scan_promote",
        ]
        failures = validate_report_pair(native_drift, corpus["rust"])
        self.assertTrue(any("native storage_cache_semantics drift" in failure for failure in failures))

        rust_missing = json.loads(json.dumps(corpus["rust"]))
        del rust_missing["storage_cache_semantics"]
        failures = validate_report_pair(corpus["native"], rust_missing)
        self.assertIn("rust report missing required top-level `storage_cache_semantics`", failures)

    def test_storage_reclaim_metrics_and_scope_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

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
            failures = validate_report_pair(corpus["native"], rust_missing)
            self.assertIn(f"rust metrics missing `{metric}`", failures)

        native_bad_scope = json.loads(json.dumps(corpus["native"]))
        native_bad_scope["storage_reclaim_scope"] = {
            "owner": "matrixark_context_gc",
            "matrixark_context_gc_role": "owns_physical_reclaim",
            "physical_reclaim_context_specific": True,
        }
        failures = validate_report_pair(native_bad_scope, corpus["rust"])
        self.assertTrue(any("native storage_reclaim_scope drift" in failure for failure in failures))

        rust_missing_scope = json.loads(json.dumps(corpus["rust"]))
        del rust_missing_scope["storage_reclaim_scope"]
        failures = validate_report_pair(corpus["native"], rust_missing_scope)
        self.assertIn("rust report missing required top-level `storage_reclaim_scope`", failures)

    def test_storage_lifecycle_gap_fill_metrics_are_required(self) -> None:
        corpus = _load_json(REPORT_PAIR_CORPUS)
        self.assertEqual(validate_report_pair(corpus["native"], corpus["rust"]), [])

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
                native_missing = json.loads(json.dumps(corpus["native"]))
                del native_missing["storage_lifecycle_metrics"][metric]
                failures = validate_report_pair(native_missing, corpus["rust"])
                self.assertIn(f"native metrics missing `{metric}`", failures)

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
                "native": {
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
        self.assertIn("native_selected_refs_non_empty", blocker_names)
        self.assertIn("native_placement_index_driven", blocker_names)

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
                "native": {
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
        self.assertIn("native_selected_refs_non_empty", blocker_names)
        self.assertIn("rust_selected_refs_non_empty", blocker_names)

    def test_linux_so_preflight_rejects_windows_python(self) -> None:
        original = validate_runtime_host.__globals__["_is_windows_host"]
        validate_runtime_host.__globals__["_is_windows_host"] = lambda: True
        try:
            with self.assertRaisesRegex(RuntimeError, "invalid_host_platform"):
                validate_runtime_host("C:\\repo\\output-ubuntu22\\release\\sdk\\lib\\libtemporalstore.so")
        finally:
            validate_runtime_host.__globals__["_is_windows_host"] = original

    def test_lib_default_prefers_canonical_ubuntu_release_when_worktree_is_clean(self) -> None:
        with tempfile.TemporaryDirectory(prefix="matrixark-native-lib-policy-") as tmpdir:
            active_root = Path(tmpdir) / "clean-worktree"
            canonical_root = Path(tmpdir) / "canonical"
            canonical_lib = canonical_root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libtemporalstore.so"
            canonical_lib.parent.mkdir(parents=True)
            canonical_lib.write_text("", encoding="utf-8")

            original_root = default_lib_path.__globals__["ROOT"]
            original_canonical = default_lib_path.__globals__["CANONICAL_UBUNTU_REPO"]
            try:
                default_lib_path.__globals__["ROOT"] = active_root
                default_lib_path.__globals__["CANONICAL_UBUNTU_REPO"] = canonical_root
                self.assertEqual(default_lib_path(), str(canonical_lib))
            finally:
                default_lib_path.__globals__["ROOT"] = original_root
                default_lib_path.__globals__["CANONICAL_UBUNTU_REPO"] = original_canonical

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
                "native": {
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
                "native": {"status": "passed", "retrieve": {"stage_metrics": dict(metrics)}},
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
                "recall_policy": {
                    "memory_layer_budget": {
                        "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 12}},
                        "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 12}},
                        "by_extraction_phase": {"final": {"refs": 1, "tokens": 12}},
                        "final_session_boundary_ref_count": 1,
                        "total_selected_refs": 1,
                        "total_selected_tokens": 12,
                    }
                },
            },
            audit_record={
                "record_type": "context_pack_audit",
                "context_pack_id": "pack-full",
                "recall_policy": {
                    "memory_layer_budget": {
                        "by_memory_scope": {"user_profile": {"refs": 1, "tokens": 12}},
                        "by_session_continuity": {"cross_session": {"refs": 1, "tokens": 12}},
                        "by_extraction_phase": {"final": {"refs": 1, "tokens": 12}},
                        "final_session_boundary_ref_count": 1,
                        "total_selected_refs": 1,
                        "total_selected_tokens": 12,
                    }
                },
            },
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
        telemetry_budget = adapter.audit_appended[0]["memory_layer_budget"]
        self.assertEqual(telemetry_budget["by_memory_scope"]["user_profile"], {"refs": 1, "tokens": 12})
        self.assertEqual(telemetry_budget["by_session_continuity"]["cross_session"], {"refs": 1, "tokens": 12})
        audit_budget = adapter.audit_appended[1]["memory_layer_budget"]
        self.assertEqual(audit_budget["by_extraction_phase"]["final"], {"refs": 1, "tokens": 12})
        self.assertEqual(audit_budget["final_session_boundary_ref_count"], 1)

    def test_rust_proxy_library_search_path_includes_temporalstore_lib_dir(self) -> None:
        search_path = rust_proxy_library_search_path(
            "/opt/temporalstore/bin/matrixark_rust_proxy",
            {
                "TEMPORALSTORE_LIB": "/opt/temporalstore/sdk/lib/libtemporalstore.so",
                "LD_LIBRARY_PATH": "/usr/local/lib:/opt/temporalstore/bin",
            },
        )
        self.assertEqual(
            search_path.split(":"),
            ["/opt/temporalstore/bin", "/opt/temporalstore/sdk/lib", "/usr/local/lib"],
        )

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
        self.assertEqual(
            selected_ref_count(
                {
                    "context_pack": {
                        "remote_context_refs": [
                            {"ref_type": "event", "ref_hash": "a"},
                            {"ref_type": "summary", "ref_hash": "b"},
                        ]
                    }
                }
            ),
            2,
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
        self.assertTrue(result["phase0_correctness"]["backend_values"]["native"]["correctness_evidence"]["selected_ref_parity"])
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
        self.assertFalse(result["phase0_correctness"]["backend_values"]["native"]["correctness_evidence"]["selected_ref_parity"])
        self.assertFalse(result["phase0_correctness"]["backend_values"]["rust"]["correctness_evidence"]["selected_ref_parity"])
        selected_row = next(row for row in result["rows"] if row["metric"] == "selected_refs_avg")
        self.assertFalse(selected_row["parity_passed"])
        self.assertIn(">= 1", selected_row["parity_threshold"])

    def test_native_zero_append_metrics_do_not_fall_back_to_adapter_average(self) -> None:
        try:
            from tools.matrixark_mcp_temporal_adapters import _float_metric_or_default
        except ModuleNotFoundError:  # Direct execution with PYTHONPATH=tools.
            from matrixark_mcp_temporal_adapters import _float_metric_or_default

        self.assertEqual(
            _float_metric_or_default({"append_engine_ms": 0.0}, "append_engine_ms", 400.0),
            0.0,
        )
        self.assertEqual(
            _float_metric_or_default({"append_queue_wait_ms": "0"}, "append_queue_wait_ms", 300.0),
            0.0,
        )
        self.assertEqual(
            _float_metric_or_default({}, "append_engine_ms", 400.0),
            400.0,
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
        self.assertEqual(result["rust_vs_parity"]["feature_parity"]["status"], "passed")
        self.assertTrue(result["rust_vs_parity"]["feature_parity"]["passed"])
        self.assertEqual(result["rust_vs_parity"]["performance_parity"]["status"], "passed")
        self.assertTrue(result["rust_vs_parity"]["performance_parity"]["passed"])
        self.assertTrue(result["rust_vs_parity"]["production_performance_parity"]["passed"])






class MatrixArkRustProxyAliasPolicyTest(unittest.TestCase):
    def test_rust_proxy_client_alias_keeps_cli_compatibility(self) -> None:
        self.assertIs(mcp.MatrixArkRustCliClient, mcp.MatrixArkRustProxyClient)

    def test_rust_proxy_reports_parity_hot_path_capabilities(self) -> None:
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
                "retrieval_metrics": {
                    "memory_layer_budget": {
                        "by_memory_scope": {"user_profile": {"refs": 2, "tokens": 34}},
                        "by_session_continuity": {"cross_session": {"refs": 2, "tokens": 34}},
                        "by_extraction_phase": {"final": {"refs": 1, "tokens": 20}},
                        "by_ref_type": {"summary": {"refs": 1, "tokens": 20}},
                        "by_entity_type": {"decision": {"refs": 1, "tokens": 20}},
                        "by_source_role": {"assistant": {"refs": 1, "tokens": 20}},
                        "by_hook_type": {"hook_boundary": {"refs": 1, "tokens": 20}},
                        "by_codex_event": {"Stop": {"refs": 1, "tokens": 20}},
                        "source_message_counts_by_role": {"assistant": 3, "user": 1},
                        "source_hook_counts_by_type": {"hook_boundary": 4},
                        "source_codex_event_counts_by_event": {"Stop": 4},
                        "final_session_boundary_ref_count": 1,
                        "provisional_ref_count": 1,
                        "final_ref_count": 1,
                        "total_selected_refs": 3,
                        "total_selected_tokens": 44,
                    }
                },
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
        layer_totals = metrics["memory_layer_budget_totals"]
        self.assertEqual(layer_totals["by_memory_scope"]["user_profile"], {"refs": 2, "tokens": 34})
        self.assertEqual(layer_totals["by_session_continuity"]["cross_session"], {"refs": 2, "tokens": 34})
        self.assertEqual(layer_totals["by_ref_type"]["summary"], {"refs": 1, "tokens": 20})
        self.assertEqual(layer_totals["by_entity_type"]["decision"], {"refs": 1, "tokens": 20})
        self.assertEqual(layer_totals["by_source_role"]["assistant"], {"refs": 1, "tokens": 20})
        self.assertEqual(layer_totals["by_hook_type"]["hook_boundary"], {"refs": 1, "tokens": 20})
        self.assertEqual(layer_totals["by_codex_event"]["Stop"], {"refs": 1, "tokens": 20})
        self.assertEqual(layer_totals["source_message_counts_by_role"], {"assistant": 3, "user": 1})
        self.assertEqual(layer_totals["source_hook_counts_by_type"], {"hook_boundary": 4})
        self.assertEqual(layer_totals["source_codex_event_counts_by_event"], {"Stop": 4})
        self.assertEqual(layer_totals["final_session_boundary_ref_count"], 1)
        self.assertEqual(layer_totals["provisional_ref_count"], 1)
        self.assertEqual(layer_totals["final_ref_count"], 1)
        self.assertEqual(layer_totals["total_selected_refs"], 3)
        self.assertEqual(layer_totals["total_selected_tokens"], 44)

    def test_rust_server_exposes_rust_proxy_argument(self) -> None:
        repo = Path(__file__).resolve().parents[1]
        source = (repo / "tools" / "matrixark_mcp_server.py").read_text()
        self.assertIn("--rust-proxy", source)
        self.assertIn("MATRIXARK_TEMPORALSTORE_RUST_PROXY", source)
        self.assertIn("single-shot CLI mode is debug-only", source)


if __name__ == "__main__":
    unittest.main()




