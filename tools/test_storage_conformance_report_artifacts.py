#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Tests for committed storage parity report artifact validation."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from validate_storage_lifecycle_conformance import (
    REQUIRED_STORAGE_CACHE_LAYERS,
    REQUIRED_STORAGE_CACHE_SEMANTICS,
    REQUIRED_STORAGE_CACHE_CONTRACT_FIELDS,
    REQUIRED_STORAGE_COLD_SCAN_SEQUENCE,
    REQUIRED_STORAGE_COLD_SCAN_METRICS,
    REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS,
    REQUIRED_STORAGE_INDEX_BEHAVIORS,
    REQUIRED_STORAGE_INDEX_CONTRACT_FIELDS,
    REQUIRED_STORAGE_LIFECYCLE_METRICS,
    REQUIRED_STORAGE_LIFECYCLE_PHASES,
    REQUIRED_STORAGE_MANAGER_CONTRACT_FIELDS,
    REQUIRED_STORAGE_MANAGER_PHASE_METRICS,
    REQUIRED_STORAGE_READ_SEQUENCE,
    REQUIRED_STORAGE_READ_METRICS,
    REQUIRED_STORAGE_READ_RESULT_FIELDS,
    REQUIRED_STORAGE_RECLAIM_CONTRACT_FIELDS,
    REQUIRED_STORAGE_RECLAIM_SEMANTICS,
    REQUIRED_STORAGE_RECLAIM_SCOPE,
    REQUIRED_STORAGE_WRITE_METRICS,
    REQUIRED_STORAGE_WRITE_RESULT_FIELDS,
    REQUIRED_STORAGE_WRITE_SEQUENCE,
    validate_report_pair,
)
from validate_storage_conformance_report_artifacts import REQUIRED_PUBLIC_STORAGE_CONTRACT, validate_artifacts
from validate_storage_tuning_conformance import EXPECTED_DEFAULTS


def _zero_metrics() -> dict[str, int]:
    return {name: 0 for name in REQUIRED_STORAGE_LIFECYCLE_METRICS}


def _contract_with_fields(fields: list[str]) -> dict[str, object]:
    return {field: 0 for field in fields}


def _manager_contract() -> dict[str, object]:
    return {
        "manager_identity": "StorageManager/StoreManager",
        "native_public_name": "StorageManager",
        "rust_public_name": "StoreManager",
        "phase_order": list(REQUIRED_STORAGE_LIFECYCLE_PHASES),
        "phase_metrics": dict(REQUIRED_STORAGE_MANAGER_PHASE_METRICS),
        "phase_counts": {phase: 0 for phase in REQUIRED_STORAGE_LIFECYCLE_PHASES},
        "loop_metric": "storage_manager_loop_ms",
        "loop_ms": 0,
        "phase_order_enforced": True,
        "missing_phase_count": 0,
    }


def _index_contract() -> dict[str, object]:
    return {
        "page_address_codec": "PageAddress",
        "block_address_codec": "BlockAddress",
        "stable_order": ["shard_id", "zone_id", "segment_id", "page_id", "offset"],
        "slot_index": "slot -> object/page refs",
        "object_index_entry": "{model/table/object_key} -> current page chain",
        "page_index": "logical timestamp/key ranges -> page addresses",
        "block_index": "page addresses -> physical durable locations",
        "required_behaviors": list(REQUIRED_STORAGE_INDEX_BEHAVIORS),
        "page_address_encode_decode": True,
        "block_address_encode_decode": True,
        "stable_order_verified": True,
        "timestamp_range_lookup_verified": True,
        "slot_index_entry_count": 1,
        "slot_object_ref_count": 1,
        "slot_page_ref_count": 1,
        "object_index_entry_count": 1,
        "page_index_entry_count": 1,
        "block_index_entry_count": 1,
        "restart_rebuild_verified": True,
        "unreadable_page_refs": 0,
        "checksum_mismatches": 0,
    }


def _cache_contract() -> dict[str, object]:
    return {
        "layers": list(REQUIRED_STORAGE_CACHE_LAYERS),
        "semantics": list(REQUIRED_STORAGE_CACHE_SEMANTICS),
        "metrics": [
            "memory_cache_hits",
            "memory_cache_misses",
            "page_index_cache_hits",
            "page_index_cache_misses",
            "block_index_cache_hits",
            "block_index_cache_misses",
            "disk_cache_hits",
            "disk_cache_misses",
            "shared_store_read_throughs",
            "cache_refills",
            "cache_invalidations",
            "cache_writeback_queue_depth",
            "cache_writeback_rejections",
        ],
        "hot_to_cold_lookup": True,
        "durable_refill_on_miss": True,
        "append_watermark_invalidation": True,
        "compaction_watermark_invalidation": True,
        "cold_scan_no_promote": True,
        "writeback_backpressure_measured": True,
        "cache_refills": 0,
        "cache_invalidations": 0,
        "cache_writeback_queue_depth": 0,
        "cache_writeback_rejections": 0,
        "hot_cache_promotions": 0,
    }


def _reclaim_contract() -> dict[str, object]:
    return {
        "cache_eviction_frees_memory_only": True,
        "logical_gc_marks_expired_deletable": True,
        "physical_reclaim_requires_compaction_or_safe_skip": True,
        "cache_evictions": 0,
        "tombstone_records": 0,
        "stale_page_tombstones": 0,
        "stale_block_tombstones": 0,
        "stale_pages_rewritten": 0,
        "stale_pages_skipped": 0,
        "stale_blocks_rewritten": 0,
        "stale_blocks_skipped": 0,
        "reclaimable_bytes": 0,
        "compaction_reclaimed_bytes": 0,
        "physical_reclaimed_bytes": 0,
        "physical_reclaim_errors": 0,
    }


def _valid_report(backend: str) -> dict[str, object]:
    return {
        "backend": backend,
        "effective_storage_tuning": dict(EXPECTED_DEFAULTS),
        "public_storage_contract": dict(REQUIRED_PUBLIC_STORAGE_CONTRACT),
        "storage_write_sequence": list(REQUIRED_STORAGE_WRITE_SEQUENCE),
        "storage_write_contract": _contract_with_fields(
            [*REQUIRED_STORAGE_WRITE_RESULT_FIELDS, *REQUIRED_STORAGE_WRITE_METRICS]
        ),
        "storage_read_sequence": list(REQUIRED_STORAGE_READ_SEQUENCE),
        "storage_read_contract": _contract_with_fields(
            [*REQUIRED_STORAGE_READ_RESULT_FIELDS, *REQUIRED_STORAGE_READ_METRICS]
        ),
        "storage_cold_scan_sequence": list(REQUIRED_STORAGE_COLD_SCAN_SEQUENCE),
        "storage_cold_scan_contract": _contract_with_fields(
            [*REQUIRED_STORAGE_COLD_SCAN_RESULT_FIELDS, *REQUIRED_STORAGE_COLD_SCAN_METRICS]
        ),
        "storage_lifecycle_phases": list(REQUIRED_STORAGE_LIFECYCLE_PHASES),
        "storage_lifecycle_metrics": _zero_metrics(),
        "storage_cache_layers": list(REQUIRED_STORAGE_CACHE_LAYERS),
        "storage_cache_semantics": list(REQUIRED_STORAGE_CACHE_SEMANTICS),
        "storage_reclaim_semantics": list(REQUIRED_STORAGE_RECLAIM_SEMANTICS),
        "storage_reclaim_scope": dict(REQUIRED_STORAGE_RECLAIM_SCOPE),
        "storage_cache_contract": _cache_contract(),
        "storage_reclaim_contract": _reclaim_contract(),
        "storage_manager_contract": _manager_contract(),
        "storage_index_contract": _index_contract(),
    }


class StorageParityReportArtifactTest(unittest.TestCase):
    def test_accepts_canonical_rust_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            (report_dir / "native.json").write_text(
                json.dumps(_valid_report("native")), encoding="utf-8"
            )
            (report_dir / "rust.json").write_text(
                json.dumps(_valid_report("rust")), encoding="utf-8"
            )

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 2)
        self.assertEqual(failures, [])

    def test_rejects_missing_metric_and_legacy_alias_leak(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            report = _valid_report("native")
            del report["storage_lifecycle_metrics"]["storage_manager_prepare_count"]  # type: ignore[index]
            report["page_store"] = {"leaked": True}
            (report_dir / "native.json").write_text(json.dumps(report), encoding="utf-8")

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 1)
        self.assertTrue(
            any("missing storage lifecycle metric `storage_manager_prepare_count`" in item for item in failures)
        )
        self.assertTrue(any("legacy alias exposed outside compatibility_aliases" in item for item in failures))

    def test_rejects_nested_storage_alias_leaks_outside_compatibility_container(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            report = _valid_report("rust")
            report["storage_write_contract"]["zone"] = "leaked implementation name"  # type: ignore[index]
            report["storage_read_contract"]["page_segment"] = "leaked implementation name"  # type: ignore[index]
            report["storage_cache_contract"]["compatibility_aliases"] = {"block_store": "storage_zone"}  # type: ignore[index]
            (report_dir / "rust.json").write_text(json.dumps(report), encoding="utf-8")

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 1)
        self.assertTrue(any("storage_write_contract.zone" in item for item in failures))
        self.assertTrue(any("storage_read_contract.page_segment" in item for item in failures))
        self.assertFalse(any("compatibility_aliases.block_store" in item for item in failures))

    def test_rejects_comparison_backend_shape_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            native = _valid_report("native")
            rust = copy.deepcopy(_valid_report("rust"))
            rust["storage_cache_layers"] = ["memory_object_cache"]
            (report_dir / "comparison.json").write_text(
                json.dumps({"backends": {"native": native, "rust": rust}}),
                encoding="utf-8",
            )

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 2)
        self.assertTrue(any("rust storage_cache_layers drift" in item for item in failures))

    def test_rejects_thin_contract_sections(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            report = _valid_report("rust")
            del report["storage_write_contract"]["append_engine_ms"]  # type: ignore[index]
            del report["storage_index_contract"]["restart_rebuild_verified"]  # type: ignore[index]
            (report_dir / "rust.json").write_text(json.dumps(report), encoding="utf-8")

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 1)
        self.assertTrue(any("storage_write_contract missing `append_engine_ms`" in item for item in failures))
        self.assertTrue(
            any("storage_index_contract missing `restart_rebuild_verified`" in item for item in failures)
        )

    def test_rejects_semantic_contract_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            report = _valid_report("rust")
            report["storage_manager_contract"]["phase_order_enforced"] = False  # type: ignore[index]
            report["storage_index_contract"]["required_behaviors"] = []  # type: ignore[index]
            report["storage_cache_contract"]["cold_scan_no_promote"] = False  # type: ignore[index]
            report["storage_reclaim_contract"]["physical_reclaim_errors"] = 1  # type: ignore[index]
            (report_dir / "rust.json").write_text(json.dumps(report), encoding="utf-8")

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 1)
        self.assertTrue(
            any("storage_manager_contract.phase_order_enforced must be true" in item for item in failures)
        )
        self.assertTrue(any("storage_index_contract.required_behaviors drift" in item for item in failures))
        self.assertTrue(any("storage_cache_contract.cold_scan_no_promote must be true" in item for item in failures))
        self.assertTrue(any("storage_reclaim_contract.physical_reclaim_errors must be zero" in item for item in failures))

    def test_pair_validator_rejects_missing_public_storage_contract(self) -> None:
        native = _valid_report("native")
        rust = _valid_report("rust")
        del native["public_storage_contract"]

        failures = validate_report_pair(native, rust)

        self.assertTrue(
            any("native report missing required top-level `public_storage_contract`" in item for item in failures)
        )
        self.assertTrue(any("native public storage shape missing canonical `page_address`" in item for item in failures))

    def test_rejects_effective_storage_tuning_value_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            report = _valid_report("rust")
            report["effective_storage_tuning"]["TS_STORAGE_ZONE_SIZE"] = 123  # type: ignore[index]
            (report_dir / "rust.json").write_text(json.dumps(report), encoding="utf-8")

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 1)
        # Read from EXPECTED_DEFAULTS rather than written out: this assertion carried 10485760,
        # a value the engine has never used, so correcting the expectation broke a test that was
        # only ever checking the literal it had been given.
        expected = EXPECTED_DEFAULTS["TS_STORAGE_ZONE_SIZE"]
        self.assertTrue(
            any(
                f"effective storage tuning `TS_STORAGE_ZONE_SIZE` drift: expected {expected} got 123"
                in item
                for item in failures
            ),
            f"no drift failure naming the expected value {expected}: {failures}",
        )


if __name__ == "__main__":
    unittest.main()
