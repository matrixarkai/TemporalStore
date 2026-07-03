#!/usr/bin/env python3
"""Tests for committed storage parity report artifact validation."""

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from validate_storage_lifecycle_parity import (
    REQUIRED_STORAGE_CACHE_LAYERS,
    REQUIRED_STORAGE_CACHE_SEMANTICS,
    REQUIRED_STORAGE_COLD_SCAN_SEQUENCE,
    REQUIRED_STORAGE_LIFECYCLE_METRICS,
    REQUIRED_STORAGE_LIFECYCLE_PHASES,
    REQUIRED_STORAGE_READ_SEQUENCE,
    REQUIRED_STORAGE_RECLAIM_SEMANTICS,
)
from validate_storage_parity_report_artifacts import REQUIRED_PUBLIC_STORAGE_CONTRACT, validate_artifacts
from validate_storage_tuning_parity import EXPECTED_KNOBS


def _zero_metrics() -> dict[str, int]:
    return {name: 0 for name in REQUIRED_STORAGE_LIFECYCLE_METRICS}


def _valid_report(backend: str) -> dict[str, object]:
    return {
        "backend": backend,
        "effective_storage_tuning": {
            key: (True if key == "TS_COLD_SCAN_NO_CACHE_FILL" else 1)
            for key in EXPECTED_KNOBS
        },
        "public_storage_contract": dict(REQUIRED_PUBLIC_STORAGE_CONTRACT),
        "storage_read_sequence": list(REQUIRED_STORAGE_READ_SEQUENCE),
        "storage_cold_scan_sequence": list(REQUIRED_STORAGE_COLD_SCAN_SEQUENCE),
        "storage_lifecycle_phases": list(REQUIRED_STORAGE_LIFECYCLE_PHASES),
        "storage_lifecycle_metrics": _zero_metrics(),
        "storage_cache_layers": list(REQUIRED_STORAGE_CACHE_LAYERS),
        "storage_cache_semantics": list(REQUIRED_STORAGE_CACHE_SEMANTICS),
        "storage_reclaim_semantics": list(REQUIRED_STORAGE_RECLAIM_SEMANTICS),
        "storage_cache_contract": {"layers": list(REQUIRED_STORAGE_CACHE_LAYERS)},
        "storage_reclaim_contract": {"physical_reclaim_errors": 0},
        "storage_manager_contract": {"phase_order": list(REQUIRED_STORAGE_LIFECYCLE_PHASES)},
        "storage_index_contract": {"page_address_codec": "PageAddress"},
    }


class StorageParityReportArtifactTest(unittest.TestCase):
    def test_accepts_canonical_cpp_rust_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            (report_dir / "cpp.json").write_text(
                json.dumps(_valid_report("cpp")), encoding="utf-8"
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
            report = _valid_report("cpp")
            del report["storage_lifecycle_metrics"]["storage_manager_prepare_count"]  # type: ignore[index]
            report["page_store"] = {"leaked": True}
            (report_dir / "cpp.json").write_text(json.dumps(report), encoding="utf-8")

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 1)
        self.assertTrue(
            any("missing storage lifecycle metric `storage_manager_prepare_count`" in item for item in failures)
        )
        self.assertTrue(any("legacy alias exposed outside compatibility_aliases" in item for item in failures))

    def test_rejects_comparison_backend_shape_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_dir = root / "parity_smoke"
            report_dir.mkdir()
            cpp = _valid_report("cpp")
            rust = copy.deepcopy(_valid_report("rust"))
            rust["storage_cache_layers"] = ["memory_object_cache"]
            (report_dir / "comparison.json").write_text(
                json.dumps({"backends": {"cpp": cpp, "rust": rust}}),
                encoding="utf-8",
            )

            scanned, failures = validate_artifacts(root)

        self.assertEqual(scanned, 2)
        self.assertTrue(any("rust storage_cache_layers drift" in item for item in failures))


if __name__ == "__main__":
    unittest.main()
