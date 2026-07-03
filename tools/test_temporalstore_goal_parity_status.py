#!/usr/bin/env python3
"""Unit tests for goal-level C++/Rust TemporalStore parity status validation."""

from __future__ import annotations

import json
import pathlib
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from validate_temporalstore_cpp_rust_goal_parity import (  # noqa: E402
    STATUS,
    validate_status,
)
from validate_temporalstore_cpp_rust_performance_parity import (  # noqa: E402
    MATRIX,
    _validate_missing_evidence_hint,
)


class TemporalStoreGoalParityStatusTest(unittest.TestCase):
    def _status(self) -> dict:
        return json.loads(STATUS.read_text(encoding="utf-8"))

    def _performance_matrix(self) -> dict:
        return json.loads(MATRIX.read_text(encoding="utf-8"))

    def test_current_goal_status_is_valid_but_not_complete(self) -> None:
        data = self._status()

        self.assertEqual(validate_status(data), [])
        self.assertFalse(data["global_status"]["goal_complete"])
        self.assertFalse(data["global_status"]["production_performance_parity"])

    def test_required_scale_metrics_include_storage_watermarks_and_scan_metrics(self) -> None:
        data = self._status()
        data["required_scale_matrix"]["required_metrics"].remove("scanned_records")
        data["required_scale_matrix"]["required_metrics"].remove("append_watermark")

        failures = validate_status(data)

        self.assertTrue(
            any(
                "required_scale_matrix.required_metrics missing: scanned_records, append_watermark"
                in failure
                for failure in failures
            )
        )

    def test_generated_from_requires_core_phase_validators(self) -> None:
        data = self._status()
        data["generated_from"].remove("tools/validate_grafana_metrics_parity.py")
        data["generated_from"].remove("tools/validate_storage_proxy_client_parity_coverage.py")

        failures = validate_status(data)

        self.assertTrue(
            any(
                "generated_from missing: tools/validate_storage_proxy_client_parity_coverage.py, tools/validate_grafana_metrics_parity.py"
                in failure
                for failure in failures
            )
        )

    def test_storage_manager_evidence_is_required(self) -> None:
        data = self._status()
        data["areas"]["storage_manager_parity"]["evidence"].remove(
            "storage_manager_follower_cursor_safety_count"
        )

        failures = validate_status(data)

        self.assertIn(
            "storage_manager_parity.evidence missing: storage_manager_follower_cursor_safety_count",
            failures,
        )

    def test_store_manager_pipeline_evidence_is_required(self) -> None:
        data = self._status()
        data["areas"]["store_manager_parity"]["evidence"].remove("storage_cold_scan_sequence")

        failures = validate_status(data)

        self.assertIn(
            "store_manager_parity.evidence missing: storage_cold_scan_sequence",
            failures,
        )

    def test_zone_stream_segment_slot_evidence_is_required(self) -> None:
        data = self._status()
        data["areas"]["zone_stream_segment_slot_parity"]["evidence"].remove(
            "slot_owner_mismatch_count"
        )

        failures = validate_status(data)

        self.assertIn(
            "zone_stream_segment_slot_parity.evidence missing: slot_owner_mismatch_count",
            failures,
        )

    def test_missing_live_performance_rows_require_actionable_next_run_hint(self) -> None:
        data = self._performance_matrix()
        row = next(row for row in data["rows"] if row["workload"] == "10K_event_ingestion")
        row = json.loads(json.dumps(row))
        del row["next_run_hint"]
        failures: list[str] = []

        _validate_missing_evidence_hint(row, failures)

        self.assertIn("10K_event_ingestion missing_live_evidence requires next_run_hint", failures)

    def test_next_run_hint_command_is_validated(self) -> None:
        data = self._performance_matrix()
        row = next(row for row in data["rows"] if row["workload"] == "retrieve_workers_16")
        row = json.loads(json.dumps(row))
        row["next_run_hint"]["command"].remove("--require-perf-parity")
        failures: list[str] = []

        _validate_missing_evidence_hint(row, failures)

        self.assertIn(
            "retrieve_workers_16 next_run_hint.command missing `--require-perf-parity`",
            failures,
        )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
