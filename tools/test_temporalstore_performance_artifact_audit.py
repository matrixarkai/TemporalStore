#!/usr/bin/env python3
"""Tests for C++/Rust performance artifact audit reporting."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from audit_temporalstore_cpp_rust_performance_artifacts import audit_artifacts
from test_temporalstore_performance_evidence_import import _matrix, _report_with_bad_qps_ratio


class PerformanceArtifactAuditTest(unittest.TestCase):
    def test_blocked_artifact_reports_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            matrix = root / "matrix.json"
            report_dir = root / "run"
            report_dir.mkdir()
            report = report_dir / "comparison.json"
            matrix.write_text(json.dumps(_matrix()), encoding="utf-8")
            report.write_text(json.dumps(_report_with_bad_qps_ratio()), encoding="utf-8")

            audit = audit_artifacts(root, matrix)

        self.assertEqual(audit["reports_scanned"], 1)
        self.assertEqual(audit["reports_with_candidate_workloads"], 1)
        self.assertEqual(audit["reports_with_importable_workloads"], 0)
        self.assertIn("10K_event_ingestion", audit["missing_required_workloads"])
        self.assertIn("1K_event_ingestion", audit["blocked_required_workloads"])
        coverage = audit["workload_coverage"]["1K_event_ingestion"]
        self.assertEqual(coverage["candidate_report_count"], 1)
        self.assertEqual(coverage["importable_report_count"], 0)
        self.assertIn("message_qps_ratio_below_0.8", coverage["blockers"])
        statuses = audit["required_workload_status"]
        self.assertEqual(statuses["1K_event_ingestion"]["status"], "blocked_no_importable")
        self.assertEqual(statuses["10K_event_ingestion"]["status"], "missing_candidate")
        self.assertIn("batch_size", statuses["1K_event_ingestion"]["next_run_hint"]["required_same_config_fields"])
        self.assertIn("selected_ref_parity=true", statuses["1K_event_ingestion"]["next_run_hint"]["required_result"])
        next_runs = audit["next_required_runs"]
        self.assertEqual(next_runs[0]["workload"], "10K_event_ingestion")
        self.assertEqual(next_runs[0]["reason"], "missing_candidate")
        self.assertEqual(next_runs[-1]["workload"], "1K_event_ingestion")
        self.assertEqual(next_runs[-1]["reason"], "blocked_no_importable")
        self.assertIn("message_qps_ratio_below_0.8", next_runs[-1]["blockers"])
        blocked = audit["entries"][0]["blocked_workloads"][0]
        self.assertEqual(blocked["workload"], "1K_event_ingestion")
        self.assertIn("message_qps_ratio_below_0.8", blocked["open_blockers"])


if __name__ == "__main__":
    unittest.main()
