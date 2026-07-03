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
        blocked = audit["entries"][0]["blocked_workloads"][0]
        self.assertEqual(blocked["workload"], "1K_event_ingestion")
        self.assertIn("message_qps_ratio_below_0.8", blocked["open_blockers"])


if __name__ == "__main__":
    unittest.main()
