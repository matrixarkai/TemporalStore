#!/usr/bin/env python3
"""Unit tests for C++/Rust performance parity matrix validation helpers."""

from __future__ import annotations

import unittest

from validate_temporalstore_cpp_rust_performance_parity import _exceeds_limit, _validate_metric_block


class PerformanceParityValidatorTest(unittest.TestCase):
    def test_limit_allows_values_below_or_equal_threshold(self) -> None:
        self.assertFalse(_exceeds_limit(0, 2))
        self.assertFalse(_exceeds_limit(1, 2))
        self.assertFalse(_exceeds_limit(2, 2))

    def test_limit_blocks_values_above_threshold(self) -> None:
        self.assertTrue(_exceeds_limit(3, 2))

    def test_limit_ignores_missing_or_non_numeric_values(self) -> None:
        self.assertFalse(_exceeds_limit(None, 0))
        self.assertFalse(_exceeds_limit("unknown", 0))
        self.assertFalse(_exceeds_limit(True, 0))

    def test_selected_ref_parity_is_required_by_default(self) -> None:
        failures: list[str] = []
        row = {"workload": "1K_event_ingestion", "cpp": {"selected_ref_parity": False}}

        _validate_metric_block(row, "cpp", failures)

        self.assertIn("1K_event_ingestion cpp.selected_ref_parity must be true", failures)

    def test_selected_ref_parity_can_follow_threshold_policy(self) -> None:
        failures: list[str] = []
        row = {"workload": "1K_event_ingestion", "cpp": {"selected_ref_parity": False}}

        _validate_metric_block(row, "cpp", failures, require_selected_ref_parity=False)

        self.assertNotIn("1K_event_ingestion cpp.selected_ref_parity must be true", failures)


if __name__ == "__main__":
    unittest.main()
