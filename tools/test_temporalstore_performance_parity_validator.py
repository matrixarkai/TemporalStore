#!/usr/bin/env python3
"""Unit tests for C++/Rust performance parity matrix validation helpers."""

from __future__ import annotations

import unittest

from validate_temporalstore_cpp_rust_performance_parity import _exceeds_limit


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


if __name__ == "__main__":
    unittest.main()
