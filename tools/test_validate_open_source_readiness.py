#!/usr/bin/env python3
"""Unit coverage for open-source readiness validation."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import validate_open_source_readiness as readiness


class OpenSourceReadinessTest(unittest.TestCase):
    def test_private_path_scan_skips_redaction_fixture_files(self) -> None:
        with patch.object(
            readiness,
            "repository_files",
            return_value=[
                "tools/validate_open_source_readiness.py",
                "tools/validate_temporalstore_performance_execution_redaction.py",
                "tools/test_temporalstore_performance_execution_redaction.py",
            ],
        ):
            readiness.validate_no_private_paths()

    def test_private_path_scan_rejects_non_fixture_file(self) -> None:
        with patch.object(readiness, "repository_files", return_value=["docs/leak.md"]):
            with patch.object(readiness.Path, "is_file", return_value=True):
                private_marker = "/mnt/c/" + "Users/example"
                with patch.object(readiness.Path, "read_text", return_value=f"leaked {private_marker}"):
                    with self.assertRaisesRegex(SystemExit, "tracked files contain local/private path markers"):
                        readiness.validate_no_private_paths()


if __name__ == "__main__":
    unittest.main()
