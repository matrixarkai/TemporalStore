#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
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


class TheTrackedTreeCarriesNoLocalPathTest(unittest.TestCase):
    """The same check the CI job runs, run here.

    Everything above exercises the scanner on a fixture: it rejects what it should and skips what
    it should. Nothing asked it about the repository, so a local path could be committed, pass
    every local suite, and fail only in `oss-readiness` -- which is how
    `C:/Users/<name>/.claude` reached `main` in a test fixture and turned every open pull
    request's gate red for a reason none of them caused.

    The scan takes well under a second. There is no reason the answer should arrive from CI first.
    """

    def test_no_tracked_file_carries_a_private_path(self) -> None:
        try:
            readiness.validate_no_private_paths()
        except SystemExit as exit_error:
            self.fail(str(exit_error))

    def test_the_scan_actually_looked_at_the_tree(self) -> None:
        """The floor: an empty file list makes the rule above pass without reading anything, and
        `repository_files()` falls back to a directory walk when git is unavailable -- exactly the
        condition under which a silent zero would look like a clean tree."""
        tracked = readiness.repository_files()
        self.assertGreater(len(tracked), 200,
                           "the scan found %d tracked files" % len(tracked))

    def test_the_tokens_it_looks_for_are_still_there(self) -> None:
        """And it must still be looking for something. A rule with an empty token list would pass
        on any tree at all."""
        self.assertGreaterEqual(len(readiness.PRIVATE_PATH_TOKENS), 3)


if __name__ == "__main__":
    unittest.main()
