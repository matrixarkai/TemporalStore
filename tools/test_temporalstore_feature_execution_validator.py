#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import validate_temporalstore_cpp_rust_feature_execution as validator


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _static_corpus() -> dict:
    return {
        "coverage": {
            "cpp_adapter_coverage": [
                {
                    "family": "storage/cache",
                    "status": "temporary_static_surface_gate",
                    "suites": ["cpp_storage_parity"],
                    "blocker": "native C++ runner missing",
                    "expected_runner_command": (
                        "TS_CPP_UNIFIED_NATIVE_CMD=cpp-storage-corpus-runner "
                        "python3 tools/run_temporalstore_unified_tests.py "
                        "--family storage/cache --cpp --corpus {corpus} --require-cpp-native"
                    ),
                    "comparison_command": (
                        "python3 tools/compare_unified_cpp_rust_case_reports.py "
                        "--rust-report rust.json --cpp-report cpp.json "
                        "--require-schema temporalstore_unified_case_report_v1"
                    ),
                    "exit_criteria": [
                        "native C++ runner emits temporalstore_unified_case_report_v1",
                        "selected shared cases pass",
                        "no static surface gate remains",
                    ],
                }
            ]
        },
        "cases": [
            {
                "case": "cache_refill",
                "family": "storage/cache",
                "steps": [{"command": {"suite": "cpp_storage_parity"}}],
            }
        ],
    }


def _static_matrix(blockers: list[str]) -> dict:
    return {
        "schema": "temporalstore_cpp_rust_feature_execution_matrix_v1",
        "status": {"feature_correct": False, "open_blockers": blockers},
        "rows": [
            {
                "family": "storage/cache",
                "status": "temporary_static_surface_gate",
                "native_cpp_executable": False,
                "selected_case_count": 1,
                "blocker": "native C++ runner missing",
                "expected_runner_command": (
                    "TS_CPP_UNIFIED_NATIVE_CMD=cpp-storage-corpus-runner "
                    "python3 tools/run_temporalstore_unified_tests.py "
                    "--family storage/cache --cpp --corpus {corpus} --require-cpp-native"
                ),
                "suites": ["cpp_storage_parity"],
                "comparison_command": (
                    "python3 tools/compare_unified_cpp_rust_case_reports.py "
                    "--rust-report rust.json --cpp-report cpp.json "
                    "--require-schema temporalstore_unified_case_report_v1"
                ),
                "exit_criteria": [
                    "native C++ runner emits temporalstore_unified_case_report_v1",
                    "selected shared cases pass",
                    "no static surface gate remains",
                ],
            }
        ],
    }


class FeatureExecutionValidatorTest(unittest.TestCase):
    def test_static_family_must_be_named_in_matrix_blockers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix = root / "matrix.json"
            corpus = root / "corpus.json"
            _write_json(matrix, _static_matrix(["C++ shared-corpus execution still has temporary static surface gates."]))
            _write_json(corpus, _static_corpus())
            old_matrix, old_corpus = validator.MATRIX, validator.CORPUS
            try:
                validator.MATRIX, validator.CORPUS = matrix, corpus
                with self.assertRaises(SystemExit) as raised:
                    validator.main()
            finally:
                validator.MATRIX, validator.CORPUS = old_matrix, old_corpus

        self.assertIn("status.open_blockers must name static/mixed family `storage/cache`", str(raised.exception))

    def test_static_family_named_in_matrix_blockers_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix = root / "matrix.json"
            corpus = root / "corpus.json"
            _write_json(matrix, _static_matrix(["storage/cache native C++ runner is still pending"]))
            _write_json(corpus, _static_corpus())
            old_matrix, old_corpus = validator.MATRIX, validator.CORPUS
            try:
                validator.MATRIX, validator.CORPUS = matrix, corpus
                self.assertEqual(validator.main(), 0)
            finally:
                validator.MATRIX, validator.CORPUS = old_matrix, old_corpus

    def test_static_matrix_must_match_corpus_blocker_and_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            matrix = root / "matrix.json"
            corpus = root / "corpus.json"
            payload = _static_matrix(["storage/cache native C++ runner is still pending"])
            payload["rows"][0]["blocker"] = "short blocker"
            payload["rows"][0]["expected_runner_command"] = "python3 tools/run_temporalstore_unified_tests.py --family storage/cache --cpp --corpus {corpus} --require-cpp-native"
            _write_json(matrix, payload)
            _write_json(corpus, _static_corpus())
            old_matrix, old_corpus = validator.MATRIX, validator.CORPUS
            try:
                validator.MATRIX, validator.CORPUS = matrix, corpus
                with self.assertRaises(SystemExit) as raised:
                    validator.main()
            finally:
                validator.MATRIX, validator.CORPUS = old_matrix, old_corpus

        message = str(raised.exception)
        self.assertIn("storage/cache static gate blocker must match corpus coverage blocker", message)
        self.assertIn("storage/cache static gate expected_runner_command must match corpus coverage", message)


if __name__ == "__main__":
    unittest.main()
