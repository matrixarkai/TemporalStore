#!/usr/bin/env python3
"""Unit tests for fair OSS model/encoding contract validation."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
VALIDATOR = REPO / "tools" / "validate_oss_model_contract.py"


class OssModelContractValidationTest(unittest.TestCase):
    def test_same_oss_reader_and_encoding_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json")
            baseline = write_report(tmp_path / "openviking.json")

            result = run_validator(matrixark, baseline)

            self.assertEqual(result.returncode, 0, result.stderr)

    def test_reader_model_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json")
            baseline = write_report(tmp_path / "openviking.json", reader_model="llama3.1:8b")

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("reader_model_mismatch", result.stderr)

    def test_encoding_alias_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json", encoding_model="nomic-embed-text")
            baseline = write_report(tmp_path / "openviking.json")

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("embedding_encoding_model_mismatch", result.stderr)

    def test_shared_hash_encoder_is_not_oss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(
                tmp_path / "matrixark.json",
                embedding_model="matrixark-hash-embedding-32",
            )
            baseline = write_report(
                tmp_path / "openviking.json",
                embedding_model="matrixark-hash-embedding-32",
            )

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("embedding_model_not_oss", result.stderr)
            self.assertIn("encoding_model_not_oss", result.stderr)

    def test_deterministic_reader_is_not_oss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json", reader_model="deterministic-reader")
            baseline = write_report(tmp_path / "openviking.json", reader_model="deterministic-reader")

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("reader_model_not_oss", result.stderr)

    def test_matching_proprietary_reader_is_not_oss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json", reader_model="gpt-4o-mini")
            baseline = write_report(tmp_path / "openviking.json", reader_model="gpt-4o-mini")

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("reader_model_not_oss", result.stderr)

    def test_reader_output_token_budget_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json")
            baseline = write_report(tmp_path / "openviking.json", reader_max_tokens=192)

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("reader_max_tokens_mismatch", result.stderr)

    def test_reader_fallback_policy_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json", reader_fallback_allowed=False)
            baseline = write_report(tmp_path / "openviking.json", reader_fallback_allowed=True)

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("reader_fallback_allowed_mismatch", result.stderr)

    def test_unforced_shared_stack_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            matrixark = write_report(tmp_path / "matrixark.json")
            baseline = write_report(tmp_path / "openviking.json", forced=False)

            result = run_validator(matrixark, baseline)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("shared_oss_models_forced_missing_or_false", result.stderr)


def run_validator(matrixark: Path, baseline: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(VALIDATOR),
            "--report",
            str(matrixark),
            "--label",
            "matrixark_locomo",
            "--report",
            str(baseline),
            "--label",
            "openviking_locomo",
            "--allow-diagnostic",
        ],
        cwd=REPO,
        text=True,
        capture_output=True,
    )


def write_report(
    path: Path,
    *,
    reader_model: str = "qwen2.5:7b",
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    encoding_model: str | None = None,
    reader_max_tokens: int = 128,
    reader_fallback_allowed: bool = False,
    forced: bool = True,
) -> Path:
    encoding = encoding_model or embedding_model
    matrixark_like = path.name.startswith("matrixark")
    prefix = "matrixark" if matrixark_like else "baseline"
    other_prefix = "baseline" if matrixark_like else "matrixark"
    contract = {
        f"{prefix}_provider_name": f"{prefix}-{reader_model}",
        f"{prefix}_reader_model": reader_model,
        f"{prefix}_embedding_model": embedding_model,
        f"{prefix}_encoding_model": encoding,
        f"{prefix}_max_events": 64,
        f"{prefix}_reader_max_context_chars": 4096,
        f"{prefix}_reader_max_tokens": reader_max_tokens,
        f"{prefix}_reader_fallback_allowed": reader_fallback_allowed,
        f"{prefix}_adaptive_max_events": False,
        f"{prefix}_adaptive_base_max_events": 0,
        f"{prefix}_retrieval_same_session_percent": 0.7,
        f"{prefix}_retrieval_cross_session_percent": 0.45,
        f"{prefix}_retrieval_summary_percent": 0.25,
        f"{prefix}_retrieval_entity_percent": 0.35,
        f"{prefix}_retrieval_event_percent": 0.8,
        f"{other_prefix}_provider_name": f"{other_prefix}-qwen2.5:7b",
        f"{other_prefix}_reader_model": "qwen2.5:7b",
        f"{other_prefix}_embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
        f"{other_prefix}_encoding_model": "sentence-transformers/all-MiniLM-L6-v2",
        f"{other_prefix}_max_events": 64,
        f"{other_prefix}_reader_max_context_chars": 4096,
        f"{other_prefix}_reader_max_tokens": 128,
        f"{other_prefix}_reader_fallback_allowed": False,
        f"{other_prefix}_adaptive_max_events": False,
        f"{other_prefix}_adaptive_base_max_events": 0,
        f"{other_prefix}_retrieval_same_session_percent": 0.7,
        f"{other_prefix}_retrieval_cross_session_percent": 0.45,
        f"{other_prefix}_retrieval_summary_percent": 0.25,
        f"{other_prefix}_retrieval_entity_percent": 0.35,
        f"{other_prefix}_retrieval_event_percent": 0.8,
        "shared_oss_model_contract_required": True,
        "shared_oss_model_contract_passed": True,
        "shared_oss_models_forced": forced,
        "same_oss_reader_model_forced": forced,
        "same_oss_encoding_model_forced": forced,
    }
    path.write_text(
        json.dumps(
            {
                "benchmark_model_contract": contract,
                "reader_open_source_calls": 1,
                "reader_fallback_count": 0,
                "reader_error_count": 0,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


if __name__ == "__main__":
    raise SystemExit(unittest.main())
