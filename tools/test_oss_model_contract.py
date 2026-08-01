#!/usr/bin/env python3
"""Regression tests for MatrixArk/OpenViking OSS benchmark model contracts."""

from __future__ import annotations

import unittest

from oss_model_contract import contract_from_values, validate_shared_oss_contract


class OssModelContractTest(unittest.TestCase):
    def test_matching_real_oss_models_pass(self) -> None:
        matrixark = contract_from_values(
            reader_model="qwen2.5:1.5b",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            max_events=40,
            reader_max_context_chars=5000,
        )
        baseline = contract_from_values(
            reader_model="qwen2.5:1.5b",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            max_events=40,
            reader_max_context_chars=5000,
        )

        passed, mismatches = validate_shared_oss_contract(matrixark, baseline)

        self.assertTrue(passed, mismatches)
        self.assertEqual([], mismatches)

    def test_shared_hash_encoder_is_not_oss(self) -> None:
        matrixark = contract_from_values(
            reader_model="qwen2.5:1.5b",
            embedding_model="matrixark-hash-embedding-32",
            max_events=40,
            reader_max_context_chars=5000,
        )
        baseline = contract_from_values(
            reader_model="qwen2.5:1.5b",
            embedding_model="matrixark-hash-embedding-32",
            max_events=40,
            reader_max_context_chars=5000,
        )

        passed, mismatches = validate_shared_oss_contract(matrixark, baseline)

        self.assertFalse(passed)
        self.assertIn("embedding_model:matrixark=not_oss:'matrixark-hash-embedding-32'", mismatches)
        self.assertIn("embedding_model:baseline=not_oss:'matrixark-hash-embedding-32'", mismatches)

    def test_missing_model_is_not_oss(self) -> None:
        matrixark = contract_from_values(
            reader_model="",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            max_events=40,
            reader_max_context_chars=5000,
        )
        baseline = contract_from_values(
            reader_model="",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            max_events=40,
            reader_max_context_chars=5000,
        )

        passed, mismatches = validate_shared_oss_contract(matrixark, baseline)

        self.assertFalse(passed)
        self.assertIn("reader_model:matrixark=missing", mismatches)
        self.assertIn("reader_model:baseline=missing", mismatches)

    def test_matching_proprietary_reader_is_not_oss(self) -> None:
        matrixark = contract_from_values(
            reader_model="gpt-4o-mini",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            max_events=40,
            reader_max_context_chars=5000,
        )
        baseline = contract_from_values(
            reader_model="gpt-4o-mini",
            embedding_model="sentence-transformers/all-MiniLM-L6-v2",
            max_events=40,
            reader_max_context_chars=5000,
        )

        passed, mismatches = validate_shared_oss_contract(matrixark, baseline)

        self.assertFalse(passed)
        self.assertIn("reader_model:matrixark=not_oss:'gpt-4o-mini'", mismatches)
        self.assertIn("reader_model:baseline=not_oss:'gpt-4o-mini'", mismatches)


if __name__ == "__main__":
    unittest.main()
