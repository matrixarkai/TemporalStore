#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tools.run_matrixark_dataset_benchmark import (
    dataset_items,
    judge_answer,
    load_json_or_jsonl,
    read_answer,
    validate_dataset_shape,
)


class MatrixArkDatasetBenchmarkLoaderTest(unittest.TestCase):
    def test_locomo_wrapped_dataset_shape(self):
        raw = {
            "data": [
                {
                    "sample_id": "sample-a",
                    "conversation": {
                        "session_1_date_time": "2024-03-02",
                        "session_1": [
                            {"speaker": "Alice", "dia_id": "1", "text": "I moved to Seattle."},
                            {"speaker": "Bob", "dia_id": "2", "text": "Noted."},
                        ],
                    },
                    "qa": [
                        {
                            "question": "Where did Alice move?",
                            "answer": "Seattle",
                            "evidence": ["I moved to Seattle."],
                            "category": "temporal",
                        }
                    ],
                }
            ]
        }
        items = dataset_items(raw, "locomo")
        validation = validate_dataset_shape(items, "locomo", max_message_chars=1600)
        self.assertEqual(validation["status"], "ok")
        self.assertEqual(validation["items"], 1)
        self.assertEqual(validation["sessions"], 1)
        self.assertEqual(validation["turns"], 2)
        self.assertEqual(validation["questions"], 1)

    def test_longmemeval_jsonl_dataset_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "longmemeval.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "question_id": "q1",
                        "question": "What city did Alice move to?",
                        "answer": "Austin",
                        "answer_session_ids": ["s2"],
                        "haystack_session_ids": ["s1", "s2"],
                        "haystack_dates": ["2024-03-02", "2024-04-10"],
                        "haystack_sessions": [
                            [{"role": "user", "content": "Alice lived in Seattle."}],
                            [{"role": "assistant", "content": "Alice moved to Austin."}],
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            raw = load_json_or_jsonl(str(path))
        items = dataset_items(raw, "longmemeval_s")
        validation = validate_dataset_shape(items, "longmemeval_s", max_message_chars=1600)
        self.assertEqual(validation["status"], "ok")
        self.assertEqual(validation["sessions"], 2)
        self.assertEqual(validation["turns"], 2)
        self.assertEqual(validation["questions"], 1)

    def test_invalid_dataset_shape_reports_missing_sessions_and_questions(self):
        items = dataset_items({"items": [{"foo": "bar"}]}, "longmemeval_s")
        validation = validate_dataset_shape(items, "longmemeval_s", max_message_chars=1600)
        self.assertEqual(validation["status"], "invalid")
        self.assertEqual(validation["missing_session_rows"], 1)
        self.assertEqual(validation["missing_question_rows"], 1)

    def test_deterministic_reader_and_judge_work_without_api_key(self):
        args = SimpleNamespace(
            reader_provider="deterministic",
            reader_model="matrixark-context-substring-v1",
            judge_provider="deterministic",
            judge_model="matrixark-local-support-v1",
        )
        selected = [{"ref_type": "event", "ref_hash": 1, "text": "Alice moved to Austin.", "score": 1.0}]
        reader = read_answer(args, selected, "Where did Alice move?", "Austin", "fact")
        self.assertEqual(reader["reader_provider"], "deterministic")
        judge = judge_answer(
            args,
            question={"query": "Where did Alice move?", "answer": "Austin"},
            prediction=reader["prediction"],
            context="Alice moved to Austin.",
            support_score=reader["score"],
        )
        self.assertEqual(judge["judge_provider"], "deterministic")
        self.assertEqual(judge["score"], 1)

    def test_openai_compatible_reader_fails_fast_without_api_key(self):
        env_name = "MATRIXARK_TEST_MISSING_OPENAI_KEY"
        os.environ.pop(env_name, None)
        args = SimpleNamespace(
            reader_provider="openai-compatible",
            reader_model="gpt-4o-mini",
            openai_api_key_env=env_name,
            openai_base_url="https://api.openai.com/v1",
            openai_timeout_sec=1,
        )
        with self.assertRaises(SystemExit) as raised:
            read_answer(args, [], "Where did Alice move?", "Austin", "fact")
        self.assertIn(env_name, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
