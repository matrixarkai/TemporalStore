#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.run_matrixark_dataset_benchmark import (
    dataset_items,
    load_json_or_jsonl,
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


if __name__ == "__main__":
    unittest.main()
