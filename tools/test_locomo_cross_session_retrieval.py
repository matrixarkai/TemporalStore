#!/usr/bin/env python3
"""Regression tests for LoCoMo cross-session retrieval tuning."""

from __future__ import annotations

import unittest

import run_locomo_ingest_once as bench


class LocomoCrossSessionRetrievalTest(unittest.TestCase):
    def test_where_has_question_keeps_distributed_place_evidence(self) -> None:
        sources = [
            {
                "kind": "message",
                "title": f"conv_26 session_1 turn {index}",
                "body": f"D1:{index} Melanie says hello and talks about family work and school.",
            }
            for index in range(1, 18)
        ]
        sources.extend(
            [
                {
                    "kind": "message",
                    "title": "conv_26 session_4 turn 6",
                    "body": "D4:6 Melanie took her family camping in the mountains last week.",
                },
                {
                    "kind": "message",
                    "title": "conv_26 session_6 turn 16",
                    "body": "D6:16 Melanie shared a picture of her family camping at the beach.",
                },
                {
                    "kind": "message",
                    "title": "conv_26 session_8 turn 32",
                    "body": "D8:32 Melanie went on another camping trip in the forest.",
                },
            ]
        )

        selected = bench.rank_sources(
            "Where has Melanie camped?",
            sources,
            6,
            bench.RetrievalBudgetConfig(0.7, 0.45, 0.25, 0.35, 0.8),
        )
        bodies = "\n".join(source["body"].lower() for source in selected)

        self.assertIn("mountains", bodies)
        self.assertIn("beach", bodies)
        self.assertIn("forest", bodies)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
