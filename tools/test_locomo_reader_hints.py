#!/usr/bin/env python3
"""Regression tests for LoCoMo extractive reader hints."""

from __future__ import annotations

import unittest

from run_locomo_ingest_once import extractive_reader_hint


class LocomoReaderHintTest(unittest.TestCase):
    def test_yesterday_relative_date_beats_vague_ordering(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_1 observation Caroline 1",
                "body": (
                    "Observation: Caroline went to a LGBTQ support group last week. "
                    "The conversation timestamp was 1:56 pm on 8 May, 2023."
                ),
            },
            {
                "title": "conv_26 session_1 turn 3",
                "body": (
                    "turn 3: D1:3 Caroline: I went to a LGBTQ support group yesterday "
                    "and met welcoming people. The conversation timestamp was 1:56 pm on 8 May, 2023."
                ),
            }
        ]

        hint = extractive_reader_hint("When did Caroline go to the LGBTQ support group?", blocks)

        self.assertIn("7 May 2023", hint)
        self.assertNotIn("week before", hint.lower())

    def test_relevant_absolute_date_beats_unrelated_exact_relative_date(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_1 turn 3",
                "body": (
                    "turn 3: D1:3 Caroline: I went to a LGBTQ support group yesterday. "
                    "The conversation timestamp was 1:56 pm on 8 May, 2023."
                ),
            },
            {
                "title": "conv_26 session_1 turn 12",
                "body": "turn 12: D1:12 Melanie: I painted a sunrise in 2022.",
            },
        ]

        hint = extractive_reader_hint("When did Melanie paint a sunrise?", blocks)

        self.assertIn("2022", hint)
        self.assertNotIn("7 May 2023", hint)

    def test_last_year_answer_beats_same_sentence_timestamp(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_1 turn 14",
                "body": (
                    "1:56 pm on 8 May, 2023. D1:14 Melanie: "
                    "Yeah, I painted that lake sunrise last year! It's special to me."
                ),
            },
        ]

        hint = extractive_reader_hint("When did Melanie paint a sunrise?", blocks)

        self.assertIn("2022", hint)
        self.assertNotIn("8 May", hint)

    def test_raw_turn_date_beats_extracted_event_timestamp(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_1 event Caroline 1",
                "body": "event: Caroline went to a LGBTQ support group on 8 May, 2023.",
            },
            {
                "title": "conv_26 session_1 turn 3",
                "body": (
                    "D1:3 Caroline: I went to a LGBTQ support group yesterday "
                    "and met welcoming people. The conversation timestamp was 1:56 pm on 8 May, 2023."
                ),
            },
        ]

        hint = extractive_reader_hint("When did Caroline go to the LGBTQ support group?", blocks)

        self.assertIn("7 May 2023", hint)
        self.assertNotIn("8 May", hint)


if __name__ == "__main__":
    unittest.main()
