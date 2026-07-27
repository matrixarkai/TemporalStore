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

    def test_classic_childrens_books_support_dr_seuss_yes(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_7 summary",
                "body": "Caroline collects classic children's books and likes reading them on her bookshelf.",
            },
            {
                "title": "conv_26 session_6 turn 9",
                "body": "Caroline said she had no plans to become a writer.",
            },
        ]

        hint = extractive_reader_hint("Would Caroline likely have Dr. Seuss books on her bookshelf?", blocks)

        self.assertIn("Yes", hint)
        self.assertIn("classic children's books", hint)

    def test_multi_book_answer_keeps_title_case_variants(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_7 turn 8",
                "body": "Melanie mentioned reading nothing is impossible and Charlotte's Web with her kids.",
            },
        ]

        hint = extractive_reader_hint("What books has Melanie read?", blocks)

        self.assertIn("Nothing is Impossible", hint)
        self.assertIn("Charlotte's Web", hint)

    def test_destress_answer_extracts_activities(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_7 event Melanie 1",
                "body": "Melanie has been running more to destress and clear her mind.",
            },
            {
                "title": "conv_26 session_16 summary",
                "body": "Pottery classes are therapeutic for Melanie and help her unwind.",
            },
        ]

        hint = extractive_reader_hint("What does Melanie do to destress?", blocks)

        self.assertIn("running", hint.lower())
        self.assertIn("pottery", hint.lower())

    def test_recent_paint_question_accepts_paint_wording(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_13 summary",
                "body": "Melanie recently painted a sunset after returning from camping.",
            },
        ]

        hint = extractive_reader_hint("What did Melanie paint recently?", blocks)

        self.assertEqual("sunset", hint)


if __name__ == "__main__":
    unittest.main()
