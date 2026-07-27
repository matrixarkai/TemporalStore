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

    def test_who_when_question_does_not_return_date(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_19 observation Caroline 4",
                "body": (
                    "Caroline had a negative experience, but her friends, family, and mentors "
                    "supported her. The conversation timestamp was 3:19 pm on 17 August, 2023."
                ),
            },
        ]

        hint = extractive_reader_hint("Who supports Caroline when she has a negative experience?", blocks)

        self.assertIn("mentors", hint.lower())
        self.assertNotIn("August", hint)

    def test_pet_names_answer_uses_names_not_species(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_13 observation Melanie 1",
                "body": "Luna and Oliver are Melanie's pets, and they got another cat named Bailey too.",
            },
        ]

        hint = extractive_reader_hint("What are Melanie's pets' names?", blocks)

        self.assertIn("Oliver", hint)
        self.assertIn("Luna", hint)
        self.assertIn("Bailey", hint)

    def test_symbols_answer_includes_transgender_symbol(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_12 observation Caroline 3",
                "body": "The rainbow flag and transgender symbol are important symbols for Caroline.",
            },
        ]

        hint = extractive_reader_hint("What symbols are important to Caroline?", blocks)

        self.assertIn("Rainbow flag", hint)
        self.assertIn("transgender symbol", hint)

    def test_instrument_answer_extracts_instrument_names(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_15 observation Melanie 2",
                "body": "Melanie plays the clarinet as a way to relax and also plays violin.",
            },
        ]

        hint = extractive_reader_hint("What instruments does Melanie play?", blocks)

        self.assertIn("clarinet", hint.lower())
        self.assertIn("violin", hint.lower())

    def test_last_night_resolves_to_previous_day(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_11 turn 1",
                "body": (
                    "2:24 pm on 14 August, 2023. Melanie: Last night was amazing! "
                    "We celebrated my daughter's birthday with a concert."
                ),
            },
        ]

        hint = extractive_reader_hint("When is Melanie's daughter's birthday?", blocks)

        self.assertIn("13 August 2023", hint)
        self.assertNotIn("14 August", hint)

    def test_pride_festival_together_prefers_last_year(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_12 turn 15",
                "body": (
                    "20 July, 2023. Caroline: We had a blast last year at the Pride fest "
                    "with supportive friends."
                ),
            },
        ]

        hint = extractive_reader_hint("When did Caroline and Melanie go to a pride fesetival together?", blocks)

        self.assertEqual("2022", hint)

    def test_pottery_plate_uses_plate_event_anchor(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_14 turn 4",
                "body": (
                    "24 August 2023. Melanie made a plate in pottery class and loved "
                    "the result."
                ),
            },
            {
                "title": "conv_26 session_5 turn 4",
                "body": "2 July 2023. Melanie signed up for a pottery class.",
            },
        ]

        hint = extractive_reader_hint("When did Melanie make a plate in pottery class?", blocks)

        self.assertIn("24 August 2023", hint)

    def test_lgbtq_participation_uses_action_phrases(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_5 summary",
                "body": (
                    "Caroline joined an activist group, went to pride parades, "
                    "participated in an art show, and joined a mentorship program."
                ),
            },
        ]

        hint = extractive_reader_hint("In what ways is Caroline participating in the LGBTQ community?", blocks)

        self.assertIn("Joining activist group", hint)
        self.assertIn("going to pride parades", hint)
        self.assertIn("participating in an art show", hint)
        self.assertIn("mentoring program", hint)

    def test_adoption_plan_answer_is_compact(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_2 turn 8",
                "body": "Caroline is researching adoption agencies to give a loving home to kids who need it.",
            },
        ]

        hint = extractive_reader_hint("What are Caroline's plans for the summer?", blocks)

        self.assertEqual("researching adoption agencies", hint)

    def test_counseling_motivation_answer_is_exact(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_4 turn 15",
                "body": (
                    "Caroline said her own journey and the support she received, "
                    "and how counseling improved her life, motivated her to pursue counseling."
                ),
            },
        ]

        hint = extractive_reader_hint("What motivated Caroline to pursue counseling?", blocks)

        self.assertIn("own journey", hint)
        self.assertIn("support she received", hint)

    def test_camping_activities_answer_is_exact(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_4 turn 8",
                "body": "Melanie and her family explored nature, roasted marshmallows, and went on a hike while camping.",
            },
        ]

        hint = extractive_reader_hint("What did Melanie and her family do while camping?", blocks)

        self.assertIn("explored nature", hint)
        self.assertIn("roasted marshmallows", hint)
        self.assertIn("hike", hint)

    def test_self_care_realization_answer_is_not_date(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_2 turn 5",
                "body": "After the charity race, Melanie realized that self-care is important.",
            },
        ]

        hint = extractive_reader_hint("What did Melanie realize after the charity race?", blocks)

        self.assertEqual("self-care is important", hint)

    def test_black_and_white_bowl_yes_answer(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_5 turn 8",
                "body": "Melanie showed a photo of a black and white bowl she made in pottery class.",
            },
        ]

        hint = extractive_reader_hint("Did Melanie make the black and white bowl in the photo?", blocks)

        self.assertEqual("Yes", hint)

    def test_practicing_art_duration_maps_to_since_year(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_16 turn 8",
                "body": "Melanie said she has been practicing art for seven years.",
            },
        ]

        hint = extractive_reader_hint("How long has Melanie been practicing art?", blocks)

        self.assertEqual("Since 2016", hint)

    def test_caroline_library_books_are_not_melanie_titles(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_6 turn 9",
                "body": "Caroline has kids' books - classics, stories from different cultures, educational books in her library.",
            },
        ]

        hint = extractive_reader_hint("What kind of books does Caroline have in her library?", blocks)

        self.assertIn("kids' books", hint)
        self.assertIn("educational books", hint)

    def test_becoming_nicole_takeaway_answer(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_7 turn 11",
                "body": "Caroline took lessons on self-acceptance and finding support from the book Becoming Nicole.",
            },
        ]

        hint = extractive_reader_hint('What did Caroline take away from the book "Becoming Nicole"?', blocks)

        self.assertIn("self-acceptance", hint)
        self.assertIn("finding support", hint)

    def test_birthday_performer_answer(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_11 turn 3",
                "body": "Matt Patterson performed at the concert at Melanie's daughter's birthday.",
            },
        ]

        hint = extractive_reader_hint("Who performed at the concert at Melanie's daughter's birthday?", blocks)

        self.assertEqual("Matt Patterson", hint)

    def test_last_year_what_did_see_question_is_not_date(self) -> None:
        blocks = [
            {
                "title": "conv_26 session_10 turn 18",
                "body": "Melanie and her family saw the Perseid meteor shower during their camping trip last year.",
            },
        ]

        hint = extractive_reader_hint("What did Melanie and her family see during their camping trip last year?", blocks)

        self.assertEqual("Perseid meteor shower", hint)


if __name__ == "__main__":
    unittest.main()
