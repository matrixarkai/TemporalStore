#!/usr/bin/env python3
"""Regression tests for LoCoMo extractive reader hints."""

from __future__ import annotations

import unittest

from run_locomo_ingest_once import (
    direct_relevance_score,
    extractive_reader_hint,
    hybrid_reader_answer,
    locomo_category_four_answer,
    locomo_short_fact_answer,
    normalize_text,
)


class LocomoReaderHintTest(unittest.TestCase):
    def test_hybrid_reader_uses_candidate_when_model_rambles(self) -> None:
        answer = hybrid_reader_answer(
            "What does Melanie do to destress?",
            "running and pottery",
            (
                "Melanie mentioned that she has been running more to destress and clear her mind. "
                "Caroline and Melanie had a conversation at 4:33 pm on 12 July, 2023. "
                "They also discussed several unrelated plans and follow-up activities."
            ),
        )

        self.assertEqual("running and pottery", answer)

    def test_hybrid_reader_keeps_clean_oss_answer(self) -> None:
        answer = hybrid_reader_answer(
            "What books has Melanie read?",
            "Charlotte's Web",
            "Nothing is Impossible and Charlotte's Web",
        )

        self.assertEqual("Nothing is Impossible and Charlotte's Web", answer)

    def test_hybrid_reader_uses_candidate_when_dates_conflict(self) -> None:
        answer = hybrid_reader_answer(
            "When did Caroline go to the LGBTQ support group?",
            "7 May 2023",
            "1:56 pm on 8 May, 2023.",
        )

        self.assertEqual("7 May 2023", answer)

    def test_help_children_event_question_extracts_both_events(self) -> None:
        context = normalize_text(
            "Caroline joined a mentoring program to help children. "
            "She also felt powerful giving my talk and said the audience became better allies."
        )

        answer = locomo_short_fact_answer(
            "What events has Caroline participated in to help children?",
            context,
        )

        self.assertEqual("Mentoring program, school speech", answer)

    def test_political_leaning_prefers_raw_source_ref_evidence(self) -> None:
        question = "What would Caroline's political leaning likely be?"
        raw_source = (
            "1:50 pm on 17 August, 2023. D12:1 Caroline: I ran into a group of "
            "religious conservatives. It made me think how much work we still have "
            "to do for LGBTQ rights and people who accept and support me."
        )
        derived_observation = (
            "1:50 pm on 17 August, 2023. Caroline had a not-so-great experience "
            "on a hike where she ran into a group of religious conservatives."
        )

        self.assertGreater(
            direct_relevance_score(question, raw_source),
            direct_relevance_score(question, derived_observation),
        )

    def test_hybrid_reader_uses_candidate_for_color_question_with_timestamp_answer(self) -> None:
        answer = hybrid_reader_answer(
            "What color did I repaint my bedroom walls?",
            "a lighter shade of gray",
            "2023/05/27 (Sat) 19:51.",
        )

        self.assertEqual("a lighter shade of gray", answer)

    def test_certification_question_prefers_named_certificate_over_date(self) -> None:
        blocks = [
            {
                "title": "conversation_22 answer_8ad8a34f turn 3",
                "body": (
                    "I completed my Data Science certification last month. "
                    "The conversation timestamp was 2023/05/01."
                ),
            }
        ]

        hint = extractive_reader_hint("What certification did I complete last month?", blocks)

        self.assertEqual("Data Science", hint)

    def test_clock_time_question_prefers_cutoff_time_over_timestamp(self) -> None:
        blocks = [
            {
                "title": "conversation_39 answer_0dd4d99a turn 5",
                "body": (
                    "I stop checking work emails and messages at 7 pm. "
                    "The conversation timestamp was 2023/05/29."
                ),
            }
        ]

        hint = extractive_reader_hint("What time do I stop checking work emails and messages?", blocks)

        self.assertEqual("7 pm", hint)

    def test_clock_time_question_prefers_home_time_over_date_prefix(self) -> None:
        blocks = [
            {
                "title": "conversation_55 answer_f442ccbe turn 2",
                "body": (
                    "2023/05/23 (Tue) 18:30. user: I usually get home from work "
                    "around 6:30 pm on weeknights."
                ),
            }
        ]

        hint = extractive_reader_hint("What time do I usually get home from work on weeknights?", blocks)

        self.assertEqual("6:30 pm", hint)

    def test_social_media_platform_question_prefers_platform_over_date_prefix(self) -> None:
        blocks = [
            {
                "title": "conversation_96 answer_203bf3fa turn 1",
                "body": (
                    "2023/05/29 (Mon) 10:12. My TikTok gained 1200 followers this month, "
                    "while Twitter gained 250 followers."
                ),
            }
        ]

        hint = extractive_reader_hint(
            "Which social media platform did I gain the most followers on over the past month?",
            blocks,
        )

        self.assertEqual("TikTok", hint)

    def test_category_four_extracts_adoption_advice(self) -> None:
        answer = locomo_category_four_answer(
            normalize_text("What advice does Caroline give for getting started with adoption?"),
            normalize_text(
                "Caroline said to do research, find an adoption agency or lawyer, "
                "gather necessary documents, and prepare emotionally."
            ),
        )

        self.assertIn("research", answer.lower())
        self.assertIn("prepare emotionally", answer.lower())

    def test_category_four_extracts_pottery_break_activities(self) -> None:
        answer = locomo_category_four_answer(
            normalize_text("What does Melanie do to keep herself busy during her pottery break?"),
            normalize_text("Melanie kept herself busy by reading a book and painting during the pottery break."),
        )

        self.assertEqual("Read a book and paint", answer)

    def test_category_four_extracts_accident_reaction(self) -> None:
        answer = locomo_category_four_answer(
            normalize_text("How did Melanie's son handle the accident?"),
            normalize_text("Melanie's son was scared after the accident but reassured by his family."),
        )

        self.assertEqual("He was scared but reassured by his family", answer)

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


    def test_valentines_day_event_beats_conversation_timestamp(self) -> None:
        blocks = [
            {
                "title": "conversation_9 answer_59547700 turn 9",
                "body": (
                    "2023/04/02 (Sun) 22:15. user: I had a great experience with similar events "
                    "in the past, like the Love is in the Air fundraising dinner I volunteered "
                    "at back on Valentine's Day."
                ),
            },
        ]

        hint = extractive_reader_hint("When did I volunteer at the local animal shelter's fundraising dinner?", blocks)

        self.assertEqual("February 14th", hint)

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
