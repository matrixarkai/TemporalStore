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

    def test_model_kit_question_keeps_distributed_named_kit_evidence(self) -> None:
        sources = [
            {
                "kind": "message",
                "title": f"conversation_73 filler {index}",
                "body": f"General modeling advice item {index} with numbered steps and model-building tips.",
            }
            for index in range(20)
        ]
        sources.extend(
            [
                {
                    "kind": "message",
                    "title": "conversation_73 model 1",
                    "body": "I finished a simple Revell F-15 Eagle kit from the hobby store.",
                },
                {
                    "kind": "message",
                    "title": "conversation_73 model 2",
                    "body": "I recently completed a Tamiya 1/48 scale Spitfire Mk.V model kit.",
                },
                {
                    "kind": "message",
                    "title": "conversation_73 model 3",
                    "body": "I started working on a diorama featuring a 1/16 scale German Tiger I tank.",
                },
                {
                    "kind": "message",
                    "title": "conversation_73 model 4",
                    "body": "I got a 1/72 scale B-29 bomber model kit and a 1/24 scale '69 Camaro.",
                },
            ]
        )

        selected = bench.rank_sources(
            "How many model kits have I worked on or bought?",
            sources,
            8,
            bench.RetrievalBudgetConfig(0.7, 0.45, 0.25, 0.35, 0.8),
        )
        hint = bench.extractive_reader_hint("How many model kits have I worked on or bought?", selected)

        self.assertEqual("5", hint)

    def test_longmemeval_aggregate_hints_prefer_exact_synthesis_over_decoy_numbers(self) -> None:
        movie_blocks = [
            {"body": "I watched all 22 Marvel Cinematic Universe movies in two weeks."},
            {"body": "I finished a Star Wars marathon, all the main films in a week and a half."},
            {"body": "Movie list item 5 is just a row number."},
        ]
        road_blocks = [
            {"body": "My recent trip to Outer Banks only took about four hours to drive there."},
            {"body": "The drive to Tybee Island from there is around 4-5 hours."},
            {"body": "I drove for six hours to Washington D.C. recently."},
            {"body": "Route option 5 is a numbered list entry."},
        ]
        japan_blocks = [
            {"body": "When I was in Japan a few months ago, I spent two weeks traveling solo around the country."},
            {"body": "I also took a 6-week course later, unrelated to the Japan trip."},
        ]

        self.assertEqual(
            "3.5 weeks",
            bench.extractive_reader_hint(
                "How many weeks did it take me to watch all the Marvel Cinematic Universe movies and the main Star Wars films?",
                movie_blocks,
            ),
        )
        self.assertEqual(
            "15 hours",
            bench.extractive_reader_hint(
                "How many hours in total did I spend driving to my three road trip destinations combined?",
                road_blocks,
            ),
        )
        self.assertEqual("2 weeks", bench.extractive_reader_hint("How long was I in Japan for?", japan_blocks))

    def test_longmemeval_inventory_totals_keep_distinct_duplicate_counts(self) -> None:
        blocks = [
            {
                "body": (
                    "User: I'm thinking of adding live plants to my new 20-gallon tank, "
                    "which currently has 10 neon tetras, 5 golden honey gouramis, "
                    "and a small pleco catfish."
                )
            },
            {
                "body": (
                    "User: I also upgraded my old 10-gallon tank, which has my "
                    "betta fish, Bubbles."
                )
            },
            {"body": "Assistant: Here are 50 generic aquarium tips and 20 plant ideas."},
        ]

        self.assertEqual(
            "17",
            bench.extractive_reader_hint("How many fish are there in total in both of my aquariums?", blocks),
        )

    def test_longmemeval_charity_total_uses_user_raised_amounts_only(self) -> None:
        blocks = [
            {"body": "User: I recently participated in a charity walk and managed to raise $250 through sponsors."},
            {"body": "User: I just helped organize a charity yoga event that raised $600 for a local animal shelter."},
            {"body": "User: I recently participated in a Bike-a-Thon for Cancer Research and my team managed to raise $5,000."},
            {"body": "Assistant: Here are 10 tips for charity events and 3 possible sponsors."},
        ]

        self.assertEqual(
            "$5,850",
            bench.extractive_reader_hint(
                "How much money did I raise in total through all the charity events I participated in?",
                blocks,
            ),
        )

    def test_longmemeval_absent_count_target_beats_distractor_numbers(self) -> None:
        blocks = [
            {"body": "User: I baked cookies twice and made 3 cakes for a party."},
            {"body": "Assistant: Use 150 grams of flour and 125 grams of sugar."},
        ]

        self.assertIn(
            "not enough",
            bench.extractive_reader_hint("How many times did I bake egg tarts in the past two weeks?", blocks).lower(),
        )

    def test_longmemeval_project_count_excludes_thesis(self) -> None:
        blocks = [
            {
                "body": (
                    "User: I've created separate boards for my thesis, Data Mining project, "
                    "and Database Systems project. It's been helpful while juggling multiple projects."
                )
            },
            {"body": "Assistant: Here are 3 Trello label ideas and 1 checklist template."},
        ]

        self.assertEqual(
            "2",
            bench.extractive_reader_hint(
                "How many projects have I been working on simultaneously, excluding my thesis?",
                blocks,
            ),
        )


if __name__ == "__main__":
    raise SystemExit(unittest.main())
