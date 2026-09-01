#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Which engine answered, and whether it used the encoder.

One store, two implementations that rank differently: the Python adapter blends dense cosine with
lexical overlap, the native Rust pack orders by term overlap and reads no vector at all. Nothing in
the response said which one answered, so the portal described retrieval from the CONFIGURATION -- an
encoder is set, therefore semantic -- which is a different question from whether the path that
answered consults it.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_context_pack as packmod  # noqa: E402
import matrixark_mcp_server as mcp  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(HERE, "portal")


def _read(name: str) -> str:
    with open(os.path.join(PORTAL, name), encoding="utf-8") as handle:
        return handle.read()


def scope() -> dict:
    return {"account_id": "acct_local", "tenant_id": "sb", "user_id": "alice",
            "session_id": "s0", "agent_name": "sb"}


class ServedByOnTheWireTest(unittest.TestCase):
    """A real retrieve, through the adapter, says who answered."""

    @classmethod
    def setUpClass(cls) -> None:
        adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "sb.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        for index in range(6):
            server.call_tool("matrixark_ingest", {
                "scope": scope(), "finalize": True,
                "messages": [{"role": "user",
                              "content": "Note %d: the widget code is W%03d." % (index, index)}]})
        server.call_tool("matrixark_session_commit", {"scope": scope()})
        cls.pack = server.call_tool("matrixark_retrieve",
                                    {"scope": scope(), "query": "widget code"})

    def test_the_pack_names_the_implementation_that_answered(self) -> None:
        self.assertEqual("python_local_adapter", self.pack["served_by"]["assembly"])

    def test_it_says_how_it_ranked(self) -> None:
        self.assertIn("dense", self.pack["served_by"]["ranking"])
        self.assertTrue(self.pack["served_by"]["ranking_uses_vectors"])

    def test_it_is_reported_whether_or_not_anything_went_wrong(self) -> None:
        # "Why do these two deployments rank differently" is a question asked about working
        # systems, so this is not conditioned on a failure. (An ordinary retrieve does carry
        # warnings of its own -- pipeline stages still pending -- which is unrelated.)
        self.assertIn("served_by", self.pack)
        self.assertNotIn("embedding_conflicts", self.pack,
                         "nothing was declined here, so served_by is not riding on a conflict")


class ServedByCompactionTest(unittest.TestCase):
    """What survives the strip to the serving pack, and what is not invented."""

    def test_a_producer_that_states_its_ranking_is_passed_through(self) -> None:
        compact = packmod.compact_context_pack_for_serving({
            "context_pack_id": "p1", "groups": [],
            "served_by": {"assembly": "native_rust_proxy", "ranking": "lexical_term_overlap",
                          "ranking_uses_vectors": False}})
        self.assertEqual("native_rust_proxy", compact["served_by"]["assembly"])
        self.assertIs(False, compact["served_by"]["ranking_uses_vectors"])

    def test_a_native_pack_is_read_from_its_own_metrics(self) -> None:
        compact = packmod.compact_context_pack_for_serving({
            "context_pack_id": "p2", "groups": [],
            "context_pack_assembly": "native_rust_proxy",
            "retrieval_metrics": {"ranking": "lexical_term_overlap_and_boosts",
                                  "ranking_uses_vectors": False}})
        self.assertEqual("native_rust_proxy", compact["served_by"]["assembly"])
        self.assertEqual("lexical_term_overlap_and_boosts", compact["served_by"]["ranking"])
        self.assertIs(False, compact["served_by"]["ranking_uses_vectors"])

    def test_a_producer_that_says_nothing_is_not_guessed_at(self) -> None:
        # Silence about the ranking is not evidence of either answer. Reporting "uses vectors" for
        # a path that never said so would be the same mistake as reading it off the config.
        compact = packmod.compact_context_pack_for_serving({
            "context_pack_id": "p3", "groups": [],
            "context_pack_assembly": "some_future_backend"})
        self.assertEqual("some_future_backend", compact["served_by"]["assembly"])
        self.assertNotIn("ranking", compact["served_by"])
        self.assertNotIn("ranking_uses_vectors", compact["served_by"])

    def test_a_pack_with_no_provenance_at_all_carries_no_claim(self) -> None:
        compact = packmod.compact_context_pack_for_serving(
            {"context_pack_id": "p4", "groups": []})
        self.assertNotIn("served_by", compact)

    def test_false_survives_rather_than_being_dropped_as_empty(self) -> None:
        # `ranking_uses_vectors: False` is the whole point; a filter that drops falsey values would
        # delete exactly the case worth reporting.
        compact = packmod.compact_context_pack_for_serving({
            "context_pack_id": "p5", "groups": [],
            "served_by": {"assembly": "x", "ranking_uses_vectors": False}})
        self.assertIn("ranking_uses_vectors", compact["served_by"])


class AskShowsProvenanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.page = _read("explore_portal.html")

    def test_the_page_reports_who_answered(self) -> None:
        self.assertIn('id="askServed"', self.page)
        self.assertIn("Answered by", self.page)

    def test_it_says_plainly_when_an_answer_was_ranked_without_vectors(self) -> None:
        # The sentence that matters to someone who configured an encoder, watched the encoding
        # panel fill, and was warned about changing it.
        self.assertIn("ranked without using the embedding model", self.page)

    def test_the_warning_is_driven_by_the_backend_not_by_the_configuration(self) -> None:
        # Reading it off the configured provider is what made the old summary card wrong: whether
        # an encoder is SET and whether the answering path consults it are different questions.
        source = _read(os.path.join("build_portal_pages.py"))
        start = source.index("function renderAskWarnings(")
        block = source[start:source.index("function ask()", start)]
        self.assertIn("served.ranking_uses_vectors === false", block)
        self.assertNotIn("embedding.provider", block)


if __name__ == "__main__":
    unittest.main()
