#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An encoder swap must be visible on the path that actually serves retrieval.

The engine declines a stored vector whose encoder is not the active one. The Python retrieve
adapter -- what the gateway, the SDK and mem0 run -- never read the model, though ingest stamps it
on every vector-bearing record. Two encoders of the same width produce no length mismatch and no
error, so nothing anywhere said the store had gone stale.

Every test here builds a store, then changes the active encoder, which is the sequence a customer
performs. Asserting on a store that never had a second encoder would pass with the guard removed.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_core as core
import matrixark_mcp_server as mcp  # noqa: F401  (imported for its side effect: see below)
# Imported through the server, which is the only import order that does not hit the adapter's
# circular import. The retrieve module holds its OWN binding of embedding_model_name -- patching
# the one in mcp_core leaves the caller untouched, which is how the first version of this file
# passed while measuring nothing.
import matrixark_local_adapter_retrieve as retrieve_mod


def scope():
    return {"account_id": "acct_local", "tenant_id": "enc", "user_id": "alice",
            "session_id": "s0", "agent_name": "enc"}


def build_store(turns=10):
    tmp = tempfile.mkdtemp()
    adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "enc.jsonl")
    server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
    for index in range(turns):
        server.call_tool("matrixark_ingest", {
            "scope": scope(), "finalize": True,
            "messages": [{"role": "user",
                          "content": "Note %d: the widget code is W%03d." % (index, index)}]})
    server.call_tool("matrixark_session_commit", {"scope": scope()})
    server.call_tool("matrixark_refresh_summaries", {"scope": scope(), "limit": 200})
    return adapter, server


def retrieve(server):
    return server.call_tool("matrixark_retrieve", {"scope": scope(), "query": "widget code"})


def declined(pack):
    """The decline counts as a caller receives them.

    Read from the served pack, not from an audit record: what a customer can act on is exactly what
    survives compaction, and an earlier version of this file asserted against a planner block the
    serving pack does not return -- so it measured a number nobody could ever see.
    """
    block = pack.get("embedding_conflicts")
    return block if isinstance(block, dict) else {}


def warnings_of(pack):
    return [str(w) for w in (pack.get("warnings") or [])]


class PredicateTest(unittest.TestCase):
    """`embedding_model_conflicts` must answer exactly as the engine's helper does."""

    def test_two_named_encoders_conflict(self) -> None:
        self.assertTrue(core.embedding_model_conflicts("bge-m3", "e5-large"))

    def test_the_same_encoder_never_conflicts(self) -> None:
        self.assertFalse(core.embedding_model_conflicts("e5-large", "e5-large"))

    def test_surrounding_whitespace_is_not_a_different_encoder(self) -> None:
        self.assertFalse(core.embedding_model_conflicts("  e5-large ", "e5-large"))

    def test_unknown_on_either_side_never_conflicts(self) -> None:
        # A stored blank predates the field; an active blank means nothing named an encoder.
        # Treating either as a conflict declines every vector in every older store.
        self.assertFalse(core.embedding_model_conflicts("", "e5-large"))
        self.assertFalse(core.embedding_model_conflicts("e5-large", ""))
        self.assertFalse(core.embedding_model_conflicts("", ""))
        self.assertFalse(core.embedding_model_conflicts("   ", "e5-large"))


class ModelNameTest(unittest.TestCase):
    """A repository prefix is not part of the model's identity.

    Both forms occur in real configuration -- the demo store was written under the short name while
    the catalogue offers the full one. Comparing exactly would decline every stored vector over a
    rename that changed nothing, which is the guard causing the outage it exists to prevent.
    """

    def test_a_repository_prefix_is_not_a_different_model(self) -> None:
        self.assertTrue(core.same_embedding_model(
            "sentence-transformers/all-MiniLM-L6-v2", "all-MiniLM-L6-v2"))
        self.assertFalse(core.embedding_model_conflicts(
            "paraphrase-multilingual-MiniLM-L12-v2",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"))
        self.assertFalse(core.embedding_model_conflicts(
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
            "paraphrase-multilingual-MiniLM-L12-v2"))

    def test_a_trailing_slash_is_not_a_different_model(self) -> None:
        self.assertTrue(core.same_embedding_model("intfloat/multilingual-e5-small/",
                                                  "intfloat/multilingual-e5-small"))

    def test_two_genuinely_different_models_still_conflict(self) -> None:
        # The loosening must not swallow the case the guard exists for.
        self.assertFalse(core.same_embedding_model(
            "intfloat/multilingual-e5-small",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"))
        self.assertTrue(core.embedding_model_conflicts(
            "intfloat/multilingual-e5-small",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"))
        # Same publisher, different model.
        self.assertTrue(core.embedding_model_conflicts(
            "intfloat/multilingual-e5-small", "intfloat/multilingual-e5-large"))

    def test_case_is_significant(self) -> None:
        # These are identifiers a loader resolves, not prose.
        self.assertFalse(core.same_embedding_model("BAAI/bge-m3", "baai/BGE-M3"))

    def test_an_empty_name_is_not_the_same_as_anything(self) -> None:
        self.assertFalse(core.same_embedding_model("", ""))
        self.assertFalse(core.same_embedding_model("", "bge-m3"))


class ServingPathTest(unittest.TestCase):
    def setUp(self) -> None:
        self._original = retrieve_mod.embedding_model_name
        self.adapter, self.server = build_store()

    def tearDown(self) -> None:
        retrieve_mod.embedding_model_name = self._original

    def swap_encoder(self, name: str) -> None:
        """Change the active encoder.

        In production this needs no patching at all -- embedding_model_name reads the environment
        on every call, so changing the setting changes the answer. Here the binding is replaced
        because the environment route would also change which encoder computes the QUERY vector,
        and then the two sides would differ for a second reason.
        """
        retrieve_mod.embedding_model_name = lambda: name

    def test_nothing_is_declined_while_the_encoder_is_unchanged(self) -> None:
        # The control. Without it, a guard that declined everything would pass the test below.
        counts = declined(retrieve(self.server))
        self.assertEqual(0, counts.get("encoder_change", 0))
        self.assertEqual(0, counts.get("vector_width", 0))

    def test_the_store_holds_vectors_to_decline(self) -> None:
        # The precondition the interesting tests rest on. A store with no vectors would report zero
        # declines with the guard working AND with it removed.
        status = self.server.call_tool("matrixark_embedding_status", {"scope": scope()})
        self.assertGreater(status.get("total", 0), 0)
        self.assertGreater(status.get("encoded", 0), 0,
                           "nothing is embedded, so a decline count of zero proves nothing")

    def test_the_store_still_answers_before_the_swap(self) -> None:
        pack = retrieve(self.server)
        items = [item for group in pack.get("groups") or [] for item in group.get("items") or []]
        self.assertGreater(len(items), 0, "the store answered nothing, so the comparison below "
                                          "would be between two empty results")

    def test_a_swapped_encoder_declines_the_stored_vectors_and_counts_them(self) -> None:
        self.swap_encoder("some-other-encoder")
        counts = declined(retrieve(self.server))
        self.assertGreater(
            counts.get("encoder_change", 0), 0,
            "the store was embedded by one encoder and queried with another, and every vector was "
            "scored anyway -- the two are the same width, so nothing else would have noticed")

    def test_a_swapped_encoder_is_reported_apart_from_a_width_change(self) -> None:
        # Two widths means a provider outage seeded fallback vectors; two encoders at one width
        # means the model was changed. Same symptom, different fix, so one count must not absorb
        # the other.
        self.swap_encoder("some-other-encoder")
        counts = declined(retrieve(self.server))
        self.assertEqual(0, counts.get("vector_width", 0),
                         "an encoder change was charged to the width counter")

    def test_the_active_encoder_is_reported_so_the_count_can_be_acted_on(self) -> None:
        # A count with nothing naming the encoder in force leaves an operator with a number and no
        # next step.
        self.swap_encoder("some-other-encoder")
        counts = declined(retrieve(self.server))
        self.assertEqual("some-other-encoder", counts.get("active_embedding_model"))

    def test_an_unnamed_active_encoder_declines_nothing(self) -> None:
        # Unknown is not foreign. If a deployment stops naming its encoder, retrieval must keep
        # working rather than going dark against its own store.
        self.swap_encoder("")
        counts = declined(retrieve(self.server))
        self.assertEqual(0, counts.get("encoder_change", 0))

    def test_the_reason_arrives_with_the_answer(self) -> None:
        # A pack that is thinner than it should be, with nothing saying why, reads as bad
        # retrieval -- and the fix for bad retrieval is not the fix for this.
        self.swap_encoder("some-other-encoder")
        text = " ".join(warnings_of(retrieve(self.server))).lower()
        self.assertIn("different model", text)
        self.assertIn("some-other-encoder", text)

    def test_no_warning_is_raised_while_the_encoder_is_unchanged(self) -> None:
        # A warning shown on every healthy retrieve is one nobody reads by the time it matters.
        text = " ".join(warnings_of(retrieve(self.server))).lower()
        self.assertNotIn("different model", text)

    def test_a_swapped_encoder_does_not_empty_the_pack(self) -> None:
        # The declined vectors fall back to the lexical path, exactly as un-embedded records do.
        # Retrieval gets worse; it must not stop.
        self.swap_encoder("some-other-encoder")
        pack = retrieve(self.server)
        items = [item for group in pack.get("groups") or [] for item in group.get("items") or []]
        self.assertGreater(len(items), 0,
                           "declining the vectors emptied the pack -- the lexical fallback is what "
                           "makes this a degradation rather than an outage")


class LivePathTest(unittest.TestCase):
    """Which retrieve path these tests are guarding, stated as an assertion.

    There are two implementations over one store. Against a TemporalStore backend the engine
    assembles the pack and Python packing is refused; against the local backend the Python scan IS
    retrieval. A guard built on the wrong assumption about which is live is a guard that never runs,
    and prose in a commit message cannot fail when the answer changes.
    """

    def test_the_local_backend_retrieves_through_the_python_scan(self) -> None:
        adapter, _server = build_store(turns=2)
        self.assertIsNone(
            adapter.native_context_pack({}),
            "the local adapter returned a native pack, so the guarded scan is not the live path "
            "here and these tests are measuring something nothing runs")
        self.assertFalse(
            adapter.native_context_pack_required(),
            "the local adapter demands a native pack it cannot produce")

    def test_the_guard_is_consulted_during_a_real_retrieve(self) -> None:
        # The direct evidence, rather than an argument about which branch is reachable: count the
        # calls during an actual retrieve. Zero would mean the code is dead however good it looks.
        import matrixark_local_adapter_retrieve as retrieve_mod

        _adapter, server = build_store()
        original = retrieve_mod.embedding_model_conflicts
        calls = []

        def counting(stored, active):
            calls.append((stored, active))
            return original(stored, active)

        retrieve_mod.embedding_model_conflicts = counting
        try:
            retrieve(server)
        finally:
            retrieve_mod.embedding_model_conflicts = original
        self.assertTrue(calls, "the encoder check was never reached during a retrieve")


class EncodingPanelTest(unittest.TestCase):
    """`embedding_status` is what the portal's encoding panel and the model picker read."""

    def test_a_store_written_by_current_ingest_is_not_reported_as_empty(self) -> None:
        # It counted only `context_embedding` rows, and the fold-and-drop retired those: the vector
        # now rides on its owner. So a store with a hundred vectors answered total 0 -- which the
        # panel renders as "nothing stored yet", and which the model picker turns into "nothing a
        # change could strand". Wrong in the reassuring direction about the one operation that
        # silently invalidates data.
        _adapter, server = build_store()
        status = server.call_tool("matrixark_embedding_status", {"scope": scope()})
        self.assertGreater(status.get("total", 0), 0)
        self.assertGreater(status.get("encoded", 0), 0)

    def test_it_names_the_encoder_the_vectors_were_made_with(self) -> None:
        # The picker's whole warning rests on this answer.
        _adapter, server = build_store()
        status = server.call_tool("matrixark_embedding_status", {"scope": scope()})
        names = [str(row.get("model") or "") for row in status.get("models") or []]
        self.assertTrue(any(names), "no encoder was named for a store full of vectors")

    def test_it_reports_the_width(self) -> None:
        _adapter, server = build_store()
        status = server.call_tool("matrixark_embedding_status", {"scope": scope()})
        dims = [int(row.get("dim") or 0) for row in status.get("dimensions") or []]
        self.assertTrue(any(dims), "no vector width was reported")

    def test_the_owner_types_it_counts_are_the_ones_retrieval_scores(self) -> None:
        # Two lists of record types, in two modules, that have to agree. A type in one and not the
        # other means the panel reports a store retrieval does not search, or the reverse -- and
        # either way a customer reasons about the wrong number.
        from matrixark_local_adapter_dashboard import EMBEDDING_OWNER_KEY_FIELDS
        from matrixark_local_adapter_retrieve import _EMBEDDING_OWNER_REFS
        self.assertEqual(_EMBEDDING_OWNER_REFS, EMBEDDING_OWNER_KEY_FIELDS)

    def test_a_vector_is_counted_once_when_both_the_row_and_its_owner_exist(self) -> None:
        # An older log can hold the separate row AND the owner it was folded into; they are the
        # same vector. Counting both would inflate every number on the panel.
        from matrixark_local_adapter_dashboard import embedding_owner_key
        owner = {"record_type": "context_event", "event_id_hash": 42, "vector": [0.1]}
        legacy = {"record_type": "context_embedding", "ref_type": "event", "ref_hash": 42,
                  "vector": [0.1]}
        self.assertEqual(embedding_owner_key(owner), embedding_owner_key(legacy))

    def test_a_record_with_no_vector_is_not_counted_as_a_backlog(self) -> None:
        from matrixark_local_adapter_dashboard import embedding_owner_key
        self.assertIsNone(embedding_owner_key(
            {"record_type": "context_event", "event_id_hash": 42}))
        self.assertIsNone(embedding_owner_key({"record_type": "context_index"}))


if __name__ == "__main__":
    unittest.main()
