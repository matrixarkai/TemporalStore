#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The three encoder controls say what changing them does to what is already stored.

Embeddings are keyed by a model-specific ref, and ``embedding_model_conflicts`` declines a stored
vector whose model differs from the active one. So changing the provider, the model name or the
model path on a populated store leaves every vector in it declined, and retrieval falls back to
lexical and recency. The engine's own MIGRATION note calls that *"a quiet degradation rather than an
error"* -- which is the whole reason it has to be said next to the control: nothing fails, nothing
is logged, and the portal reports the save as applied.

``embedding_model_conflicts`` is explicit that **width is not the signal**: two models truncated to
a common width look identical, so a swap raises no length mismatch anywhere. The recorded model name
is the only thing separating them, which is why the note promises what it promises.

The portal offers all three as **live** controls, and no help text in the registry mentioned any of
it. Searching every setting for "backfill", "existing vector", "re-embed" or "lexical" found none.

The tests below do not check prose against prose. They assert the *behaviour the note describes*:
that two encoder names produce different refs, and that a stored vector under one is declined under
the other. If that ever stops being true, these fail and the note gets revisited rather than
quietly becoming false -- which is the failure mode of every warning written once and left.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_mcp_core as core  # noqa: E402

# The controls that change which encoder makes a vector. There were three: `embedding.model_path`
# was a second field for the same value, read only where it OVERRODE the model name, so it is gone
# and the note it carried belongs to the field that absorbed it.
ENCODER_CONTROLS = ("embedding.provider", "embedding.model")


class TheControlsCarryTheNoteTest(unittest.TestCase):

    def test_the_controls_exist_and_are_live(self) -> None:
        """A live control is one a customer changes without an operator, which is exactly why the
        cost has to be written where they are looking."""
        for key in ENCODER_CONTROLS:
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)
                self.assertEqual("live", cfg.SETTINGS_BY_KEY[key].applies)

    def test_each_one_says_what_happens_to_what_is_already_stored(self) -> None:
        for key in ENCODER_CONTROLS:
            with self.subTest(setting=key):
                help_text = cfg.SETTINGS_BY_KEY[key].help
                self.assertIn("already holds documents", help_text)
                self.assertIn("declined as a different model", help_text)

    def test_each_one_names_the_remedy(self) -> None:
        """A warning a reader cannot act on is one they learn to scroll past."""
        for key in ENCODER_CONTROLS:
            with self.subTest(setting=key):
                self.assertIn("context_embed_backfill", cfg.SETTINGS_BY_KEY[key].help)

    def test_the_note_is_one_sentence_not_three(self) -> None:
        """Three copies is how two of them end up saying different things."""
        notes = {cfg.SETTINGS_BY_KEY[key].help[-len(cfg.ENCODER_CHANGE_NOTE):]
                 for key in ENCODER_CONTROLS}
        self.assertEqual(1, len(notes), notes)
        self.assertEqual({cfg.ENCODER_CHANGE_NOTE}, notes)

    def test_it_keeps_what_each_control_already_said(self) -> None:
        """The note is added to the help, not instead of it."""
        self.assertIn("hash vectors", cfg.SETTINGS_BY_KEY["embedding.provider"].help)
        self.assertIn("co-located encoder server",
                      cfg.SETTINGS_BY_KEY["embedding.model"].help)
        # What the retired path field used to say, now said by the field that took its place.
        self.assertIn("or a path to one you have downloaded",
                      cfg.SETTINGS_BY_KEY["embedding.model"].help)

    def test_settings_that_do_not_change_the_encoder_are_left_alone(self) -> None:
        """A note on every control is a note nobody reads."""
        for key, setting in cfg.SETTINGS_BY_KEY.items():
            if key in ENCODER_CONTROLS:
                continue
            with self.subTest(setting=key):
                self.assertNotIn("context_embed_backfill", setting.help or "")


class TheNoteDescribesWhatTheCodeDoesTest(unittest.TestCase):
    """Prose against behaviour, so the warning cannot quietly become false."""

    def test_two_encoders_key_their_vectors_differently(self) -> None:
        one = core.embedding_model_ref_for_name("sentence-transformers/all-MiniLM-L6-v2")
        other = core.embedding_model_ref_for_name("intfloat/multilingual-e5-large")
        self.assertNotEqual(one, other,
                            "the note says a changed encoder leaves the old vectors behind, and "
                            "they would be found under the same ref")

    def test_a_vector_from_another_encoder_is_declined(self) -> None:
        self.assertTrue(core.embedding_model_conflicts("sentence-transformers/all-MiniLM-L6-v2",
                                                 "intfloat/multilingual-e5-large"))

    def test_the_deterministic_provider_is_an_encoder_too(self) -> None:
        """Switching the PROVIDER changes the name as surely as switching the model, which is why
        that control carries the note as well."""
        self.assertTrue(core.embedding_model_conflicts("matrixark-local-token-hash-v1",
                                                 "intfloat/multilingual-e5-large"))

    def test_the_same_encoder_is_not_declined(self) -> None:
        """The floor. A guard that declined everything would make the note true and the product
        useless, and every test above would still pass."""
        self.assertFalse(core.embedding_model_conflicts("intfloat/multilingual-e5-large",
                                                  "intfloat/multilingual-e5-large"))
        self.assertFalse(core.embedding_model_conflicts("sentence-transformers/all-MiniLM-L6-v2",
                                                  "all-MiniLM-L6-v2"),
                         "a repository prefix is not an encoder change")

    def test_an_unknown_model_is_not_declined(self) -> None:
        """An older store wrote no model name. Declining those would take retrieval dark, which is
        the outcome the guard exists to prevent -- so the note must not promise it."""
        self.assertFalse(core.embedding_model_conflicts("", "intfloat/multilingual-e5-large"))


if __name__ == "__main__":
    unittest.main()
