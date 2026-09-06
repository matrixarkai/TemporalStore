#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""What a customer is told before changing the encoder matches what happens after.

Two texts warn about the same trap, at the two ends of one decision:

* ``ENCODER_CHANGE_NOTE`` in the settings registry, on ``embedding.provider`` and
  ``embedding.model`` -- "they are declined as a different model, so retrieval falls back to
  lexical and recency";
* ``_EMBEDDING_CHANGE_WARNING``, rendered on the Setup page beside the encoder picker.

They contradicted each other. The second ended "in which case nothing in the stack sees a mismatch
to complain about", which describes the world before the model-name guard. This deployment decides
conflicts on the **recorded model name**, not on width -- `context_embedding_model_conflicts` in the
engine and its mirror in `matrixark_mcp_core` both say so -- so a same-width swap is caught like any
other, the vectors are declined rather than mixed, and the retrieve path reports how many were not
searched.

"Nothing will notice" and "those memories stop being searched, and you will be told how many" are
different failures with different remedies. Only one of them is this deployment's.

The tests below check the warning against the CODE rather than against a copy of itself: a text
asserted to equal a string in the test file is a text nobody has checked.
"""
from __future__ import annotations

import io
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_v1_gateway as gw  # noqa: E402

ADAPTER = os.path.join(TOOLS, "matrixark_local_adapter_retrieve.py")
CORE = os.path.join(TOOLS, "matrixark_mcp_core.py")


def read(path: str) -> str:
    with io.open(path, encoding="utf-8") as handle:
        return handle.read()


class TheGuardTheWarningDescribesExistsTest(unittest.TestCase):
    """Each claim the warning makes, checked where the behaviour lives."""

    def test_conflicts_are_decided_on_the_model_name(self) -> None:
        core = read(CORE)
        self.assertIn("Width is not the signal", core,
                      "the guard no longer says it decides on the name; the warning claims it does")
        self.assertIn("recorded model name", core)

    def test_the_retrieve_path_counts_what_it_declined(self) -> None:
        """Anchored on the MODEL conflict specifically.

        The adapter says "not searched" twice -- once for a model clash and once for a width
        clash -- so asserting the phrase appears anywhere passed while the model half was
        reworded to say nothing. The block is located by its own counter first.
        """
        adapter = read(ADAPTER)
        self.assertIn("embedding_model_conflict_records", adapter)
        start = adapter.find("if embedding_model_conflict_records:")
        self.assertGreater(start, 0, "the model-conflict report is gone")
        # Bounded by the NEXT report, not by a character count. The width-conflict block sits
        # about nine lines below this one and also says "not searched", so a fixed window spans
        # both and finds the phrase in the wrong warning -- which it did, and the mutation that
        # reworded the model half survived.
        end = adapter.find("if embedding_width_conflict_records:", start)
        self.assertGreater(end, start, "the width-conflict report no longer follows this one")
        block = adapter[start:end]
        self.assertIn("not searched", block,
                      "the model-conflict report no longer says the memories were not searched, "
                      "so the warning's promise that they are reported is unfounded")
        self.assertIn("different model", block)

    def test_the_two_reports_are_told_apart(self) -> None:
        """The floor for the bound above: both blocks exist and are distinct, so slicing between
        them is meaningful rather than an empty range."""
        adapter = read(ADAPTER)
        model = adapter.find("if embedding_model_conflict_records:")
        width = adapter.find("if embedding_width_conflict_records:", model)
        self.assertGreater(model, 0)
        self.assertGreater(width, model)
        self.assertIn("different width", adapter[width:width + 400],
                      "the width report is not about width")

    def test_and_names_the_active_model_when_it_does(self) -> None:
        """The customer needs to know which model is in use now, not only that there is a clash."""
        self.assertIn("active_embedding_model", read(ADAPTER))


class TheWarningDoesNotContradictTheGuardTest(unittest.TestCase):

    WARNING = gw._EMBEDDING_CHANGE_WARNING

    def test_it_no_longer_says_nothing_complains(self) -> None:
        self.assertNotIn("nothing in the stack", self.WARNING,
                         "the warning describes the world before the model-name guard")

    def test_it_says_the_vectors_are_declined(self) -> None:
        self.assertIn("DECLINED", self.WARNING)

    def test_it_names_what_the_decision_is_made_on(self) -> None:
        self.assertIn("recorded model name", self.WARNING)
        self.assertIn("not on vector width", self.WARNING)

    def test_it_keeps_the_same_width_example(self) -> None:
        """The example is the reason the trap is not obvious, and dropping it would leave the
        reader thinking a width they can see is the thing that protects them.

        Both NAMES, not just the number: the sentence wraps across two source lines, so checking
        for "384" alone passed while the half naming the two encoders was deleted.
        """
        self.assertIn("all-MiniLM-L6-v2", self.WARNING)
        self.assertIn("BGE-M3", self.WARNING)
        self.assertIn("384", self.WARNING)

    def test_it_says_the_count_is_reported(self) -> None:
        self.assertIn("reports how many", self.WARNING)


class TheTwoTextsAgreeTest(unittest.TestCase):
    """One trap, two places a customer meets it. They must not say different things."""

    def test_both_say_the_vectors_are_declined(self) -> None:
        note = cfg.ENCODER_CHANGE_NOTE
        self.assertIn("declined", note.lower())
        self.assertIn("declined", gw._EMBEDDING_CHANGE_WARNING.lower())

    def test_both_say_retrieval_falls_back_rather_than_returning_noise(self) -> None:
        note = cfg.ENCODER_CHANGE_NOTE.lower()
        warning = gw._EMBEDDING_CHANGE_WARNING.lower()
        self.assertIn("lexical", note)
        self.assertIn("lexical", warning)

    def test_neither_promises_the_change_goes_unnoticed(self) -> None:
        for name, text in (("ENCODER_CHANGE_NOTE", cfg.ENCODER_CHANGE_NOTE),
                           ("_EMBEDDING_CHANGE_WARNING", gw._EMBEDDING_CHANGE_WARNING)):
            lowered = text.lower()
            for phrase in ("no error", "nothing sees", "nothing in the stack", "raise no error"):
                self.assertNotIn(phrase, lowered, "%s still promises silence" % name)

    def test_the_settings_that_change_the_encoder_carry_the_note(self) -> None:
        """Derived from the registry: whichever settings feed the encoder must carry it, so a new
        one cannot be added without the warning."""
        carrying = [s.key for s in cfg.SETTINGS
                    if cfg.ENCODER_CHANGE_NOTE.strip() in (s.help or "")]
        self.assertIn("embedding.provider", carrying)
        self.assertIn("embedding.model", carrying)


class ThePageShowsItTest(unittest.TestCase):

    def test_the_setup_page_renders_the_warning_it_is_sent(self) -> None:
        page = read(os.path.join(TOOLS, "portal", "setup_portal.html"))
        self.assertIn("change_warning", page,
                      "the endpoint sends it and the page never draws it")


if __name__ == "__main__":
    unittest.main()
