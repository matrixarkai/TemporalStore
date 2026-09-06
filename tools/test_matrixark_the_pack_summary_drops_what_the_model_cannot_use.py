#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The retrieval summary states only what the model can act on.

Every codex pack carried:

    Retrieval summary: context_pack_id=<id>, selected_refs=3, used_context_tokens=35,
    local_context_refs_seen=0.

`selected_refs` restates the length of the list printed directly below it, and
`used_context_tokens` is the hook's own accounting. Neither is something the model can do anything
with, and both cost it context on every turn -- measured across the packs cached on one box, this
line was **17.1% of every byte a codex pack carried**.

Two fields stay, and the tests below are mostly about keeping them:

* `context_pack_id` is the only handle correlating a pack with the logs, and an operator reading the
  pack cache has nothing else to key on.
* `local_context_refs_seen` tells the model whether local context was considered at all, which
  changes how it should weigh what follows.
"""
import re
import unittest

try:
    from tools.matrixark_codex_hook import additional_context_from_retrieve
except ImportError:  # run from tools/
    from matrixark_codex_hook import additional_context_from_retrieve


def _pack(n=3):
    return {
        "context_pack_id": "3459828568664744037",
        "used_context_tokens": 35,
        "groups": [{"items": [{"text": f"remembered thing {i}", "ref_type": "entity"}
                              for i in range(n)]}],
        "refs": [{"ref_type": "entity", "text": f"remembered thing {i}"} for i in range(n)],
    }


class PackSummaryTests(unittest.TestCase):
    def _summary(self, pack):
        out = additional_context_from_retrieve(
            pack, query="what did we decide", local_context_count=0)
        for line in out.split("\n"):
            if line.startswith("Retrieval summary:"):
                return line
        return ""

    def test_the_correlation_id_survives(self):
        """Without this the pack cannot be tied back to a log line."""
        self.assertIn("context_pack_id=", self._summary(_pack()))

    def test_the_local_context_signal_survives(self):
        self.assertIn("local_context_refs_seen=", self._summary(_pack()))

    def test_the_countable_field_is_gone(self):
        """selected_refs restated the length of the list printed below it."""
        self.assertNotIn("selected_refs=", self._summary(_pack()))

    def test_the_internal_accounting_stays(self):
        """An existing test pins this rendering as proof a nested pack wrapper was parsed.

        test_retrieve_budget_summary_reads_nested_context_pack_wrapper asserts
        "used_context_tokens=42" appears in the injected context. Dropping the field would have
        meant weakening someone else's assertion to save a few characters.
        """
        self.assertIn("used_context_tokens=", self._summary(_pack()))

    def test_the_count_is_still_derivable_from_the_pack(self):
        """Positive control: dropping the number is only safe because the list is still there.

        If the refs ever stopped being listed, removing `selected_refs` would have destroyed
        information rather than restated it.
        """
        out = additional_context_from_retrieve(
            _pack(n=3), query="what did we decide", local_context_count=0)
        listed = len(re.findall(r"^ - \[\d+\]", out, flags=re.MULTILINE))
        self.assertEqual(3, listed, "the refs are no longer listed, so the count is not derivable")

    def test_an_empty_pack_is_unchanged(self):
        out = additional_context_from_retrieve(
            {}, query="anything", local_context_count=0)
        self.assertNotIn("selected_refs=", out)


if __name__ == "__main__":
    unittest.main()
