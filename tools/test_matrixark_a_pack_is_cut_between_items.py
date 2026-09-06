#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The injected context pack is cut between items, not mid-character.

`additional_context_from_retrieve` joined every retrieved item and sliced the result to
`max_chars`. The cut landed wherever the budget ran out. On a pack this hook actually injected it
landed mid-word: the model's last remembered turn arrived as

    "...upgrading still costs cold-load time, but tha"

A fragment that stops mid-clause is worse than not sending the item at all -- it reads as a
complete thought whose meaning changes at the cut, and the tokens are spent either way.

The budget itself is unchanged. This only moves where the cut falls.
"""
import unittest

try:
    from tools.matrixark_agent_hook import additional_context_from_retrieve
except ImportError:  # run from tools/
    from matrixark_agent_hook import additional_context_from_retrieve

HEADER = "Relevant context from earlier turns (MatrixArk memory):"


def _retrieve(*texts):
    return {"groups": [{"items": [{"text": t} for t in texts]}]}


class PackCutTests(unittest.TestCase):
    def test_no_item_is_cut_in_half(self):
        items = ["x" * 300 for _ in range(10)]
        # make each distinct so the flattener's dedupe keeps them all
        items = [item + str(i) for i, item in enumerate(items)]
        out = additional_context_from_retrieve(_retrieve(*items), max_chars=1000)
        self.assertLessEqual(len(out), 1000)
        for line in out.split("\n")[1:]:
            self.assertTrue(line.startswith("- "))
            body = line[2:]
            self.assertIn(body, items, "an item was truncated mid-way")

    def test_it_still_fills_the_budget(self):
        """Cutting between items must not become an excuse to send almost nothing."""
        items = [f"{i}" + "y" * 90 for i in range(40)]
        out = additional_context_from_retrieve(_retrieve(*items), max_chars=1000)
        self.assertGreater(len(out), 900, f"only used {len(out)} of 1000 chars")
        self.assertLessEqual(len(out), 1000)

    def test_the_budget_is_never_exceeded(self):
        for cap in (120, 300, 1000, 8000):
            items = [f"{i}" + "z" * 200 for i in range(60)]
            out = additional_context_from_retrieve(_retrieve(*items), max_chars=cap)
            self.assertLessEqual(len(out), cap, f"cap {cap} exceeded")

    def test_one_oversized_item_is_still_sent(self):
        """Returning "" here would read to the caller as 'nothing was retrieved'."""
        out = additional_context_from_retrieve(_retrieve("q" * 5000), max_chars=200)
        self.assertEqual(200, len(out))
        self.assertTrue(out.startswith(HEADER))

    def test_everything_fits_when_it_fits(self):
        out = additional_context_from_retrieve(_retrieve("alpha", "beta"), max_chars=8000)
        self.assertEqual(HEADER + "\n- alpha\n- beta", out)

    def test_nothing_retrieved_is_still_empty(self):
        """The fail-open contract: no items must mean "", not a bare header."""
        self.assertEqual("", additional_context_from_retrieve(_retrieve(), max_chars=8000))
        self.assertEqual("", additional_context_from_retrieve({}, max_chars=8000))
        self.assertEqual("", additional_context_from_retrieve(None, max_chars=8000))

    def test_duplicates_are_still_dropped(self):
        out = additional_context_from_retrieve(_retrieve("same", "same", "other"), max_chars=8000)
        self.assertEqual(HEADER + "\n- same\n- other", out)


if __name__ == "__main__":
    unittest.main()
