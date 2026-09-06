#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A ref header carrying only an index and a type does not earn a line of its own.

A retrieved ref was rendered as two lines:

     - [1] ref
       tool_evidence: tool_evidence = tool: Exit code: 0; Ran 159 tests

When the header has no citation, no score and no token count, the newline and the following indent
are spent on nothing. Measured on the packs cached on one box, **48 of 49 headers were bare**.

A header that DOES carry something keeps its own line: running a citation or a score together with
the text would bury it, and the point of the header is that those are visible at a glance.
"""
import re
import unittest

try:
    from tools.matrixark_codex_hook import additional_context_from_retrieve
except ImportError:  # run from tools/
    from matrixark_codex_hook import additional_context_from_retrieve


def _render(refs):
    pack = {"context_pack_id": "pack-1", "refs": refs,
            "groups": [{"items": refs}]}
    return additional_context_from_retrieve(pack, query="q", local_context_count=0)


def _ref_lines(out):
    return [l for l in out.split("\n") if l.startswith(" - [")]


class BareRefHeaderTests(unittest.TestCase):
    def test_a_bare_header_shares_its_line_with_the_text(self):
        out = _render([{"ref_type": "entity", "text": "a remembered thing"}])
        lines = _ref_lines(out)
        self.assertEqual(1, len(lines))
        self.assertIn("[1] entity", lines[0])
        self.assertIn("a remembered thing", lines[0])

    def test_the_text_is_not_lost(self):
        out = _render([{"ref_type": "entity", "text": "a remembered thing"}])
        self.assertIn("a remembered thing", out)

    def test_a_header_with_a_score_keeps_its_own_line(self):
        """The control: a header that carries something must not be run together with the text."""
        out = _render([{"ref_type": "entity", "text": "a remembered thing", "score": 0.91}])
        lines = _ref_lines(out)
        self.assertEqual(1, len(lines))
        self.assertIn("score=0.91", lines[0])
        self.assertNotIn("a remembered thing", lines[0],
                         "a header carrying a score should keep the text on its own line")
        self.assertIn("a remembered thing", out)

    def test_a_ref_with_no_text_still_renders_its_header(self):
        out = _render([{"ref_type": "entity"}])
        self.assertIn("[1] entity", out)

    def test_every_ref_is_still_numbered_in_order(self):
        out = _render([{"ref_type": "entity", "text": f"thing {i}"} for i in range(4)])
        numbers = [int(n) for n in re.findall(r"^ - \[(\d+)\]", out, flags=re.MULTILINE)]
        self.assertEqual([1, 2, 3, 4], numbers)

    def test_the_pack_gets_shorter(self):
        """The reason this exists, asserted rather than assumed."""
        refs = [{"ref_type": "entity", "text": f"a remembered thing number {i}"}
                for i in range(6)]
        out = _render(refs)
        # two lines per ref would cost at least an extra newline and three spaces each
        self.assertEqual(6, len(_ref_lines(out)))
        self.assertNotIn("\n   a remembered thing", out)


if __name__ == "__main__":
    unittest.main()
