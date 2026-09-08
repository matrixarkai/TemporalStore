#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A stored vector whose width cannot match its model name is called out.

Measured on the live store while writing this: **9,098 vectors carrying
``text-embedding-3-large`` at 32 dimensions.** That model is 3072 wide. 32 is the width of the
deterministic fallback, and every encoder in the catalogue is 384 or more -- so those vectors were
produced by the fallback and recorded under whatever name happened to be configured.

**Why it was invisible.** ``embedding_status`` computed each record's model and its width in the
same pass and then put them in two separate tallies -- one of names, one of widths. The store could
say "these models appear" and "these widths appear", and never "this model wrote this width", which
is the only form in which the contradiction exists.

The consequence is not cosmetic: two vector spaces sit in one store under one name, so nothing
downstream can separate them. The model hash cannot, because both were hashed from the same
configured string, and a backfill that trusted the name would skip exactly the records that need
redoing.

**This reports; it does not repair.** Rewriting the labels would make the store self-consistent and
still wrong. Recording the encoder that actually ran is a change to the write path with its own
migration.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from matrixark_mcp_embeddings import EMBEDDING_DIM  # noqa: E402

# The shape the live store is in, as measured.
LIVE = [{"model": "text-embedding-3-large", "dim": EMBEDDING_DIM, "count": 9098}]


class ANarrowVectorIsCalledOutTest(unittest.TestCase):

    def test_the_live_shape_is_reported(self) -> None:
        found = gw._impossible_model_widths(LIVE)
        self.assertEqual(1, len(found))
        self.assertEqual("text-embedding-3-large", found[0]["model"])
        self.assertEqual(9098, found[0]["count"])

    def test_it_says_what_is_wrong_and_why_it_matters(self) -> None:
        """A finding that names a row without saying what it costs gets read as a warning about
        tidiness. The cost is that the two spaces cannot be told apart afterwards."""
        detail = gw._impossible_model_widths(LIVE)[0]["detail"]
        self.assertIn("fallback", detail)
        self.assertIn("model hash", detail)

    def test_the_fallback_naming_itself_is_not_a_finding(self) -> None:
        """The honest case, and the common one on a build with no encoder configured. Reporting it
        would bury the real finding under a row that is behaving correctly."""
        honest = [{"model": "matrixark-local-token-hash-v1", "dim": EMBEDDING_DIM, "count": 8}]
        self.assertEqual([], gw._impossible_model_widths(honest))

    def test_a_real_encoder_at_its_real_width_is_not_a_finding(self) -> None:
        wide = [{"model": "intfloat/multilingual-e5-small", "dim": 384, "count": 500}]
        self.assertEqual([], gw._impossible_model_widths(wide))

    def test_it_is_keyed_on_the_encoders_own_width_not_a_literal(self) -> None:
        """The floor under the whole check. If the fallback's width ever changes and this still
        looked for 32, it would report nothing and keep passing."""
        at_width = [{"model": "some-api-model", "dim": EMBEDDING_DIM, "count": 1}]
        beside_it = [{"model": "some-api-model", "dim": EMBEDDING_DIM + 1, "count": 1}]
        self.assertEqual(1, len(gw._impossible_model_widths(at_width)))
        self.assertEqual([], gw._impossible_model_widths(beside_it))

    def test_nothing_stored_is_not_a_finding(self) -> None:
        self.assertEqual([], gw._impossible_model_widths([]))


class TheStoreCanAnswerTheQuestionTest(unittest.TestCase):
    """The pairing this rests on. Without it the check has nothing to read, and every assertion
    above would pass against a store that could not answer."""

    def test_embedding_status_reports_which_model_wrote_which_width(self) -> None:
        import io
        import re
        with io.open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "matrixark_local_adapter_dashboard.py"), encoding="utf-8") as h:
            source = h.read()
        body = source[source.index("def embedding_status"):]
        body = body[:body.index("\n    def ")]
        self.assertIn("model_dimensions", body,
                      "the store counts names and widths separately again, so the contradiction "
                      "this file reports cannot be seen")
        self.assertTrue(re.search(r"model_dimensions\[pair\]", body),
                        "model_dimensions is returned but nothing fills it")


if __name__ == "__main__":
    unittest.main()
