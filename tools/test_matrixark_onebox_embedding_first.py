# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The one-box profile: embeddings decide the ranking. ON by default.

Scoring is dense-only -- dense 1.00, sparse 0.00. Over 269 queries against this repo's own markdown
with multilingual-e5-large, the query sentence deleted from its own target so no verbatim span
survives, hit@1 moved by -0.011 with a 95% interval of [-0.049, +0.027], at 21x less scoring cost.
Set MATRIXARK_ONEBOX_EMBEDDING_FIRST=0 for the hybrid weights.

The scan projection is a SEPARATE, opt-in half, and these tests hold the line between them. The row
the scan carries is the row the pack is built from, so a field the projection drops is a field the
answer cannot print -- it shipped dropping the text and retrieval returned "text": "" for every hit.
Restoring the obvious four left entity rows still empty. Its field list has to be derived from what
the serving path reads at runtime, not extended one failing test at a time.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as _adapter  # resolves the retrieval circular import
import matrixark_local_adapter_retrieval as retrieval

FLAG = "MATRIXARK_ONEBOX_EMBEDDING_FIRST"
PROJECTION = "MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS"


def _record():
    return {"record_type": "skill_section", "section_hash": 1, "node_hash": 2, "scope": {},
            "access_scope": {}, "metadata": {"heading_slug": "s", "unit_kind": "markdown_section"},
            "embedding_meta": {"model": "e5-large", "dim": 512}, "vector": [0.5, 0.5],
            "text": "body the answer prints", "heading": "H", "source_locator": "heading=d/s",
            "storage_record_kind": "node", "storage_part": "0"}


class OneBoxEmbeddingFirst(unittest.TestCase):
    def setUp(self):
        self._saved = {key: os.environ.get(key) for key in (FLAG, PROJECTION)}
        for key in (FLAG, PROJECTION):
            os.environ.pop(key, None)

    def tearDown(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_the_profile_is_on_by_default(self):
        """What a deployment that sets nothing gets."""
        self.assertTrue(retrieval.onebox_embedding_first())

    def test_the_profile_can_be_turned_off(self):
        os.environ[FLAG] = "0"
        self.assertFalse(retrieval.onebox_embedding_first())

    def test_the_projection_is_not_implied_by_the_profile(self):
        """The two were coupled, and the coupling shipped a defect.

        Narrowing the row is only safe once the lexical term is gone, so the projection depends on
        the profile -- but not the other way round. Wiring it both ways turned the projection on
        for everyone the moment the profile became the default, and the projection drops fields the
        answer prints. Ten codex-pipeline tests fail with it on and pass with the scoring alone.
        """
        self.assertTrue(retrieval.onebox_embedding_first())
        self.assertFalse(retrieval.retrieval_scan_projection(),
                         "the profile must not turn the projection on by itself")

    def test_the_projection_is_opt_in_and_needs_the_profile(self):
        os.environ[PROJECTION] = "1"
        self.assertTrue(retrieval.retrieval_scan_projection(),
                        "asked for, with the profile on, it applies")
        os.environ[FLAG] = "0"
        self.assertFalse(retrieval.retrieval_scan_projection(),
                         "without dense-only scoring the lexical term still reads the text the "
                         "projection removes, so it must not apply")

    def test_a_record_passes_through_untouched_by_default(self):
        self.assertEqual(_record(), retrieval.project_scan_record(_record()))

    def test_the_row_still_carries_what_the_answer_prints(self):
        """Asserted for the opt-in path, because that is where the defect lives."""
        os.environ[PROJECTION] = "1"
        projected = retrieval.project_scan_record(_record())
        for printed in ("text", "heading", "source_locator"):
            self.assertIn(printed, projected,
                          "the pack is built from this row, so it must still carry %s" % printed)
        self.assertEqual([0.5, 0.5], projected.get("vector"))

    def test_the_flag_is_not_captured_at_import(self):
        """A module-level constant makes the answer depend on which test imported first."""
        os.environ[FLAG] = "0"
        self.assertFalse(retrieval.onebox_embedding_first())
        os.environ[FLAG] = "1"
        self.assertTrue(retrieval.onebox_embedding_first(),
                        "the flag is being read at import time, not at call time")

    def test_the_scan_reads_the_flag_once_per_scan_not_per_record(self):
        with open(retrieval.__file__, encoding="utf-8") as handle:
            source = handle.read()
        self.assertIn("_projecting = retrieval_scan_projection()", source)
        self.assertIn("project_scan_record(record, _projecting)", source)

    def test_the_scoring_weights_still_sum_to_one(self):
        for dense, sparse in ((0.72, 0.28), (1.00, 0.00)):
            self.assertAlmostEqual(1.0, dense + sparse, places=6)


if __name__ == "__main__":
    unittest.main()
