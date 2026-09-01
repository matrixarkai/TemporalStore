# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One flag for the one-box baseline: embeddings decide the ranking, the scan holds little else.

It turns on two things together because each is unsound alone. The scan projection removes the text
the 0.28 lexical term reads; dropping that term is what makes removing the text safe. Enabling
either half by itself is the bug this flag exists to prevent.

Measured on a skill ingest: 4.11 MB held -> 2.65 MB, vectors 50.1% -> 77.8%, no held row carrying
text. And on this repo's own markdown with real multilingual-e5-large, the query sentence deleted
from its own target so no verbatim span survives, over 269 queries: hit@1 -0.011 with a 95%
interval of [-0.049, +0.027] -- indistinguishable -- at 21x less scoring time.

DEFAULT OFF: the ranking difference is within measurement error, not proven absent.
"""
import importlib
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_mcp_local_adapter as _adapter  # resolves the retrieval circular import
import matrixark_local_adapter_retrieval as retrieval

FLAG = "MATRIXARK_ONEBOX_EMBEDDING_FIRST"
PROJECTION = "MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS"


def _reload(**env):
    for key in (FLAG, PROJECTION):
        os.environ.pop(key, None)
    for key, value in env.items():
        os.environ[key] = value
    return importlib.reload(retrieval)


def _record():
    return {"record_type": "skill_section", "section_hash": 1, "node_hash": 2, "scope": {},
            "access_scope": {}, "metadata": {"heading_slug": "s", "unit_kind": "markdown_section"},
            "embedding_meta": {"model": "e5-large", "dim": 512}, "vector": [0.5, 0.5],
            "text": "body the scan never reads", "heading": "H", "source_locator": "heading=d/s"}


class OneBoxEmbeddingFirst(unittest.TestCase):
    def tearDown(self):
        _reload()

    def test_it_is_off_by_default(self):
        mod = _reload()
        self.assertFalse(mod.ONEBOX_EMBEDDING_FIRST)
        self.assertFalse(mod.RETRIEVAL_SCAN_PROJECTION)
        self.assertEqual(_record(), mod.project_scan_record(_record()),
                         "with the flag off a record passes through untouched")

    def test_the_profile_turns_the_projection_on(self):
        mod = _reload(**{FLAG: "1"})
        self.assertTrue(mod.ONEBOX_EMBEDDING_FIRST)
        self.assertTrue(mod.RETRIEVAL_SCAN_PROJECTION,
                        "the profile must imply the projection; scoring drops the term that "
                        "reads the text it removes")
        projected = mod.project_scan_record(_record())
        for absent in ("text", "heading", "source_locator"):
            self.assertNotIn(absent, projected)
        self.assertEqual([0.5, 0.5], projected.get("vector"),
                         "a projection that dropped the embedding would defeat its purpose")

    def test_the_projection_can_still_be_enabled_on_its_own(self):
        """The older switch keeps working, for anyone already using it."""
        mod = _reload(**{PROJECTION: "1"})
        self.assertFalse(mod.ONEBOX_EMBEDDING_FIRST)
        self.assertTrue(mod.RETRIEVAL_SCAN_PROJECTION)

    def test_the_scoring_weights_still_sum_to_one(self):
        """Scores stay comparable across the two configurations."""
        for dense, sparse in ((0.72, 0.28), (1.00, 0.00)):
            self.assertAlmostEqual(1.0, dense + sparse, places=6)


if __name__ == "__main__":
    unittest.main()
