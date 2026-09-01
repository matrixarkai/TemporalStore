# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The retrieval scan can hold only the fields it reads, instead of whole records.

`retrieval_records` returns whole records, so every candidate held during a scan carries its text,
heading and locator even though the scan reads none of them. The field list here was produced by a
probe that recorded every key access during a real scan, not by inspection -- `text` is absent
because the scan never asks for it.

Measured on a 1 MB skill: 6.56 MB resident -> 4.54 MB, vector share 53.1% -> 76.6%.

DEFAULT OFF, and the default is the point. Downstream scoring in the resource and skill scans reads
`text` for its lexical, keyword and origin terms, so enabling this without hydrating candidates
first changes ranking. Over 269 queries on real prose, dropping the lexical term moved hit@1 by
-0.011 with a 95% interval of [-0.049, +0.027] -- indistinguishable -- at 21x less scoring time.
That is a ranking decision to take deliberately, not a side effect of a footprint change.
"""
import importlib
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import through the adapter: matrixark_local_adapter_retrieval and matrixark_mcp_local_adapter
# import each other, so reaching the retrieval module first raises a partially-initialised
# ImportError. The adapter resolves the cycle, after which the module is a normal import.
import matrixark_mcp_local_adapter as _adapter  # noqa: F401
import matrixark_local_adapter_retrieval as retrieval


def _record():
    return {
        "record_type": "skill_section",
        "section_hash": 42,
        "node_hash": 7,
        "node_path": ["a"],
        "scope": {},
        "access_scope": {},
        "metadata": {"heading_slug": "step-1", "unit_kind": "markdown_section"},
        "embedding_meta": {"model": "e5-large", "dim": 512},
        "vector": [0.5, 0.5],
        "text": "the body of the section, which the scan never reads",
        "heading": "Step 1",
        "source_locator": "heading=doc/step-1",
    }


class TheScanCanHoldOnlyWhatItReads(unittest.TestCase):
    def test_it_is_off_by_default(self):
        """Enabling it changes what downstream scoring can see, so it is opt-in."""
        os.environ.pop("MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS", None)
        reloaded = importlib.reload(retrieval)
        self.assertFalse(reloaded.RETRIEVAL_SCAN_PROJECTION)
        record = _record()
        self.assertEqual(record, reloaded.project_scan_record(record),
                         "with the flag off a record must pass through untouched")

    def test_enabled_it_drops_what_the_scan_never_reads(self):
        os.environ["MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS"] = "1"
        try:
            reloaded = importlib.reload(retrieval)
            projected = reloaded.project_scan_record(_record())
            for absent in ("text", "heading", "source_locator"):
                self.assertNotIn(absent, projected,
                                 "%s is only needed to RETURN a hit" % absent)
        finally:
            os.environ.pop("MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS", None)
            importlib.reload(retrieval)

    def test_enabled_it_keeps_everything_the_scan_reads(self):
        """The probe's field list, asserted rather than trusted."""
        os.environ["MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS"] = "1"
        try:
            reloaded = importlib.reload(retrieval)
            projected = reloaded.project_scan_record(_record())
            for kept in ("record_type", "section_hash", "node_hash", "node_path", "scope",
                         "access_scope", "metadata", "embedding_meta", "vector"):
                self.assertIn(kept, projected, "the scan reads %s" % kept)
        finally:
            os.environ.pop("MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS", None)
            importlib.reload(retrieval)

    def test_the_vector_survives_projection(self):
        """A projection that dropped the embedding would defeat its own purpose."""
        os.environ["MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS"] = "1"
        try:
            reloaded = importlib.reload(retrieval)
            self.assertEqual([0.5, 0.5], reloaded.project_scan_record(_record()).get("vector"))
        finally:
            os.environ.pop("MATRIXARK_RETRIEVAL_PROJECT_SCAN_FIELDS", None)
            importlib.reload(retrieval)


if __name__ == "__main__":
    unittest.main()
