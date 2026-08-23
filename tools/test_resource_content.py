#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Reading one resource's / skill's stored text back, in order and a page at a time.

`list_skills` and `list_resources` return a POINTER — raw_uri, cloud_key — and metadata. The text
is already stored, split across `resource_chunk` records at ingest, but nothing reassembled it, so
"give me this skill's content" had no answer.

Two things are worth pinning. Ordering: content stitched in the wrong order is worse than no
content, and it is silent. And paging: an attachment can be far larger than belongs in one JSON
response, so a caller must be able to take it a page at a time and be told there is more.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_local_adapter as local_mod
except ImportError:  # run from tools/ dir
    import matrixark_mcp_local_adapter as local_mod

SKILL = 4242


def _chunk(text, index=None, chunk_hash=None, resource_hash=SKILL):
    record = {
        "record_type": "resource_chunk",
        "resource_hash": resource_hash,
        "chunk_hash": chunk_hash if chunk_hash is not None else abs(hash(text)) % 10**9,
        "source_ref": "doc#%s" % (index if index is not None else "?"),
        "token_estimate": len(text) // 4,
        "text": text,
    }
    if index is not None:
        record["chunk_index"] = index
    return record


def _manifest(name="Cold start runbook"):
    return {
        "record_type": "skill_manifest",
        "skill_hash": SKILL,
        "name": name,
        "description": "How to bring payments up from cold.",
        "raw_uri": "file:///skills/cold_start.md",
    }


def _adapter(records):
    adapter = object.__new__(local_mod.MatrixArkLocalAdapter)
    adapter.read_all = lambda: list(records)  # type: ignore[assignment]
    return adapter


class ResourceContentTest(unittest.TestCase):
    def test_reassembles_in_chunk_index_order_not_log_order(self) -> None:
        """The whole point of the index: log order must not decide the content."""
        records = [_manifest(),
                   _chunk("third. ", index=2),
                   _chunk("first. ", index=0),
                   _chunk("second. ", index=1)]
        out = _adapter(records).get_resource_content({"skill_hash": SKILL})
        self.assertEqual("first. second. third. ", out["text"])
        self.assertEqual([0, 1, 2], [c["chunk_index"] for c in out["chunks"]])

    def test_falls_back_to_log_order_for_chunks_written_before_the_index(self) -> None:
        records = [_manifest(), _chunk("alpha "), _chunk("beta "), _chunk("gamma ")]
        out = _adapter(records).get_resource_content({"skill_hash": SKILL})
        self.assertEqual("alpha beta gamma ", out["text"])

    def test_carries_the_manifest_metadata(self) -> None:
        out = _adapter([_manifest(), _chunk("x", index=0)]).get_resource_content({"skill_hash": SKILL})
        self.assertEqual("Cold start runbook", out["name"])
        self.assertEqual("file:///skills/cold_start.md", out["raw_uri"])

    def test_ignores_other_resources(self) -> None:
        records = [_manifest(),
                   _chunk("mine ", index=0),
                   _chunk("theirs ", index=0, resource_hash=999)]
        out = _adapter(records).get_resource_content({"skill_hash": SKILL})
        self.assertEqual("mine ", out["text"])
        self.assertEqual(1, out["chunk_count"])

    def test_a_reingested_chunk_takes_the_later_text(self) -> None:
        records = [_manifest(),
                   _chunk("stale", index=0, chunk_hash=7),
                   _chunk("fresh", index=0, chunk_hash=7)]
        out = _adapter(records).get_resource_content({"skill_hash": SKILL})
        self.assertEqual("fresh", out["text"])
        self.assertEqual(1, out["chunk_count"])

    # ---- paging -------------------------------------------------------------------------

    def test_pages_by_chunk_limit_and_reports_more(self) -> None:
        records = [_manifest()] + [_chunk("c%d " % i, index=i) for i in range(10)]
        adapter = _adapter(records)
        first = adapter.get_resource_content({"skill_hash": SKILL, "chunk_limit": 4})
        self.assertEqual(4, first["returned_chunks"])
        self.assertEqual(10, first["chunk_count"])
        self.assertTrue(first["has_more"])
        self.assertEqual(4, first["next_chunk_offset"])
        self.assertEqual("c0 c1 c2 c3 ", first["text"])

    def test_a_caller_can_walk_every_page_and_rebuild_the_whole_thing(self) -> None:
        records = [_manifest()] + [_chunk("c%d " % i, index=i) for i in range(10)]
        adapter = _adapter(records)
        text, offset, pages = "", 0, 0
        while offset is not None and pages < 20:
            page = adapter.get_resource_content(
                {"skill_hash": SKILL, "chunk_limit": 3, "chunk_offset": offset})
            text += page["text"]
            offset = page["next_chunk_offset"]
            pages += 1
        self.assertEqual("".join("c%d " % i for i in range(10)), text)
        self.assertEqual(4, pages)

    def test_the_last_page_says_there_is_no_more(self) -> None:
        records = [_manifest()] + [_chunk("c%d " % i, index=i) for i in range(4)]
        out = _adapter(records).get_resource_content({"skill_hash": SKILL, "chunk_limit": 4})
        self.assertFalse(out["has_more"])
        self.assertIsNone(out["next_chunk_offset"])

    def test_max_chars_truncates_and_says_so(self) -> None:
        """A caller asking for a bounded response must not get an unbounded one.

        Distinct chunk_hash per chunk on purpose: identical text would share a hash and be
        de-duplicated down to one chunk, which is correct behaviour but tests nothing here.
        """
        records = [_manifest()] + [_chunk("x" * 100, index=i, chunk_hash=i) for i in range(5)]
        out = _adapter(records).get_resource_content({"skill_hash": SKILL, "max_chars": 250})
        self.assertLessEqual(out["chars"], 250)
        self.assertTrue(out["truncated_by_max_chars"])
        self.assertTrue(out["has_more"])

    # ---- input ---------------------------------------------------------------------------

    def test_resource_hash_and_skill_hash_are_aliases(self) -> None:
        records = [_manifest(), _chunk("v", index=0)]
        a = _adapter(records).get_resource_content({"skill_hash": SKILL})
        b = _adapter(records).get_resource_content({"resource_hash": SKILL})
        self.assertEqual(a["text"], b["text"])

    def test_a_missing_or_bad_id_is_rejected(self) -> None:
        adapter = _adapter([_manifest()])
        for bad in ({}, {"skill_hash": 0}, {"skill_hash": -1}, {"resource_hash": "abc"}):
            with self.assertRaises(local_mod.MatrixArkError):
                adapter.get_resource_content(bad)

    def test_an_unknown_resource_returns_empty_rather_than_failing(self) -> None:
        out = _adapter([_manifest(), _chunk("x", index=0)]).get_resource_content({"skill_hash": 5555})
        self.assertEqual(0, out["chunk_count"])
        self.assertEqual("", out["text"])
        self.assertFalse(out["has_more"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
