#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`storage_record_kind` and `storage_part` are usually the same string arrived at twice.

`storage_record_kind(record)` maps a record_type to a kind, but its first line returns the stored
field when one is present. So the stored copy is normally redundant -- and on the live log the pair
is 1.00% of every byte written -- while still being a real override for a caller who wants a kind
the mapping would not produce.

Hence the conditional: a field is dropped only when the record derives THAT EXACT value without it.
`test_an_unmappable_kind_is_kept` is the test that makes this lossless by construction rather than
by what the corpus happens to hold.

A note on how this was nearly got wrong: deriving WITHOUT stripping the stored fields compares the
value to itself and reports a 100% match whatever the truth is. The first measurement did exactly
that and "proved" derivability on 11,398 records while proving nothing.
"""
import unittest

try:
    from tools.matrixark_mcp_temporal_append import slim_persisted_record_kind
    from tools.matrixark_mcp_storage_options import storage_record_kind
except ImportError:  # run from tools/
    from matrixark_mcp_temporal_append import slim_persisted_record_kind
    from matrixark_mcp_storage_options import storage_record_kind


class RecordKindTests(unittest.TestCase):
    def test_the_mapping_is_not_the_identity(self):
        """Positive control: the mapping really does change some record types.

        Without this, every other test here would still pass if `storage_record_kind` simply
        echoed `record_type`, and the suite would be asserting nothing about the mapping.
        """
        self.assertEqual("summary", storage_record_kind({"record_type": "context_summary"}))
        self.assertEqual("index", storage_record_kind({"record_type": "context_index"}))

    def test_a_derivable_pair_is_dropped(self):
        record = {"record_type": "context_summary", "storage_record_kind": "summary",
                  "storage_part": "summary", "text": "x"}
        slim = slim_persisted_record_kind(record)
        self.assertNotIn("storage_record_kind", slim)
        self.assertNotIn("storage_part", slim)
        self.assertEqual("x", slim["text"])

    def test_the_dropped_kind_comes_back(self):
        """The invariant: a reader gets the same answer from the slimmed record."""
        for record_type, kind in (("context_summary", "summary"), ("context_index", "index"),
                                  ("context_event", "context_event"),
                                  ("matrixark_idempotency", "matrixark_idempotency")):
            record = {"record_type": record_type, "storage_record_kind": kind,
                      "storage_part": kind}
            slim = slim_persisted_record_kind(record)
            self.assertEqual(kind, storage_record_kind(slim),
                             f"{record_type} did not derive {kind} after slimming")

    def test_an_unmappable_kind_is_kept(self):
        """A kind the mapping would NOT produce is information, not a copy."""
        record = {"record_type": "context_summary", "storage_record_kind": "something_else",
                  "storage_part": "something_else"}
        slim = slim_persisted_record_kind(record)
        self.assertEqual("something_else", slim["storage_record_kind"])
        self.assertEqual("something_else", slim["storage_part"])
        self.assertEqual("something_else", storage_record_kind(slim))

    def test_a_part_that_differs_from_the_kind_is_kept(self):
        record = {"record_type": "context_summary", "storage_record_kind": "summary",
                  "storage_part": "a_different_part"}
        slim = slim_persisted_record_kind(record)
        self.assertNotIn("storage_record_kind", slim)
        self.assertEqual("a_different_part", slim["storage_part"])

    def test_a_part_alone_is_handled(self):
        record = {"record_type": "context_index", "storage_part": "index"}
        self.assertNotIn("storage_part", slim_persisted_record_kind(record))
        kept = {"record_type": "context_index", "storage_part": "not_index"}
        self.assertEqual("not_index", slim_persisted_record_kind(kept)["storage_part"])

    def test_a_record_with_neither_is_returned_as_is(self):
        record = {"record_type": "context_event", "text": "x"}
        self.assertIs(record, slim_persisted_record_kind(record))

    def test_the_input_record_is_not_mutated(self):
        record = {"record_type": "context_summary", "storage_record_kind": "summary",
                  "storage_part": "summary"}
        slim_persisted_record_kind(record)
        self.assertEqual("summary", record["storage_record_kind"])
        self.assertEqual("summary", record["storage_part"])

    def test_a_non_dict_is_returned_as_is(self):
        self.assertEqual("nope", slim_persisted_record_kind("nope"))

    def test_the_envelope_kind_still_decides(self):
        """The mapping consults envelope.kind, which must survive for the derivation to hold."""
        record = {"record_type": "context_event", "envelope": {"kind": "feedback"},
                  "storage_record_kind": "feedback", "storage_part": "feedback"}
        slim = slim_persisted_record_kind(record)
        self.assertNotIn("storage_record_kind", slim)
        self.assertEqual("feedback", storage_record_kind(slim))


if __name__ == "__main__":
    unittest.main()
