#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A lane record must read the same whether it arrived as a document or as a string.

The lane has always sent a record's stored JSON as a STRING, so every reader parses the envelope
and then parses the record again. The proxy can now send the record as a sub-document instead,
which removes that second parse -- but the switch is only safe if readers accept both shapes,
because the two sides are deployed independently.
"""
from __future__ import annotations

import unittest

# `matrixark_temporal_direct_read` is a mixin the adapters module imports back, so it does
# not import standalone. Import in the order production does, then take the helper.
import matrixark_mcp_temporal_adapters  # noqa: F401
from matrixark_temporal_direct_read import lane_record_payload


class LaneRecordPayloadTest(unittest.TestCase):
    def test_a_string_payload_reads_as_it_always_did(self):
        self.assertEqual(
            lane_record_payload('{"record_type":"context_event","n":7}'),
            {"record_type": "context_event", "n": 7},
        )

    def test_a_document_payload_reads_the_same(self):
        """The point of the change: no second parse, same answer."""
        as_doc = {"record_type": "context_event", "n": 7}
        self.assertEqual(lane_record_payload(as_doc), lane_record_payload('{"record_type":"context_event","n":7}'))
        # and it is the SAME object, not a copy -- the parse is genuinely skipped
        self.assertIs(lane_record_payload(as_doc), as_doc)

    def test_junk_is_empty_rather_than_raising(self):
        """The callers' old try/except swallowed bad payloads; that behaviour is preserved."""
        for junk in ("", None, "not json", "[1,2,3]", 17):
            self.assertEqual(lane_record_payload(junk), {}, junk)


if __name__ == "__main__":
    unittest.main()
