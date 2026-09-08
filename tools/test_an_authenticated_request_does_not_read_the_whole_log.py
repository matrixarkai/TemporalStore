#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""An authenticated request must not read the whole record log to find one key.

Every `latest_*` lookup in the access layer is a newest-wins scan for a SINGLE record type, and
each one used to read the entire log. On an authenticated request that is several full-corpus
reads before any work happens, so the cost of authenticating grew with the corpus.

These tests pin the three things that make the narrow path safe rather than merely faster:
the answer is unchanged, the narrow path is actually taken, and an authoritative empty result
does not fall back to the full read.
"""
from __future__ import annotations

import unittest

from matrixark_access import MatrixArkRecordLogMetadataStore


class RecordingAdapter:
    """An adapter that can serve a typed scan, and counts who asked for what."""

    def __init__(self, records, *, supports_scan=True, scan_returns=None):
        self._records = records
        self._supports_scan = supports_scan
        self._scan_returns = scan_returns
        self.read_all_calls = 0
        self.scanned_types = []

    def read_all(self):
        self.read_all_calls += 1
        return list(self._records)

    def __getattr__(self, name):
        # Only expose the scan when this fixture is meant to support it, so the fallback path
        # is exercised by a fixture that genuinely cannot answer rather than by a flag.
        if name == "_scan_records_of_types" and self._supports_scan:
            return self._scan
        raise AttributeError(name)

    def _scan(self, record_types, **kwargs):
        self.scanned_types.append(list(record_types))
        if self._scan_returns is not None:
            return list(self._scan_returns)
        return [
            record for record in self._records
            if str(record.get("record_type", "")) in set(record_types)
        ]


CORPUS = [
    {"record_type": "context_event", "text": "not metadata"},
    {"record_type": "matrixark_api_key", "api_key_id": "k1", "status": "active"},
    {"record_type": "context_event", "text": "also not metadata"},
    {"record_type": "matrixark_user", "user_id": "u1"},
    {"record_type": "matrixark_api_key", "api_key_id": "k2", "status": "active"},
]


class MetadataRecordsOfTypeTest(unittest.TestCase):
    def test_the_answer_is_the_same_as_filtering_the_full_read(self):
        """The narrow path must not change WHICH records come back, or their order."""
        narrow = MatrixArkRecordLogMetadataStore(RecordingAdapter(CORPUS))
        wide = MatrixArkRecordLogMetadataStore(RecordingAdapter(CORPUS, supports_scan=False))
        for record_type in ("matrixark_api_key", "matrixark_user"):
            self.assertEqual(
                narrow.records_of_type(record_type),
                wide.records_of_type(record_type),
                record_type,
            )
        # Append order is what the newest-wins callers reverse; pin it explicitly.
        keys = narrow.records_of_type("matrixark_api_key")
        self.assertEqual([item["api_key_id"] for item in keys], ["k1", "k2"])

    def test_the_narrow_path_is_actually_taken(self):
        """Positive control: without this, an unused fast path would still pass the test above."""
        adapter = RecordingAdapter(CORPUS)
        store = MatrixArkRecordLogMetadataStore(adapter)
        store.records_of_type("matrixark_api_key")
        self.assertEqual(adapter.scanned_types, [["matrixark_api_key"]])
        self.assertEqual(adapter.read_all_calls, 0, "the full log was read anyway")

    def test_an_adapter_without_the_scan_still_answers(self):
        adapter = RecordingAdapter(CORPUS, supports_scan=False)
        store = MatrixArkRecordLogMetadataStore(adapter)
        self.assertEqual(len(store.records_of_type("matrixark_api_key")), 2)
        self.assertEqual(adapter.read_all_calls, 1)

    def test_an_authoritative_empty_does_not_fall_back_to_the_full_read(self):
        """`[]` means "no records of this type"; only None means "could not ask".

        Confusing the two would make a store that legitimately has no users pay for the whole
        corpus on every single request -- the exact cost this change exists to remove.
        """
        adapter = RecordingAdapter(CORPUS, scan_returns=[])
        store = MatrixArkRecordLogMetadataStore(adapter)
        self.assertEqual(store.records_of_type("matrixark_user"), [])
        self.assertEqual(adapter.read_all_calls, 0)


if __name__ == "__main__":
    unittest.main()
