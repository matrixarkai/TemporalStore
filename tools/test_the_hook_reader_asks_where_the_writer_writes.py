#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The hook reader asks for a record where the writer put it.

A record at `sequence` is written to `{prefix}:records:{sequence // shard_size:06d}` under field
`{sequence % shard_size:020d}`, with `shard_size` = `DIRECT_RECORD_LOG_SHARD_SIZE` (256). The
codex-hook query in `matrixark_http` asked for `{sequence // 10000:06d}` under the ABSOLUTE
sequence instead -- wrong in both halves, and wrong in a way that hides itself:

    sequence  writer's key/field        the reader's old key/field
    ---------------------------------------------------------------
    3         records:000000 / ...003   records:000000 / ...003     agree
    255       records:000000 / ...255   records:000000 / ...255     agree
    256       records:000001 / ...000   records:000000 / ...256     miss
    300       records:000001 / ...044   records:000000 / ...300     miss

The two layouts agree for exactly the first shard, so the query answered correctly on any store
small enough to test by hand and saw nothing after record 255 on a real one. And `_hook_collect`
walks DOWN from the newest sequence, so the records it looks at first are precisely the ones it
could not address.

The miss is silent by construction: a candidate key that returns nothing is skipped, the row is
never built, and the endpoint reports `no_matching_rows` -- a status that names the wrong cause.
An operator reads it as "this store has no real user messages".

This file pins BOTH halves. `test_a_record_past_the_first_shard_is_found` fails if either the
shard divisor or the field changes back, and the legacy test below keeps the old layout reachable
as a fallback rather than deleted, so a store written that way is not stranded by the fix.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_http  # noqa: E402
from matrixark_mcp_runtime_config import DIRECT_RECORD_LOG_SHARD_SIZE  # noqa: E402

PREFIX = "matrixark:codex-hook:test-layout"

#: Two sequences: one inside the first shard, where the old and new layouts agree, and one past
#: it, where they do not. The first is the control -- without it, a harness that found nothing at
#: all would look the same as the bug.
INSIDE_FIRST_SHARD = 3
PAST_FIRST_SHARD = 300


def _message(text: str) -> str:
    return json.dumps({"role": "user", "text": text, "timestamp_ms": 1_700_000_000_000})


class _DictReader:
    """The smallest thing `_hook_collect` needs: a count and a hash get."""

    def __init__(self, name: str, store: dict[tuple[str, str], str], count: int) -> None:
        self.name = name
        self._store = store
        self._count = count
        self.asked: list[tuple[str, str]] = []

    def get_string(self, key: str) -> str | None:
        if key == f"{PREFIX}:record_count":
            return str(self._count)
        return None

    def hget(self, key: str, field: str) -> str | None:
        self.asked.append((key, field))
        return self._store.get((key, field))


def _written_by_the_writer(sequences: dict[int, str]) -> dict[tuple[str, str], str]:
    """Seed exactly as `_record_location` writes: shard by division, field by remainder."""
    store: dict[tuple[str, str], str] = {}
    for sequence, text in sequences.items():
        shard = sequence // DIRECT_RECORD_LOG_SHARD_SIZE
        offset = sequence % DIRECT_RECORD_LOG_SHARD_SIZE
        store[(f"{PREFIX}:records:{shard:06d}", f"{offset:020d}")] = _message(text)
    return store


def _written_by_the_legacy_layout(sequences: dict[int, str]) -> dict[tuple[str, str], str]:
    store: dict[tuple[str, str], str] = {}
    for sequence, text in sequences.items():
        shard = sequence // matrixark_http._CODEX_HOOK_LEGACY_SHARD_SIZE
        store[(f"{PREFIX}:records:{shard:06d}", f"{sequence:020d}")] = _message(text)
    return store


class TheHookReaderAsksWhereTheWriterWrites(unittest.TestCase):

    def _query(self, store):
        reader = _DictReader("native", store, PAST_FIRST_SHARD + 1)
        original_native = matrixark_http._NativeHookStoreReader
        original_service = matrixark_http._RustServiceHookStoreReader
        try:
            matrixark_http._NativeHookStoreReader = lambda args: reader
            matrixark_http._RustServiceHookStoreReader = lambda args: (_ for _ in ()).throw(
                RuntimeError("one backend is enough for a layout test")
            )
            result = matrixark_http.query_codex_hook_messages(
                {"backend": "native", "prefix": PREFIX, "top_k": 10, "scan_limit": 500}
            )
        finally:
            matrixark_http._NativeHookStoreReader = original_native
            matrixark_http._RustServiceHookStoreReader = original_service
        return result, reader

    def _rows(self, result):
        rows = []
        for entry in result.get("results", []) if isinstance(result, dict) else []:
            rows.extend(entry.get("rows", []))
        if not rows:
            rows = result.get("rows", []) if isinstance(result, dict) else []
        return rows

    def test_the_two_layouts_agree_only_inside_the_first_shard(self) -> None:
        """The fixture has to discriminate, or every assertion below is about nothing."""
        self.assertEqual(
            INSIDE_FIRST_SHARD // DIRECT_RECORD_LOG_SHARD_SIZE,
            INSIDE_FIRST_SHARD // matrixark_http._CODEX_HOOK_LEGACY_SHARD_SIZE,
            "the control sequence must be one where both layouts agree",
        )
        self.assertNotEqual(
            PAST_FIRST_SHARD // DIRECT_RECORD_LOG_SHARD_SIZE,
            PAST_FIRST_SHARD // matrixark_http._CODEX_HOOK_LEGACY_SHARD_SIZE,
            "the test sequence must land in different shards under the two layouts",
        )
        self.assertNotEqual(
            PAST_FIRST_SHARD % DIRECT_RECORD_LOG_SHARD_SIZE,
            PAST_FIRST_SHARD,
            "and under different fields, which is the half a shard-only fix would miss",
        )

    def test_a_record_past_the_first_shard_is_found(self) -> None:
        store = _written_by_the_writer(
            {INSIDE_FIRST_SHARD: "an early message", PAST_FIRST_SHARD: "a later message"}
        )
        result, reader = self._query(store)
        texts = {row.get("text") for row in self._rows(result)}
        self.assertIn(
            "an early message",
            texts,
            "the control: a record inside the first shard was always reachable. If this fails "
            "the harness is broken, not the layout",
        )
        self.assertIn(
            "a later message",
            texts,
            "a record past the first shard: unreachable before, because both the shard and the "
            "field were computed from the wrong size. Asked for: %r" % (reader.asked[:6],),
        )

    def test_the_record_says_which_layout_answered(self) -> None:
        """`projection` is how an operator tells a found record from a guessed one."""
        store = _written_by_the_writer({PAST_FIRST_SHARD: "a later message"})
        result, _reader = self._query(store)
        rows = [row for row in self._rows(result) if row.get("text") == "a later message"]
        self.assertEqual(1, len(rows), "expected exactly one row, got %r" % (rows,))
        self.assertEqual(
            "records",
            rows[0].get("projection"),
            "the writer's own layout answered, not a fallback",
        )

    def test_a_store_in_the_legacy_layout_is_not_stranded(self) -> None:
        """The old shape stays reachable, one miss later, and says so.

        Deleting it would trade one silent failure for another: a store written that way would
        stop answering, with the same `no_matching_rows` and the same wrong explanation.
        """
        store = _written_by_the_legacy_layout({PAST_FIRST_SHARD: "an old-layout message"})
        result, _reader = self._query(store)
        rows = [row for row in self._rows(result) if row.get("text") == "an old-layout message"]
        self.assertEqual(1, len(rows), "expected the legacy record, got %r" % (rows,))
        self.assertEqual(
            "records-legacy",
            rows[0].get("projection"),
            "and it is labelled as the fallback, so a reader can tell which layout served",
        )


class TheScanWindowReachesTheFirstRecord(unittest.TestCase):
    """Sequences are zero-based, and the window's floor was one.

    The writer assigns `sequence = count` and then increments, so the first record a store ever
    writes is sequence 0. `_hook_collect` floored its scan at `max(1, ...)`, which put sequence 0
    outside every window it can open. A store holding exactly one record therefore answered
    `no_matching_rows`, and every larger store was quietly missing its oldest.

    This is the same failure shape as the addressing bug above and it compounded with it: one
    made the newest records unaddressable, the other made the oldest unreachable.
    """

    def _query(self, store, count, scan_limit=500):
        reader = _DictReader("native", store, count)
        original_native = matrixark_http._NativeHookStoreReader
        original_service = matrixark_http._RustServiceHookStoreReader
        try:
            matrixark_http._NativeHookStoreReader = lambda args: reader
            matrixark_http._RustServiceHookStoreReader = lambda args: (_ for _ in ()).throw(
                RuntimeError("one backend is enough for a window test")
            )
            return matrixark_http.query_codex_hook_messages(
                {"backend": "native", "prefix": PREFIX, "top_k": 10, "scan_limit": scan_limit}
            )
        finally:
            matrixark_http._NativeHookStoreReader = original_native
            matrixark_http._RustServiceHookStoreReader = original_service

    def _texts(self, result):
        rows = []
        for entry in result.get("results", []):
            rows.extend(entry.get("rows", []))
        return {row.get("text") for row in rows}

    def test_a_store_of_one_record_is_not_reported_empty(self) -> None:
        """The smallest store there is, and the one the old floor could never answer."""
        store = _written_by_the_writer({0: "the only message"})
        result = self._query(store, count=1)
        self.assertIn(
            "the only message",
            self._texts(result),
            "a store whose single record is sequence 0 answered as if it held nothing",
        )

    def test_the_oldest_record_is_reachable(self) -> None:
        store = _written_by_the_writer({0: "oldest", 1: "middle", 2: "newest"})
        self.assertEqual(
            {"oldest", "middle", "newest"},
            self._texts(self._query(store, count=3)),
            "every record from sequence 0 up is reachable",
        )

    def test_the_window_still_bounds_what_it_reads(self) -> None:
        """The floor moved; the window did not become unbounded.

        Without this, moving the floor to 0 would be indistinguishable from removing the limit,
        and a store of a million records would be walked in full on every query.

         is 1 here, not 10: the effective limit is , so asking
        for ten rows raises the walk to ten sequences no matter what scan_limit says, and the
        first draft of this test asserted a bound the code never promised.
        """
        store = _written_by_the_writer({0: "oldest", 1: "middle", 2: "newest"})
        reader = _DictReader("native", store, 3)
        original_native = matrixark_http._NativeHookStoreReader
        original_service = matrixark_http._RustServiceHookStoreReader
        try:
            matrixark_http._NativeHookStoreReader = lambda args: reader
            matrixark_http._RustServiceHookStoreReader = lambda args: (_ for _ in ()).throw(
                RuntimeError("one backend is enough")
            )
            result = matrixark_http.query_codex_hook_messages(
                {"backend": "native", "prefix": PREFIX, "top_k": 1, "scan_limit": 1}
            )
        finally:
            matrixark_http._NativeHookStoreReader = original_native
            matrixark_http._RustServiceHookStoreReader = original_service
        sequences = {int(field) % DIRECT_RECORD_LOG_SHARD_SIZE for _key, field in reader.asked}
        self.assertNotIn(
            0, sequences,
            "with scan_limit=1 the walk must not reach sequence 0; it asked for %r"
            % (sorted(sequences),),
        )
        texts = set()
        for entry in result.get("results", []):
            for row in entry.get("rows", []):
                texts.add(row.get("text"))
        self.assertNotIn("oldest", texts, "the limit still bounds what comes back")


if __name__ == "__main__":
    unittest.main()
