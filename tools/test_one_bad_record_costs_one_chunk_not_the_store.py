# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One unreadable record must cost one chunk, not the whole store.

`_load_records_by_count` asked `batch_hget` for every record location in a single call. On the live
16,717-record store exactly one value is not valid UTF-8 (record #13526), so that one call raised;
the handler turned a non-retryable failure into an empty list, and an empty list is
indistinguishable from an empty store -- so the method refetched every record one at a time, twice
per retrieve. Measured: 33,434 `hget` calls, 33,633 socket connects, ~167s per read_all against
3.5-10.3s for the same work in chunks.

The fake client below reproduces exactly that shape: `batch_hget` raises for any block containing
the poisoned field, `hget` works for everything else. What the tests pin:

  * the poisoned record costs ONE chunk of per-record reads, not `count` of them
  * every readable record is still returned, and in order -- the fix must change cost, not content
  * a client with no failures never falls back at all
  * a RETRYABLE failure still propagates, so this cannot mask a backend that is genuinely unwell

The per-record count is asserted against `count` rather than a magic number, so the test states the
defect ("not the whole store") rather than restating the constant.
"""
from __future__ import annotations

import json
import unittest

# The adapters module is imported FIRST on purpose. `matrixark_temporal_direct_retrieve` and
# `matrixark_mcp_temporal_adapters` import each other, so importing the retrieve module first leaves
# it partially initialised and the cycle raises. Production enters through the adapters module, and
# so does this.
try:  # package path
    from tools import matrixark_mcp_temporal_adapters as _adapters  # noqa: F401
    from tools import matrixark_temporal_direct_retrieve as direct_retrieve
    from tools.matrixark_mcp_core import MatrixArkError
except ImportError:  # direct execution from tools/
    import matrixark_mcp_temporal_adapters as _adapters  # type: ignore # noqa: F401
    import matrixark_temporal_direct_retrieve as direct_retrieve  # type: ignore
    from matrixark_mcp_core import MatrixArkError  # type: ignore


POISONED_SEQUENCE = 1526


class _FakeClient:
    """A store with exactly one value that cannot be read, as the live box has."""

    def __init__(self, count, *, poisoned=POISONED_SEQUENCE, retryable=False):
        self.count = count
        self.poisoned = f"{poisoned:020d}"
        self.retryable = retryable
        self.batch_calls = 0
        self.hget_calls = 0

    def _payload(self, field):
        return json.dumps({"record_type": "context_event", "field": field})

    def _fail(self):
        if self.retryable:
            raise MatrixArkError("temporalstore request timed out")
        raise MatrixArkError(
            "Rust TemporalStore batch_hget failed: stored value is not UTF-8: "
            "invalid utf-8 sequence of 1 bytes from index 0"
        )

    def batch_hget(self, entries):
        self.batch_calls += 1
        if any(entry["field"] == self.poisoned for entry in entries):
            self._fail()
        return [
            {"key": e["key"], "field": e["field"], "value": self._payload(e["field"])}
            for e in entries
        ]

    def hget(self, key, field):
        self.hget_calls += 1
        if field == self.poisoned:
            self._fail()
        return self._payload(field)


class _Loader(direct_retrieve._TemporalDirectRetrieveMixin):
    def __init__(self, client):
        self._client = client
        self._last_read_all_native_shard_scan = False

    def _record_location(self, sequence):
        return (f"records:{sequence // 256:06d}", f"{sequence:020d}")

    def _load_records_by_native_shard_scan(self, count):
        return None  # the live box returns None here, so the batch path is what runs


class ChunkedBatchReadTest(unittest.TestCase):
    COUNT = 4000

    def test_one_unreadable_record_costs_one_chunk_not_the_store(self) -> None:
        client = _FakeClient(self.COUNT)
        records = _Loader(client)._load_records_by_count(self.COUNT)

        # The DEFECT first, stated without reference to the constant the fix introduces -- against
        # unmodified code this must fail on behaviour, not error on a missing attribute.
        self.assertLess(
            client.hget_calls, self.COUNT,
            "one bad value still cost a whole-store per-record refetch",
        )
        self.assertLessEqual(
            client.hget_calls, getattr(direct_retrieve, "BATCH_HGET_CHUNK", self.COUNT),
            "one bad value fell back to per-record reads beyond its own chunk",
        )
        self.assertEqual(self.COUNT - 1, len(records), "readable records were lost")

    def test_readable_records_come_back_in_order(self) -> None:
        client = _FakeClient(self.COUNT)
        records = _Loader(client)._load_records_by_count(self.COUNT)
        fields = [r["field"] for r in records]
        self.assertEqual(sorted(fields), fields, "chunking reordered the store")
        self.assertNotIn(client.poisoned, fields)

    def test_a_healthy_store_never_falls_back(self) -> None:
        client = _FakeClient(self.COUNT, poisoned=-1)
        records = _Loader(client)._load_records_by_count(self.COUNT)
        self.assertEqual(0, client.hget_calls, "a healthy store paid for per-record reads")
        self.assertEqual(self.COUNT, len(records))

    def test_a_retryable_failure_still_propagates(self) -> None:
        # Degrading here would hide a backend that is genuinely unwell behind a slow success.
        client = _FakeClient(self.COUNT, retryable=True)
        with self.assertRaises(MatrixArkError):
            _Loader(client)._load_records_by_count(self.COUNT)

    def test_it_batches_rather_than_asking_for_everything_at_once(self) -> None:
        client = _FakeClient(self.COUNT, poisoned=-1)
        _Loader(client)._load_records_by_count(self.COUNT)
        self.assertGreater(
            client.batch_calls, 1,
            "the whole store was still requested in a single batch_hget",
        )


if __name__ == "__main__":
    unittest.main()
