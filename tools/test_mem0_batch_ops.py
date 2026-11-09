#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0 surface completeness on the compat shim: `batch_update`, `batch_delete`, and the
read-after-write behaviour of `add`.

mem0 takes a list of dicts keyed by `memory_id`, with `text` (its name for what this API calls
`data`) and optional `metadata`, and documents a ceiling of 1000 per call.

The behaviour worth pinning is what happens when part of a batch fails. mem0's batch is one
server-side request; here an update is a supersede and a delete is a tombstone, with no
cross-memory transaction behind them, so a batch is N requests and CANNOT be atomic. These tests
pin that a failure is reported per memory and does not abort the rest, so a partial batch is
visible to the caller rather than silently half-applied.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mem0_compat as mem0
except ImportError:  # run from tools/ dir
    import matrixark_mem0_compat as mem0


class _Recorder:
    """Stands in for the shim's HTTP POST, recording (path, body) and replaying canned results."""

    def __init__(self, fail_ids=()):
        self.calls: list[tuple[str, dict]] = []
        self.fail_ids = set(fail_ids)

    def __call__(self, base_url, api_key, path, body, timeout):
        self.calls.append((path, body))
        if str(body.get("memory_id")) in self.fail_ids:
            raise RuntimeError("boom for " + str(body.get("memory_id")))
        return {"ok": True, "echo": body}


class Mem0BatchOpsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._original = mem0._post_json
        self.addCleanup(lambda: setattr(mem0, "_post_json", self._original))

    def _client(self, recorder):
        mem0._post_json = recorder  # type: ignore[assignment]
        client = object.__new__(mem0.Memory)
        client._base_url = "http://gw"
        client._api_key = None
        client._timeout = 30
        return client

    # ---- batch_update --------------------------------------------------------------------

    def test_batch_update_maps_mem0_text_onto_data(self) -> None:
        rec = _Recorder()
        client = self._client(rec)
        result = client.batch_update([
            {"memory_id": "m1", "text": "Updated text"},
            {"memory_id": "m2", "text": "Another", "metadata": {"verified": True}},
        ])
        self.assertEqual(["/v1/update", "/v1/update"], [c[0] for c in rec.calls])
        self.assertEqual({"memory_id": "m1", "data": "Updated text"}, rec.calls[0][1])
        self.assertEqual(
            {"memory_id": "m2", "data": "Another", "metadata": {"verified": True}},
            rec.calls[1][1],
        )
        self.assertEqual(2, result["updated"])
        self.assertEqual([], result["failed"])

    def test_batch_update_also_accepts_the_native_data_key(self) -> None:
        rec = _Recorder()
        client = self._client(rec)
        client.batch_update([{"memory_id": "m1", "data": "native name"}])
        self.assertEqual({"memory_id": "m1", "data": "native name"}, rec.calls[0][1])

    def test_batch_update_reports_a_failure_and_keeps_going(self) -> None:
        """A partial batch must be visible. Aborting would leave the caller unable to tell which
        memories were applied."""
        rec = _Recorder(fail_ids={"m2"})
        client = self._client(rec)
        result = client.batch_update([
            {"memory_id": "m1", "text": "a"},
            {"memory_id": "m2", "text": "b"},
            {"memory_id": "m3", "text": "c"},
        ])
        self.assertEqual(3, len(rec.calls), "a failure must not stop the remaining entries")
        self.assertEqual(2, result["updated"])
        self.assertEqual(["m2"], [f["memory_id"] for f in result["failed"]])

    def test_batch_update_requires_memory_id(self) -> None:
        client = self._client(_Recorder())
        with self.assertRaises(ValueError):
            client.batch_update([{"text": "no id"}])

    def test_batch_update_refuses_more_than_the_documented_ceiling(self) -> None:
        client = self._client(_Recorder())
        too_many = [{"memory_id": str(i), "text": "x"} for i in range(mem0.MEM0_BATCH_LIMIT + 1)]
        with self.assertRaises(ValueError):
            client.batch_update(too_many)

    # ---- batch_delete --------------------------------------------------------------------

    def test_batch_delete_takes_mem0_dicts(self) -> None:
        rec = _Recorder()
        client = self._client(rec)
        result = client.batch_delete([{"memory_id": "m1"}, {"memory_id": "m2"}])
        self.assertEqual(["/v1/delete", "/v1/delete"], [c[0] for c in rec.calls])
        self.assertEqual([{"memory_id": "m1"}, {"memory_id": "m2"}], [c[1] for c in rec.calls])
        self.assertEqual(2, result["deleted"])

    def test_batch_delete_also_takes_bare_ids(self) -> None:
        rec = _Recorder()
        client = self._client(rec)
        client.batch_delete(["m1", "m2"])
        self.assertEqual([{"memory_id": "m1"}, {"memory_id": "m2"}], [c[1] for c in rec.calls])

    def test_batch_delete_reports_a_failure_and_keeps_going(self) -> None:
        rec = _Recorder(fail_ids={"m1"})
        client = self._client(rec)
        result = client.batch_delete(["m1", "m2"])
        self.assertEqual(2, len(rec.calls))
        self.assertEqual(1, result["deleted"])
        self.assertEqual(["m1"], [f["memory_id"] for f in result["failed"]])

    def test_batch_delete_refuses_more_than_the_documented_ceiling(self) -> None:
        client = self._client(_Recorder())
        with self.assertRaises(ValueError):
            client.batch_delete([str(i) for i in range(mem0.MEM0_BATCH_LIMIT + 1)])

    def test_empty_batches_do_nothing(self) -> None:
        rec = _Recorder()
        client = self._client(rec)
        self.assertEqual(0, client.batch_update([])["updated"])
        self.assertEqual(0, client.batch_delete([])["deleted"])
        self.assertEqual([], rec.calls)


class Mem0AddFinalizeTest(unittest.TestCase):
    """`add` must be read-after-write, as it is on mem0.

    Without a finalize the ingest is a streaming write that only becomes visible once a debounce
    elapses, and every further write to the same scope pushes that debounce out -- so a burst of
    `add` calls can stay invisible for as long as the burst lasts. Against a live gateway, three
    `add` calls followed by `get_all` returned 0 memories; with the finalize they return 3.
    """

    def setUp(self) -> None:
        self._original = mem0._post_json
        self.addCleanup(lambda: setattr(mem0, "_post_json", self._original))
        self.rec = _Recorder()
        mem0._post_json = self.rec  # type: ignore[assignment]
        self.client = object.__new__(mem0.Memory)
        self.client._base_url = "http://gw"
        self.client._api_key = None
        self.client._timeout = 30

    def test_add_finalizes_by_default(self) -> None:
        self.client.add("I like espresso.", user_id="u1")
        path, body = self.rec.calls[0]
        self.assertEqual("/v1/ingest", path)
        self.assertTrue(body.get("finalize"), "add must commit so a later get_all sees it")

    def test_streaming_callers_can_opt_out(self) -> None:
        """Opting out says so explicitly rather than omitting the key, so the request states the
        caller's intent instead of leaning on whatever the server's default happens to be."""
        self.client.add("part of a conversation", user_id="u1", finalize=False)
        _, body = self.rec.calls[0]
        self.assertIs(False, body.get("finalize"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
