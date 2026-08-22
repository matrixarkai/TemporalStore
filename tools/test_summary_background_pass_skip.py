#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The cross-scope background refresh pass must not read the whole store to learn nothing changed.

The background refresher calls `refresh_summaries` with an EMPTY scope on a timer. The first thing
that pass does is `read_all()`, which on a native backend is a full record-log read over the proxy
and holds the single shared lane for its whole duration. Measured on a 105 MB store that is
minutes, during which every retrieve is rejected on lane backpressure -- with no client load and
nothing to refresh:

    before   8 probes, no load: all rejected at 40 s
    after    8 probes, no load: 200 in 67-583 ms, immediately after a restart

Skipping is only safe when it cannot lose work, so these pin the three conditions: the store is
unchanged, the previous pass had nothing left to do, and the caller is the background one.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters


class _StubClient:
    """Just the two key/value ops the pass-state uses, backed by a dict."""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}

    def get_string(self, key: str) -> str:
        return self.strings.get(key, "")

    def put_string(self, key: str, value: str) -> None:
        self.strings[key] = value


class SummaryBackgroundPassSkipTest(unittest.TestCase):
    def setUp(self) -> None:
        # Stand in for the parent implementation, so we can see whether the pass actually ran.
        self.calls: list[dict] = []
        self.refreshed = 0
        self._original = adapters.MatrixArkLocalAdapter.refresh_summaries

        def fake_refresh(inner_self, args):
            self.calls.append(args)
            return {"refreshed_count": self.refreshed}

        adapters.MatrixArkLocalAdapter.refresh_summaries = fake_refresh
        self.addCleanup(
            lambda: setattr(adapters.MatrixArkLocalAdapter, "refresh_summaries", self._original)
        )

    def _adapter(self, client: _StubClient, count: int = 100):
        # object.__new__ skips __init__, which would dial a metaserver / spawn a CLI.
        adapter = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
        adapter._storage_prefix = "matrixark:test"
        adapter._client = client
        adapter._get_count = lambda: count  # type: ignore[assignment]
        return adapter

    def test_unchanged_store_after_an_empty_pass_is_skipped(self) -> None:
        client = _StubClient()
        adapter = self._adapter(client, count=100)
        first = adapter.refresh_summaries({"scope": {}})
        self.assertEqual(1, len(self.calls), "the first pass must run -- nothing is known yet")
        self.assertNotIn("skipped", first)

        second = adapter.refresh_summaries({"scope": {}})
        self.assertEqual(1, len(self.calls), "an unchanged store must not be read again")
        self.assertTrue(second.get("skipped"))
        self.assertEqual(0, second.get("refreshed_count"))

    def test_a_changed_store_runs_the_pass(self) -> None:
        client = _StubClient()
        adapter = self._adapter(client, count=100)
        adapter.refresh_summaries({"scope": {}})
        self.assertEqual(1, len(self.calls))
        # A write landed: the count moved, so there may be new dirty state.
        adapter._get_count = lambda: 101  # type: ignore[assignment]
        adapter.refresh_summaries({"scope": {}})
        self.assertEqual(2, len(self.calls), "a changed store must run the pass")

    def test_a_pass_that_left_work_behind_runs_again(self) -> None:
        """A pass that hit its node limit must be allowed to continue, unchanged store or not."""
        client = _StubClient()
        adapter = self._adapter(client, count=100)
        self.refreshed = 64  # hit the limit; there is a backlog
        adapter.refresh_summaries({"scope": {}})
        self.assertEqual(1, len(self.calls))
        adapter.refresh_summaries({"scope": {}})
        self.assertEqual(2, len(self.calls), "a backlog must not be skipped")

    def test_a_scoped_caller_is_never_skipped(self) -> None:
        """The pre-retrieval refresh passes a real scope and is on the request path already."""
        client = _StubClient()
        adapter = self._adapter(client, count=100)
        for _ in range(3):
            result = adapter.refresh_summaries({"scope": {"user_id": "u1"}})
            self.assertNotIn("skipped", result)
        self.assertEqual(3, len(self.calls))

    def test_the_decision_survives_a_restart(self) -> None:
        """The state lives in the store, not the instance.

        An in-memory-only token makes every restart pay one full-store pass, which on a large
        store is a multi-minute window where the gateway answers nothing.
        """
        client = _StubClient()
        first = self._adapter(client, count=100)
        first.refresh_summaries({"scope": {}})
        self.assertEqual(1, len(self.calls))

        # A brand-new adapter over the SAME store, as after a restart.
        second = self._adapter(client, count=100)
        result = second.refresh_summaries({"scope": {}})
        self.assertEqual(1, len(self.calls), "a restart must not force a full-store pass")
        self.assertTrue(result.get("skipped"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
