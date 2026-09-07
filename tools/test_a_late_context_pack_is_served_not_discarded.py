# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A retrieve that finishes LATE must still be served; only one that never finished may be dropped.

Both agents on the live box served empty context for hours while retrieval was healthy. The
retrieve completed and returned a full pack every time; the dispatcher then discarded that pack
because it had taken longer than the request deadline and substituted an empty
`deadline_fallback_pack`. On a native backend that replacement is built from `records = []`, so a
complete answer became no answer, with no error raised and -- because the compact pack renames
`quality_warnings` to `warnings`, which no hook read -- no warning surfaced either.

The deadline bounds how long a caller WAITS. By the time the dispatcher inspects the result the
wait is over and the work is paid for, so lateness is a property to LABEL, not grounds to delete
the answer.

These tests pin both halves, and the third pins the boundary: when `retrieve` RAISES there is no
pack to serve and the fallback is still the only option. Without that case a "fix" that simply
deleted the timeout branch would look correct.
"""
from __future__ import annotations

import time
import unittest

try:  # package path
    from tools import matrixark_hook_pack_cache as pack_cache
    from tools import matrixark_mcp_dispatch as dispatch
    from tools import matrixark_mcp_server as mcp
    from tools.matrixark_mcp_core import MatrixArkError
except ImportError:  # direct execution from tools/
    import matrixark_hook_pack_cache as pack_cache  # type: ignore
    import matrixark_mcp_dispatch as dispatch  # type: ignore
    import matrixark_mcp_server as mcp  # type: ignore
    from matrixark_mcp_core import MatrixArkError  # type: ignore


REAL_PACK = {
    "context_pack_id": "pack-that-finished",
    "selected_refs": [{"text": "the fact the agent needed"}],
    "selected_ref_counts": {"context_event": 1},
}


#: Long enough that the retrieve provably overruns DEADLINE_MS, short enough to stay a unit test.
#: Without a real overrun the timeout branch never runs and every assertion below passes on
#: unmodified code -- which is no coverage at all.
SLOW_RETRIEVE_S = 0.05
DEADLINE_MS = 1


class _SlowButCompleteAdapter:
    """Returns a real pack, having taken longer than any deadline the caller set."""

    def __init__(self) -> None:
        self.fallback_calls = 0

    def _backend_label(self) -> str:
        return "temporalstore-direct"

    def retrieve(self, args):
        time.sleep(SLOW_RETRIEVE_S)
        return dict(REAL_PACK)

    def read_all(self):
        raise AssertionError("a completed pack must not be rebuilt from a full scan")

    def deadline_fallback_pack(self, **kwargs):
        self.fallback_calls += 1
        return {"context_pack_id": "deadline-fallback", "selected_refs": []}


class _RaisingAdapter(_SlowButCompleteAdapter):
    """Never produces a pack at all."""

    def retrieve(self, args):
        raise MatrixArkError("temporalstore request timed out")


def _dispatch(adapter, *, request_deadline_ms):
    server = mcp.MatrixArkMcpServer(adapter)
    args = {"query": "what did we change", "scope": {"account_id": "acct"}, "_matrixark_auth": {}}
    return server, dispatch.dispatch_matrixark_tool(
        server, "matrixark_retrieve", args, None, {}, request_deadline_ms,
    )


def _refs(response):
    for key in ("selected_refs", "refs"):
        value = response.get(key)
        if isinstance(value, list):
            return value
    groups = response.get("groups")
    if isinstance(groups, list):
        return [item for group in groups for item in (group.get("items") or [])]
    return []


class LateContextPackTest(unittest.TestCase):
    def test_a_pack_that_finished_late_is_still_served(self) -> None:
        # deadline of 1ms: any real call overruns it, so this is the late-but-complete case.
        adapter = _SlowButCompleteAdapter()
        _, response = _dispatch(adapter, request_deadline_ms=DEADLINE_MS)

        self.assertEqual(
            0, adapter.fallback_calls,
            "a completed pack was replaced by the empty deadline fallback",
        )
        self.assertTrue(
            _refs(response),
            f"the refs the retrieve computed were dropped: {response!r}",
        )

    def test_lateness_is_reported_rather_than_hidden(self) -> None:
        _, response = _dispatch(_SlowButCompleteAdapter(), request_deadline_ms=DEADLINE_MS)
        warnings = [str(w) for w in pack_cache.retrieval_warnings(response)]
        self.assertTrue(
            any("request_deadline_after_retrieve" in w for w in warnings),
            f"a late pack must say so; warnings were {warnings!r}",
        )

    def test_a_retrieve_that_never_finished_still_falls_back(self) -> None:
        # The boundary: no pack exists here, so the fallback is the only thing to serve. A fix that
        # merely removed the timeout branch would break this.
        adapter = _RaisingAdapter()
        try:
            _, response = _dispatch(adapter, request_deadline_ms=DEADLINE_MS)
        except MatrixArkError:
            self.fail("a timed-out retrieve must degrade to the fallback pack, not propagate")
        self.assertEqual(1, adapter.fallback_calls)
        self.assertEqual("deadline-fallback", response.get("context_pack_id"))


class CompactWarningNameTest(unittest.TestCase):
    """The hooks receive the COMPACT pack, which spells the field `warnings`."""

    def test_warnings_are_read_under_the_compact_name(self) -> None:
        compact = {"context_pack_id": "p", "warnings": ["retrieval_deadline_exceeded:x"]}
        self.assertEqual(["retrieval_deadline_exceeded:x"], pack_cache.retrieval_warnings(compact))

    def test_warnings_are_still_read_under_the_full_name(self) -> None:
        full = {"context_pack_id": "p", "quality_warnings": ["retrieval_deadline_exceeded:x"]}
        self.assertEqual(["retrieval_deadline_exceeded:x"], pack_cache.retrieval_warnings(full))

    def test_a_pack_with_no_warnings_reads_empty(self) -> None:
        self.assertEqual([], pack_cache.retrieval_warnings({"context_pack_id": "p"}))
        self.assertEqual([], pack_cache.retrieval_warnings(None))


if __name__ == "__main__":
    unittest.main()
