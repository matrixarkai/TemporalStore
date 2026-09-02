#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A tenant can decline the write that retrieval performs.

`recall_reinforcement` writes a protection marker for every ref a retrieval selected, so recently
recalled memory survives raw-event pruning. It is genuinely useful, and it makes **retrieval a
writer** — the knob's own description carries the measurement. A tenant paying for that had no way
to decline it: the code read `ranking.get("recall_reinforcement", True)`, a per-REQUEST dict, and
never the tenant knob.

That line is also why an earlier census reported this gate as wired. A local variable named after
the gate, reading a different source entirely — see the name-matching failure recorded in
`test_matrixark_policy_gates_wired`.

It is applied as a **ceiling**, not a default. For a knob whose effect is "retrieval writes
records", a tenant's *off* must not be re-enabled by a per-request argument.

These assert what is STORED. A test that the gate function returns False would have passed
throughout the entire period nothing consulted it.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_tenant_policy as policy  # noqa: E402

MARKER = "context_recall_reinforcement"


def _set_policy(tenant, knobs):
    """Set on every loaded copy of the policy module.

    Under `unittest discover` it is imported under two names, each keeping its own records, so
    setting only the copy this file imported leaves the reader consulting the other one.
    """
    for module in list(sys.modules.values()):
        if getattr(module, "__name__", "").endswith("matrixark_tenant_policy") and \
                hasattr(module, "set_tenant_policy"):
            module.set_tenant_policy(tenant, knobs)


def _markers_after_searching(tenant, knobs, ranking=None):
    import matrixark_mcp_server as mcp

    _set_policy(tenant, knobs)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        scope = {"tenant_id": tenant, "user_id": "u1", "session_id": "s1"}
        for message in ("I am allergic to peanuts and I live in Kyoto.",
                        "My favourite drink is matcha and I bike to work."):
            server.call_tool("matrixark_ingest", {
                "scope": scope, "finalize": True,
                "messages": [{"role": "user", "content": message}]})
        server.call_tool("matrixark_session_commit", {"scope": scope})
        for query in ("what am I allergic to?", "where do I live?", "what do I drink?"):
            request = {"scope": scope, "query": query}
            if ranking is not None:
                request["ranking"] = ranking
            server.call_tool("matrixark_retrieve", request)
        records = adapter.read_all()
    return (sum(1 for r in records if str(r.get("record_type")) == MARKER), len(records))


class ATenantCanDeclineTheWriteTest(unittest.TestCase):

    def test_declining_stops_the_markers(self) -> None:
        off, off_total = _markers_after_searching("rr_declined", {"recall_reinforcement": False})
        self.assertEqual(0, off, "a tenant that declined recall reinforcement still got markers")
        self.assertGreater(off_total, 0,
                           "nothing was stored at all, so the zero above proves nothing")

    def test_the_default_still_writes_them(self) -> None:
        # Without this the assertion above would hold on a deployment that never wrote markers.
        os.environ.pop("MATRIXARK_RECALL_REINFORCEMENT", None)
        on, _total = _markers_after_searching("rr_default", {})
        self.assertGreater(on, 0,
                           "no markers even with the knob at its default, so the comparison is "
                           "vacuous and recall protection may be broken outright")

    def test_a_request_cannot_re_enable_what_the_tenant_declined(self) -> None:
        # The ceiling. This knob decides whether retrieval WRITES, so a per-request argument must
        # not overrule a tenant that turned it off.
        count, _total = _markers_after_searching(
            "rr_ceiling", {"recall_reinforcement": False},
            ranking={"recall_reinforcement": True})
        self.assertEqual(0, count,
                         "a per-request argument switched the tenant's decision back on")

    def test_a_request_can_still_decline_for_a_tenant_that_allows_it(self) -> None:
        count, total = _markers_after_searching(
            "rr_request_off", {}, ranking={"recall_reinforcement": False})
        self.assertEqual(0, count, "the existing per-request opt-out stopped working")
        self.assertGreater(total, 0)


class TheResolverIsExplicitOnlyTest(unittest.TestCase):

    def test_nothing_set_returns_the_callers_default(self) -> None:
        os.environ.pop("MATRIXARK_RECALL_REINFORCEMENT", None)
        self.assertTrue(policy.explicit_bool("recall_reinforcement",
                                             {"tenant_id": "rr_untouched"}, True))
        self.assertFalse(policy.explicit_bool("recall_reinforcement",
                                              {"tenant_id": "rr_untouched"}, False))

    def test_an_env_var_applies_without_a_restart(self) -> None:
        scope = {"tenant_id": "rr_env"}
        os.environ.pop("MATRIXARK_RECALL_REINFORCEMENT", None)
        self.assertTrue(policy.explicit_bool("recall_reinforcement", scope, True))
        try:
            os.environ["MATRIXARK_RECALL_REINFORCEMENT"] = "0"
            self.assertFalse(policy.explicit_bool("recall_reinforcement", scope, True))
        finally:
            os.environ.pop("MATRIXARK_RECALL_REINFORCEMENT", None)
        self.assertTrue(policy.explicit_bool("recall_reinforcement", scope, True))

    def test_junk_is_ignored_rather_than_treated_as_false(self) -> None:
        # "maybe" is not off. Coercing an unparseable value to False would silently disable a
        # protection the tenant never asked to disable.
        scope = {"tenant_id": "rr_junk"}
        try:
            os.environ["MATRIXARK_RECALL_REINFORCEMENT"] = "maybe"
            self.assertTrue(policy.explicit_bool("recall_reinforcement", scope, True))
        finally:
            os.environ.pop("MATRIXARK_RECALL_REINFORCEMENT", None)


if __name__ == "__main__":
    unittest.main()
