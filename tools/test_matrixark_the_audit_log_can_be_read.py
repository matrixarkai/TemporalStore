#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The scope published as "Read the audit log" reads the audit log.

``admin:audit`` is in the catalogue this gateway serves, described in those words, and offered in
the admin preset. The records existed. A tool that reads them back with a tenant check on it
existed. No route reached it -- so the trail was write-only, and a key carrying that scope opened
nothing the usage scope did not already open.

Two things are asserted beyond "it answers 200".

The gate is narrower than its neighbour on purpose. ``_usage_read_denied`` admits ``admin:api_key``
too, which is right for usage counters: a key manager needs to see what their keys are doing. The
audit log is the record of who reached for what and was refused, and a scope that names one thing
should be the thing that opens it. The refusal is checked against a key that is otherwise perfectly
good -- it reads usage in the same test -- so the 403 is about this route and not a broken key.

And the response carries the recording mode. An empty list means two entirely different things:
nothing happened, or nothing was kept. ``MATRIXARK_AUDIT_MODE`` defaults to off, so on most
deployments it means the second, and a reader who cannot tell them apart is reassured by silence
that was never evidence of anything.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

AUDIT = "k-audit"
KEYMGR = "k-keys"
LEGACY = "k-legacy"

ROWS = [
    {"action": "admin.revoke_api_key", "status": "denied", "api_key_id": "ak_1",
     "tenant_id": "t", "created_at_ms": 2},
    {"action": "admin.create_api_key", "status": "ok", "api_key_id": "ak_1",
     "tenant_id": "t", "created_at_ms": 1},
]


class _Server:
    """Answers the audit tool and remembers what it was asked."""

    def __init__(self, rows=None, boom=False):
        self.calls = []
        self.rows = ROWS if rows is None else rows
        self.boom = boom

    def call_tool(self, name, args):
        self.calls.append((name, dict(args)))
        if self.boom:
            raise RuntimeError("the metadata store is unreachable")
        if name == "matrixark_admin_audit":
            return {"status": "ok", "audit_logs": list(self.rows), "count": len(self.rows)}
        return {"ok": name}

    def handle(self, body):
        return {"jsonrpc": "2.0", "id": body.get("id"), "result": {}}


def _drive(*args, **kwargs):
    """The gateway suite's request driver, imported when it is called rather than at import time.

    Under `unittest discover` a test module is reachable as both `tools.X` and bare `X`, so one
    test module importing another at module level pulls a second copy into the run and shifts what
    every later module sees. That has cost an afternoon before, showing up as CI failures in tests
    the branch never touched -- see test_matrixark_no_cross_test_imports.
    """
    from test_matrixark_v1_gateway import drive
    return drive(*args, **kwargs)


def _app(server=None):
    hashed = {
        gw._secret_hash(AUDIT): {"tenant_id": "t", "account_id": "acct",
                                 "scopes": ["admin:audit"]},
        gw._secret_hash(KEYMGR): {"tenant_id": "t", "account_id": "acct",
                                  "scopes": ["admin:api_key"]},
        gw._secret_hash(LEGACY): {"tenant_id": "t", "account_id": "acct"},
    }
    return gw.make_v1_app(server or _Server(),
                          gw.GatewayConfig.from_env({"enforced": True, "hashed_api_keys": hashed}))


def _as(key):
    return {"Authorization": "Bearer " + key}


def _get(app, path, key=AUDIT):
    return _drive(app, method="GET", path=path, headers=_as(key))


class ItIsReachableAtAllTest(unittest.TestCase):

    def test_it_needs_a_key(self) -> None:
        status, _h, _b = _drive(_app(), method="GET", path="/v1/admin/audit")
        self.assertEqual(401, status)

    def test_an_audit_key_reads_it(self) -> None:
        status, _h, body = _get(_app(), "/v1/admin/audit")
        self.assertEqual(200, status)
        payload = json.loads(body)
        self.assertEqual(2, payload["count"])
        self.assertEqual("admin.revoke_api_key", payload["audit_logs"][0]["action"])

    def test_it_is_in_the_published_contract(self) -> None:
        """The API page is generated from this list, so a route missing from it is a route a
        customer has no way to discover."""
        paths = {(r["method"], r["path"]) for r in gw.ROUTE_DOCS}
        self.assertIn(("GET", "/v1/admin/audit"), paths)

    def test_the_contract_warns_that_empty_may_mean_off(self) -> None:
        doc = [r for r in gw.ROUTE_DOCS if r["path"] == "/v1/admin/audit"][0]
        self.assertIn("off", doc["summary"])


class TheGateIsTheScopeThatNamesItTest(unittest.TestCase):

    def setUp(self) -> None:
        self.app = _app()

    def test_a_key_manager_key_is_refused(self) -> None:
        status, _h, body = _get(self.app, "/v1/admin/audit", KEYMGR)
        self.assertEqual(403, status)
        payload = json.loads(body)
        self.assertEqual("insufficient_scope", payload["error"])
        self.assertEqual(["admin:audit"], payload["required"])

    def test_the_control_that_key_is_otherwise_fine(self) -> None:
        """Without this the refusal above could be a broken key rather than a narrow gate."""
        status, _h, _b = _get(self.app, "/v1/admin/api_key_usage", KEYMGR)
        self.assertEqual(200, status)

    def test_an_unrestricted_key_is_allowed(self) -> None:
        """Same posture as every neighbouring route: a key with no scope list is legacy, not
        forbidden. Changing that here and nowhere else would be its own surprise."""
        status, _h, _b = _get(self.app, "/v1/admin/audit", LEGACY)
        self.assertEqual(200, status)


class TheCallerCannotAskForSomebodyElsesRecordsTest(unittest.TestCase):

    def test_the_identity_sent_to_the_tool_is_the_callers_own(self) -> None:
        server = _Server()
        _st, _h, _b = _get(_app(server), "/v1/admin/audit")
        name, args = server.calls[-1]
        self.assertEqual("matrixark_admin_audit", name)
        self.assertEqual("t", args["scope"]["tenant_id"])
        self.assertEqual("acct", args["scope"]["account_id"])

    def test_a_tenant_in_the_query_string_changes_nothing(self) -> None:
        """The tool fences on the identity it is handed. Whatever the caller writes in the URL has
        to lose to the key they authenticated with."""
        server = _Server()
        _st, _h, _b = _get(_app(server), "/v1/admin/audit?tenant_id=someone_else&account_id=other")
        _name, args = server.calls[-1]
        self.assertEqual("t", args["scope"]["tenant_id"])
        self.assertEqual("acct", args["scope"]["account_id"])


class TheLimitIsBoundedTest(unittest.TestCase):

    def _limit(self, query):
        server = _Server()
        _st, _h, _b = _get(_app(server), "/v1/admin/audit" + query)
        return server.calls[-1][1]["limit"]

    def test_the_default(self) -> None:
        self.assertEqual(100, self._limit(""))

    def test_a_huge_request_is_capped(self) -> None:
        """The tool walks the whole record log to answer, so an unbounded limit is an unbounded
        response on top of a walk that already costs."""
        self.assertEqual(500, self._limit("?limit=9999"))

    def test_zero_and_negatives_become_one(self) -> None:
        self.assertEqual(1, self._limit("?limit=0"))
        self.assertEqual(1, self._limit("?limit=-5"))

    def test_nonsense_falls_back_rather_than_failing(self) -> None:
        self.assertEqual(100, self._limit("?limit=lots"))


class AnEmptyListSaysWhichKindOfEmptyTest(unittest.TestCase):

    def setUp(self) -> None:
        previous = os.environ.get("MATRIXARK_AUDIT_MODE")

        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_AUDIT_MODE", None)
            else:
                os.environ["MATRIXARK_AUDIT_MODE"] = previous

        self.addCleanup(restore)
        # Pinned at a file that does not exist, so apply_boot() has nothing to seed.
        # make_v1_app() calls it, and on a configured box the stored document carries
        # real values -- which would land in the environment after this isolation ran
        # and be read as though the test had set them.
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        stored = os.environ.get("MATRIXARK_RUNTIME_CONFIG_FILE")

        def restore_config() -> None:
            if stored is None:
                os.environ.pop("MATRIXARK_RUNTIME_CONFIG_FILE", None)
            else:
                os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = stored

        self.addCleanup(restore_config)
        os.environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = os.path.join(
            directory.name, "runtime.json")

    def _payload(self, rows):
        _st, _h, body = _get(_app(_Server(rows=rows)), "/v1/admin/audit")
        return json.loads(body)

    def test_nothing_kept_is_reported_as_nothing_kept(self) -> None:
        os.environ["MATRIXARK_AUDIT_MODE"] = "off"
        payload = self._payload([])
        self.assertEqual([], payload["audit_logs"])
        self.assertEqual("off", payload["recording"],
                         "an empty log with nothing to explain it reads as 'all quiet'")

    def test_nothing_happened_is_a_different_answer(self) -> None:
        os.environ["MATRIXARK_AUDIT_MODE"] = "async"
        payload = self._payload([])
        self.assertEqual([], payload["audit_logs"])
        self.assertEqual("async", payload["recording"])

    def test_the_mode_travels_with_records_too(self) -> None:
        os.environ["MATRIXARK_AUDIT_MODE"] = "async"
        self.assertEqual("async", self._payload(ROWS)["recording"])

    def test_an_unset_variable_reports_off_not_blank(self) -> None:
        """Blank would render as an empty cell, which is the ambiguity this field exists to end."""
        os.environ.pop("MATRIXARK_AUDIT_MODE", None)
        self.assertEqual("off", self._payload([])["recording"])


class ABackendThatCannotAnswerSaysSoTest(unittest.TestCase):

    def test_it_is_not_reported_as_an_empty_log(self) -> None:
        """The failure that matters: a store that cannot be read returning 200 with no rows is
        indistinguishable from a clean audit trail."""
        status, _h, body = _get(_app(_Server(boom=True)), "/v1/admin/audit")
        self.assertEqual(502, status)
        self.assertEqual("backend_unavailable", json.loads(body)["error"])


if __name__ == "__main__":
    unittest.main()
