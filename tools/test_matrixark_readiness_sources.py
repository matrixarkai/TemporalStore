#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A readiness row that read a setting and one that counted records both print "ok".

Only one of them survives the deployment being misconfigured in a way that still answers 200, and
that is the case this checklist exists for. A configured encoder that is unreachable, misnamed, or
pointed at a base URL without `/v1` falls back to hash vectors while "Embedding model: ok" keeps
saying ok -- so the row has to say which kind of claim it is making.

The engine is the third kind. It publishes nothing about embeddings or models, but it does publish
the storage backend it actually resolved, which no amount of reading configuration can produce.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import (  # noqa: E402
    _FakeResponse, _FakeServer, _cfg, _factory_for, drive,
)

ADMIN = {"Authorization": "Bearer k-acme"}

_BACKEND_METRICS = (
    b'temporalstore_storage_backend{backend="shared_path",replication="shared_store"} 1\n'
)


def _overview(response=None):
    cfg = _cfg(blob_connection_factory=_factory_for(response or _FakeResponse(200, _BACKEND_METRICS)))
    app = gw.make_v1_app(_FakeServer(), cfg)
    status, _headers, body = drive(app, method="GET", path="/v1/admin/overview", headers=ADMIN)
    return status, json.loads(body)


def _rows(payload):
    return payload.get("readiness") or payload.get("checks") or []


class EveryCheckSaysWhereItsAnswerCameFromTest(unittest.TestCase):

    def test_every_row_carries_a_known_source(self) -> None:
        status, payload = _overview()
        self.assertEqual(200, status)
        rows = _rows(payload)
        self.assertTrue(rows, "no readiness rows at all -- this test would pass on an empty list")
        for row in rows:
            with self.subTest(check=row["id"]):
                self.assertIn(row.get("source"), gw._CHECK_SOURCE_LABELS)
                self.assertTrue(row.get("source_label"))

    def test_a_check_with_no_declared_source_is_refused(self) -> None:
        # The point of declaring per id rather than defaulting: a check added later cannot inherit
        # the authoritative-looking answer by saying nothing.
        self.assertNotIn("invented_check", gw._CHECK_SOURCES)
        original = dict(gw._CHECK_SOURCES)
        try:
            gw._CHECK_SOURCES.pop("memory")
            with self.assertRaises(ValueError):
                _overview()
        finally:
            gw._CHECK_SOURCES.clear()
            gw._CHECK_SOURCES.update(original)

    def test_a_configured_encoder_is_not_reported_as_a_working_one(self) -> None:
        # The claim that matters. "Embedding model: ok" is a statement about configuration; it is
        # not evidence that a single model vector was ever produced.
        _st, payload = _overview()
        by_id = {row["id"]: row for row in _rows(payload)}
        self.assertEqual("configuration", by_id["embedding"]["source"])
        self.assertEqual("configuration", by_id["extraction"]["source"])
        # ...while counts of what is actually stored are measurements.
        self.assertEqual("measured", by_id["memory"]["source"])
        self.assertEqual("measured", by_id["content"]["source"])

    def test_the_two_kinds_are_labelled_differently(self) -> None:
        # If both labels rendered the same string the distinction would be invisible on the page,
        # which is the only place it does any good.
        self.assertNotEqual(gw._CHECK_SOURCE_LABELS["configuration"],
                            gw._CHECK_SOURCE_LABELS["measured"])
        self.assertNotEqual(gw._CHECK_SOURCE_LABELS["engine"],
                            gw._CHECK_SOURCE_LABELS["configuration"])


class TheEngineRowReportsTheEngineTest(unittest.TestCase):

    def test_it_reports_the_backend_the_datanode_resolved(self) -> None:
        _st, payload = _overview()
        row = {r["id"]: r for r in _rows(payload)}["storage_backend"]
        self.assertEqual("engine", row["source"])
        self.assertEqual("ok", row["status"])
        self.assertIn("shared_path", row["detail"])

    def test_an_unreachable_datanode_says_unknown_rather_than_ok(self) -> None:
        # Reporting "ok" from silence would be the same defect one level up: a row that cannot see
        # the engine claiming to describe it.
        _st, payload = _overview(_FakeResponse(503, b""))
        row = {r["id"]: r for r in _rows(payload)}["storage_backend"]
        self.assertEqual("engine", row["source"])
        self.assertNotEqual("ok", row["status"])
        self.assertTrue(row["how"], "no steps offered for a state that needs action")

    def test_the_overview_still_answers_when_the_datanode_raises(self) -> None:
        def explode(_cfg_arg):
            raise OSError("connection refused")
        app = gw.make_v1_app(_FakeServer(), _cfg(blob_connection_factory=explode))
        status, _h, body = drive(app, method="GET", path="/v1/admin/overview", headers=ADMIN)
        self.assertEqual(200, status, "the page that reports on the deployment fell over")
        row = {r["id"]: r for r in _rows(json.loads(body))}["storage_backend"]
        self.assertNotEqual("ok", row["status"])


class ThePageShowsTheSourceTest(unittest.TestCase):
    """Read out of the rendered DOM, not grepped out of the file.

    The presence check this replaces passed a mutation that broke the actual render, because the
    variable was still mentioned elsewhere in the file. Only running the page distinguishes a label
    that reaches a row from one that is merely referenced.
    """

    def setUp(self) -> None:
        import shutil
        if not shutil.which("node"):
            self.skipTest("node is not installed")

    def _render(self):
        import subprocess
        import tempfile
        _st, payload = _overview()
        here = os.path.dirname(os.path.abspath(__file__))
        page = os.path.join(here, "portal", "overview_portal.html")
        harness = os.path.join(here, "portal", "overview_checklist_harness.js")
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(payload, handle)
            fixture = handle.name
        try:
            proc = subprocess.run(["node", harness, page, fixture],
                                  capture_output=True, text=True, timeout=60)
        finally:
            os.unlink(fixture)
        self.assertEqual(0, proc.returncode, proc.stderr)
        return json.loads(proc.stdout)

    def test_each_rendered_row_says_where_its_answer_came_from(self) -> None:
        result = self._render()
        self.assertEqual([], result["errors"], "the page's scripts threw")
        self.assertGreater(result["rows"], 1, "no checklist rows rendered")
        # Both kinds reach the page, and they read differently there.
        self.assertIn(gw._CHECK_SOURCE_LABELS["configuration"], result["checks"])
        self.assertIn(gw._CHECK_SOURCE_LABELS["measured"], result["checks"])
        self.assertIn(gw._CHECK_SOURCE_LABELS["engine"], result["checks"])


if __name__ == "__main__":
    unittest.main()
