#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""POST /v1/ingest_file honours X-Scope as the documented JSON scope object.

The header is documented as `{"user_id":"alice","session_id":"s-42"}`. Passed through unparsed it
reached the string branch of `_apply_identity`, which reads a bare string as a NAMESPACE LABEL --
so the upload was filed under `acme/{"user_id":"alice"}` and the user_id was dropped. Nothing
errored; the file simply landed in the wrong scope, which only shows up later as a retrieve that
cannot find it.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402
from test_matrixark_v1_gateway import (  # noqa: E402
    _FakeConn, _FakeResponse, _FakeServer, _cfg, _factory_for, drive,
)

ADMIN = {"Authorization": "Bearer k-acme"}


def _app(server):
    return gw.make_v1_app(server, _cfg(blob_connection_factory=_factory_for(
        _FakeResponse(200, b'{"status":"ok"}'))))


class IngestFileScopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _FakeServer()
        self.app = _app(self.server)

    def _upload(self, headers):
        return drive(self.app, method="POST", path="/v1/ingest_file", raw=b"# a skill\n",
                     headers=dict(ADMIN, **headers))

    def _ingest_args(self):
        for name, args in self.server.calls:
            if name == "matrixark_ingest":
                return args
        return None

    def test_a_json_scope_header_reaches_the_backend_as_an_object(self) -> None:
        status, _, _ = self._upload({
            "X-Filename": "checkout.md",
            "X-Scope": json.dumps({"user_id": "alice", "session_id": "s-42"}),
        })
        self.assertIn(status, (200, 202))
        args = self._ingest_args()
        self.assertIsNotNone(args, "the upload never reached the ingest handler")
        self.assertIsInstance(args["scope"], dict)
        self.assertEqual("alice", args["scope"]["user_id"])
        self.assertEqual("s-42", args["scope"]["session_id"])
        # The tenant is still pinned from the key, as on every other route.
        self.assertEqual("acme", args["scope"]["tenant_id"])

    def test_a_plain_string_scope_is_still_a_namespace_label(self) -> None:
        status, _, _ = self._upload({"X-Filename": "checkout.md", "X-Scope": "team-a"})
        self.assertIn(status, (200, 202))
        args = self._ingest_args()
        self.assertEqual("acme/team-a", args["scope"]["namespace"])

    def test_scope_json_that_does_not_parse_is_a_400_not_a_silent_misfile(self) -> None:
        status, _, body = self._upload({"X-Filename": "checkout.md", "X-Scope": '{"user_id":'})
        self.assertEqual(400, status)
        self.assertEqual("invalid_scope", json.loads(body)["error"])
        self.assertIsNone(self._ingest_args())

    def test_no_scope_header_leaves_the_scope_to_the_server_default(self) -> None:
        status, _, _ = self._upload({"X-Filename": "checkout.md"})
        self.assertIn(status, (200, 202))
        args = self._ingest_args()
        self.assertNotIn("user_id", args.get("scope", {}))


class IngestFileBlobTierTest(unittest.TestCase):
    """An unreachable blob tier is an ordinary operational state -- a datanode restarting, a wrong
    MATRIXARK_DATANODE_URL -- and it was the one failure on this route that escaped as an unhandled
    exception: a bare 500 with no reason, and a stack trace in the log that reads like a bug in the
    upload rather than a backend that is down."""

    def test_a_refused_connection_is_a_502_with_a_reason(self) -> None:
        def refuse(_cfg):
            raise ConnectionRefusedError(111, "Connection refused")

        server = _FakeServer()
        app = gw.make_v1_app(server, _cfg(blob_connection_factory=refuse))
        status, _headers, body = drive(app, method="POST", path="/v1/ingest_file",
                                       raw=b"# a skill\n",
                                       headers=dict(ADMIN, **{"X-Filename": "checkout.md"}))
        self.assertEqual(502, status)
        payload = json.loads(body)
        self.assertEqual("blob_store_unreachable", payload["error"])
        self.assertIn("ConnectionRefusedError", payload["detail"])
        # Nothing was ingested: a pointer to bytes that were never stored is worse than a failure.
        self.assertEqual([], [n for n, _a in server.calls if n == "matrixark_ingest"])

    def test_the_spool_file_is_removed_even_when_the_blob_tier_fails(self) -> None:
        # The spool is a full copy of the upload on local disk; leaking one per failed upload is
        # how a disk fills during an outage, which then looks like a different fault entirely.
        import glob
        import tempfile

        with tempfile.TemporaryDirectory() as spool_dir:
            def refuse(_cfg):
                raise ConnectionRefusedError(111, "Connection refused")

            app = gw.make_v1_app(_FakeServer(),
                                 _cfg(blob_connection_factory=refuse, ingest_spool_dir=spool_dir))
            drive(app, method="POST", path="/v1/ingest_file", raw=b"# a skill\n",
                  headers=dict(ADMIN, **{"X-Filename": "checkout.md"}))
            self.assertEqual([], glob.glob(os.path.join(spool_dir, "*")))


if __name__ == "__main__":
    unittest.main()
