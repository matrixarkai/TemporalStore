#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A refusal at the tenant boundary is recorded.

The audit trail covered two kinds of denial and not the third. `append_denied_audit` handles the
ones that happen before an identity is known -- an unknown key, a missing scope, a failed login --
and records `api_key_id: "unknown"` because at that point there is nothing better to say.

The tenant boundary is the interesting one and it wrote nothing. Authentication has passed, the
scope check has passed, and what is being refused is a valid admin key reaching for a tenant it
does not hold. An operator saw their own successful revocations and saw nothing at all when
somebody else's admin key tried the same against theirs.

The record now goes through `append_audit`, because here the identity is known: it carries the
caller's own api_key_id, role, account and tenant, and the details carry what they asked for
instead. Both halves are needed -- who, and against whom.

Auditing is off by default and stays that way. A deployment recording nothing today records
nothing after this; the hole is filled for the deployments that do record.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError

ADMIN_SCOPES = ["admin:account", "admin:user", "admin:api_key", "admin:audit", "portal:read"]
A = {"account_id": "acct_a", "tenant_id": "tenant_a"}
B = {"account_id": "acct_b", "tenant_id": "tenant_b"}


class ARefusedAdminOperationIsRecordedTest(unittest.TestCase):

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = Path(tmp.name) / "events.jsonl"

        # Process-global, so it is restored however this test ends.
        previous = os.environ.get("MATRIXARK_AUDIT_MODE")
        os.environ["MATRIXARK_AUDIT_MODE"] = "async"

        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_AUDIT_MODE", None)
            else:
                os.environ["MATRIXARK_AUDIT_MODE"] = previous

        self.addCleanup(restore)

        dev = self._server("dev")
        self.admin_a = dev.call_tool("matrixark_admin_create_api_key",
                                     {"scope": A, **A, "role": "owner", "scopes": ADMIN_SCOPES})
        self.own_a = self._service(dev, A)
        self.victim_b = self._service(dev, B)
        dev.close(timeout_s=10.0)

    def _server(self, mode: str) -> MatrixArkMcpServer:
        return MatrixArkMcpServer(MatrixArkLocalAdapter(self.log), line_json=True,
                                  access_mode=mode)

    @staticmethod
    def _service(server, scope):
        return server.call_tool("matrixark_admin_create_api_key",
                                {"scope": scope, **scope, "role": "service",
                                 "key_prefix": "sk_live", "scopes": ["context:ingest"]})

    def _attempt(self, tool, args, expect_refusal: bool) -> list:
        """Run one call as tenant A's admin, then read the audit records it produced.

        The server is closed before reading: audit writes are buffered, and a test that reads
        through the same handle can see an empty log and call it a missing record.
        """
        server = self._server("enforced")
        try:
            if expect_refusal:
                with self.assertRaises(MatrixArkError):
                    server.call_tool(tool, dict(args, api_key=self.admin_a["api_key"]))
            else:
                server.call_tool(tool, dict(args, api_key=self.admin_a["api_key"]))
        finally:
            server.close(timeout_s=10.0)

        reader = MatrixArkLocalAdapter(self.log)
        return [r for r in reader.read_all() if r.get("record_type") == "matrixark_audit_log"]

    # ---- the refusal ------------------------------------------------------------------------

    def test_a_cross_tenant_revocation_is_recorded_as_denied(self) -> None:
        audits = self._attempt("matrixark_admin_revoke_api_key",
                               {"api_key_id": self.victim_b["api_key_id"]}, True)
        denied = [r for r in audits if r.get("status") == "denied"]
        self.assertEqual(1, len(denied), "expected exactly one denial, got %r"
                         % [(r.get("action"), r.get("status")) for r in audits])
        record = denied[0]
        self.assertEqual("admin.revoke_api_key", record["action"])

    def test_the_record_says_who_asked_and_against_whom(self) -> None:
        """Either half alone is useless: the caller without the target, or the target without the
        caller, does not describe what happened."""
        audits = self._attempt("matrixark_admin_revoke_api_key",
                               {"api_key_id": self.victim_b["api_key_id"]}, True)
        record = [r for r in audits if r.get("status") == "denied"][0]
        self.assertEqual(self.admin_a["api_key_id"], record["api_key_id"])
        self.assertEqual("tenant_a", record["tenant_id"])
        self.assertEqual("tenant_b", record["details"]["requested_tenant_id"])
        self.assertEqual("acct_b", record["details"]["requested_account_id"])

    def test_a_refused_rotation_is_attributed_to_the_rotation(self) -> None:
        """Rotation refuses through the same guard. Recording it as a bare revoke would describe
        an operation nobody attempted."""
        audits = self._attempt("matrixark_admin_rotate_api_key",
                               {"api_key_id": self.victim_b["api_key_id"]}, True)
        denied = [r for r in audits if r.get("status") == "denied"]
        self.assertEqual(["admin.rotate_api_key"], [r["action"] for r in denied])

    def test_no_plaintext_key_reaches_the_record(self) -> None:
        audits = self._attempt("matrixark_admin_revoke_api_key",
                               {"api_key_id": self.victim_b["api_key_id"]}, True)
        blob = str(audits)
        self.assertNotIn(self.admin_a["api_key"], blob)
        self.assertNotIn(self.victim_b["api_key"], blob)

    # ---- the controls -----------------------------------------------------------------------

    def test_an_allowed_revocation_is_not_recorded_as_denied(self) -> None:
        """Without this, a change that marked everything denied would pass the checks above."""
        audits = self._attempt("matrixark_admin_revoke_api_key",
                               {"api_key_id": self.own_a["api_key_id"]}, False)
        self.assertEqual([], [r for r in audits if r.get("status") == "denied"])
        self.assertIn("admin.revoke_api_key",
                      [r.get("action") for r in audits if r.get("status") == "ok"])

    def test_auditing_stays_off_by_default(self) -> None:
        """The default posture is unchanged: a deployment that records nothing still records
        nothing, denials included."""
        os.environ["MATRIXARK_AUDIT_MODE"] = "off"
        audits = self._attempt("matrixark_admin_revoke_api_key",
                               {"api_key_id": self.victim_b["api_key_id"]}, True)
        # setUp built its fixtures with auditing on, so its records are in the log and this is
        # reading the right one -- which is what stops the check below passing over an empty file.
        self.assertTrue([r for r in audits if r.get("status") == "ok"],
                        "no records at all; this is not reading the log it thinks it is")
        self.assertEqual([], [r for r in audits if r.get("status") == "denied"],
                         "auditing is off, yet the refusal was recorded anyway")


if __name__ == "__main__":
    unittest.main()
