#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Revoking a key is authorized, and rotating one cannot destroy it.

Three faults in the two endpoints behind the key portal's Rotate and Revoke buttons, each
reproduced against a server in enforced mode before the fix.

**Revocation was not authorized.** Creating a key calls `ensure_identity_can_manage`, which
requires the caller's account and tenant to match. Revoking one did not: it found the record by id,
saw it was active, and revoked it. An admin key for one tenant could revoke another tenant's key.
Listing is fenced, so the id must come from elsewhere -- an audit line, a screenshot, a support
ticket -- but authorization cannot rest on an identifier being awkward to obtain.

**A refused rotation destroyed the key anyway.** Rotation revoked first and only met an
authorization check inside the create half, so rotating another tenant's key raised "account/tenant
does not match" *after* the revocation had landed. The caller was told no; the key was gone. The
replacement is now minted first, so no failure in either half can leave the caller with no key.

**Rotation changed the key's prefix.** The prefix was never stored, so rotation fell back to its
"mk_test" default and a key created as `sk_live_...` came back as `mk_test_...`.

Every check that expects a refusal is paired with the same caller doing the same thing to its own
tenant, because a test that only ever sees "refused" passes just as well when the setup is broken.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError

ADMIN_SCOPES = ["admin:account", "admin:user", "admin:api_key", "admin:audit", "portal:read"]
A = {"account_id": "acct_a", "tenant_id": "tenant_a"}
B = {"account_id": "acct_b", "tenant_id": "tenant_b"}


class KeyManagementIsFencedByTenantTest(unittest.TestCase):

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = Path(tmp.name) / "events.jsonl"

        # Enforced mode has no way to mint the first key, so the fixtures are made in dev mode and
        # the log is reopened enforced.
        dev = self._server("dev")
        self.admin_a = dev.call_tool("matrixark_admin_create_api_key",
                                     {"scope": A, **A, "role": "owner", "scopes": ADMIN_SCOPES})
        self.own_a = self._service(dev, A)
        self.victim_b = self._service(dev, B)
        dev.close(timeout_s=10.0)

        self.server = self._server("enforced")

    def _server(self, mode: str) -> MatrixArkMcpServer:
        server = MatrixArkMcpServer(MatrixArkLocalAdapter(self.log), line_json=True,
                                    access_mode=mode)
        self.addCleanup(server.close, timeout_s=10.0)
        return server

    @staticmethod
    def _service(server, scope, prefix="sk_live"):
        return server.call_tool("matrixark_admin_create_api_key",
                                {"scope": scope, **scope, "role": "service",
                                 "key_prefix": prefix, "scopes": ["context:ingest"]})

    def _as_admin_a(self, tool, args):
        return self.server.call_tool(tool, dict(args, api_key=self.admin_a["api_key"]))

    def _status(self, api_key_id: str) -> str:
        record = self._server("dev").access.latest_api_key_record(api_key_id)
        return (record or {}).get("status", "absent")

    # ---- revoke ---------------------------------------------------------------------------

    def test_an_admin_may_revoke_a_key_in_its_own_tenant(self) -> None:
        """The control. Without it, the refusal below could mean the setup was simply wrong."""
        result = self._as_admin_a("matrixark_admin_revoke_api_key",
                                  {"api_key_id": self.own_a["api_key_id"]})
        self.assertEqual("revoked", result["status"])
        self.assertEqual("revoked", self._status(self.own_a["api_key_id"]))

    def test_an_admin_may_not_revoke_another_tenants_key(self) -> None:
        with self.assertRaises(MatrixArkError):
            self._as_admin_a("matrixark_admin_revoke_api_key",
                             {"api_key_id": self.victim_b["api_key_id"]})
        self.assertEqual("active", self._status(self.victim_b["api_key_id"]),
                         "the key was revoked by an admin with no authority over its tenant")

    # ---- rotate ---------------------------------------------------------------------------

    def test_an_admin_may_rotate_a_key_in_its_own_tenant(self) -> None:
        rotated = self._as_admin_a("matrixark_admin_rotate_api_key",
                                   {"api_key_id": self.own_a["api_key_id"]})
        self.assertEqual("rotated", rotated["status"])
        self.assertTrue(rotated.get("api_key"))
        self.assertEqual("revoked", self._status(self.own_a["api_key_id"]))
        self.assertEqual("active", self._status(rotated["api_key_id"]))

    def test_a_refused_rotation_leaves_the_key_alone(self) -> None:
        """The old order revoked first, so this raised and destroyed the key at the same time."""
        with self.assertRaises(MatrixArkError):
            self._as_admin_a("matrixark_admin_rotate_api_key",
                             {"api_key_id": self.victim_b["api_key_id"]})
        self.assertEqual("active", self._status(self.victim_b["api_key_id"]),
                         "a rotation that was refused still revoked the key it could not touch")


class RotationKeepsTheKeysPrefixTest(unittest.TestCase):

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.server = MatrixArkMcpServer(
            MatrixArkLocalAdapter(Path(tmp.name) / "events.jsonl"),
            line_json=True, access_mode="dev")
        self.addCleanup(self.server.close, timeout_s=10.0)
        self.scope = {"account_id": "acct_p", "tenant_id": "tenant_p"}

    def _create(self, prefix):
        return self.server.call_tool(
            "matrixark_admin_create_api_key",
            {"scope": self.scope, **self.scope, "role": "service", "key_prefix": prefix,
             "scopes": ["context:ingest"]})

    def test_the_replacement_carries_the_same_prefix(self) -> None:
        for prefix in ("sk_live", "mk_test", "sk_staging"):
            made = self._create(prefix)
            self.assertTrue(made["api_key"].startswith(prefix + "_"), made["api_key"][:12])
            rotated = self.server.call_tool("matrixark_admin_rotate_api_key",
                                            {"api_key_id": made["api_key_id"]})
            self.assertTrue(rotated["api_key"].startswith(prefix + "_"),
                            "%s rotated into %s..." % (prefix, rotated["api_key"][:14]))

    def test_an_explicit_prefix_still_wins(self) -> None:
        made = self._create("sk_live")
        rotated = self.server.call_tool(
            "matrixark_admin_rotate_api_key",
            {"api_key_id": made["api_key_id"], "key_prefix": "sk_next"})
        self.assertTrue(rotated["api_key"].startswith("sk_next_"), rotated["api_key"][:14])

    def test_a_record_written_before_the_prefix_was_stored_still_rotates(self) -> None:
        """Older records have no prefix to carry, so they keep the previous default."""
        made = self._create("sk_live")
        record = self.server.access.latest_api_key_record(made["api_key_id"])
        legacy = {key: value for key, value in record.items() if key != "key_prefix"}
        self.server.access.metadata.append(legacy)
        self.assertNotIn("key_prefix",
                         self.server.access.latest_api_key_record(made["api_key_id"]))

        rotated = self.server.call_tool("matrixark_admin_rotate_api_key",
                                        {"api_key_id": made["api_key_id"]})
        self.assertTrue(rotated["api_key"].startswith("mk_test_"), rotated["api_key"][:14])

    def test_the_prefix_is_recorded_in_the_first_place(self) -> None:
        made = self._create("sk_live")
        record = self.server.access.latest_api_key_record(made["api_key_id"])
        self.assertEqual("sk_live", record.get("key_prefix"))

    def test_the_record_still_holds_no_plaintext_key(self) -> None:
        """Storing the prefix must not have widened what a key record keeps."""
        made = self._create("sk_live")
        record = self.server.access.latest_api_key_record(made["api_key_id"])
        self.assertIn("api_key_hash", record)
        self.assertNotIn("api_key", record)
        self.assertNotIn(made["api_key"], str(record))


if __name__ == "__main__":
    unittest.main()
