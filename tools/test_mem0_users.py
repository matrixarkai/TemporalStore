#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0 `users()`: the users / agents / runs that hold memories.

The part worth pinning is the re-scoping. A request scope carries identity fields DERIVED from
the caller -- `user_hash`, `session_hash`, `scope_key` -- and `get_all` filters on those hashes,
never on `user_id`. Both naive re-scopings are wrong, and both were observed against a live
gateway before this was fixed:

  * swap only `user_id`     -> the caller's `user_hash` stays, the lookup resolves back to the
                               caller, and every user reports the same memory count (3 and 3 for
                               users who had 2 and 1)
  * drop the hashes instead -> `user_hash == 0` reads as "no subject filter" and returns the whole
                               tenant, so a user whose memories were all forgotten never drops out

The subject's hashes have to be recomputed by the same function the ingest path uses.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools.matrixark_mcp_core_identity import identity_hashes
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    from matrixark_mcp_core_identity import identity_hashes


CALLER_SCOPE = {
    "account_id": "acct_local",
    "tenant_id": "anonymous",
    "user_id": "root",
    "agent_name": "local_agent",
    "session_id": "caller-session",
    **identity_hashes("acct_local", "anonymous", user_id="root", session_id="caller-session"),
    "_explicit_scope_keys": ["account_id", "tenant_id", "user_id"],
}


class SubjectRescopeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scope = adapters.MatrixArkTemporalStoreDirectAdapter._subject_scope(CALLER_SCOPE, "alice")

    def test_names_the_subject(self) -> None:
        self.assertEqual("alice", self.scope["user_id"])

    def test_uses_the_subjects_own_hashes(self) -> None:
        expected = identity_hashes("acct_local", "anonymous", user_id="alice")
        self.assertEqual(expected["user_hash"], self.scope["user_hash"])
        self.assertEqual(expected["scope_key"], self.scope["scope_key"])

    def test_does_not_keep_the_callers_identity(self) -> None:
        """The bug that made every user report the caller's memory count."""
        self.assertNotEqual(CALLER_SCOPE["user_hash"], self.scope["user_hash"])
        self.assertNotEqual(CALLER_SCOPE["scope_key"], self.scope["scope_key"])

    def test_user_hash_is_never_zero(self) -> None:
        """A zero hash reads as 'no subject filter' and returns the whole tenant."""
        self.assertTrue(self.scope["user_hash"])

    def test_drops_the_callers_session(self) -> None:
        """users() asks about a user, not about the caller's session."""
        self.assertNotIn("session_id", self.scope)
        self.assertFalse(self.scope.get("session_hash"))

    def test_keeps_the_tenant(self) -> None:
        """Re-scoping must stay inside the caller's tenant -- it is not a cross-tenant listing."""
        self.assertEqual("anonymous", self.scope["tenant_id"])
        self.assertEqual("acct_local", self.scope["account_id"])
        self.assertEqual(CALLER_SCOPE["tenant_hash"], self.scope["tenant_hash"])


class MemorySubjectExtractionTest(unittest.TestCase):
    def test_extracts_user_agent_and_run(self) -> None:
        record = {"scope": {"user_id": "alice", "agent_id": "assistant", "session_id": "s1"}}
        found = adapters.MatrixArkTemporalStoreDirectAdapter.memory_subjects_in_record(record)
        self.assertEqual(
            [("user", "alice"), ("agent", "assistant"), ("run", "s1")],
            found,
        )

    def test_ignores_blank_and_missing_identities(self) -> None:
        cls = adapters.MatrixArkTemporalStoreDirectAdapter
        self.assertEqual([], cls.memory_subjects_in_record({"scope": {"user_id": "  "}}))
        self.assertEqual([], cls.memory_subjects_in_record({"scope": {}}))
        self.assertEqual([], cls.memory_subjects_in_record({}))
        self.assertEqual([], cls.memory_subjects_in_record({"scope": "not-a-dict"}))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
