#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`session_scope=prefer` is the retrieval default, and it does nothing for a packed scope key.

`scope_matches` decides whether a stored record is visible to a query. When the record's access
scope carries a packed `scope_key` it hands off to `scope_key_matches_query` and returns on a
miss:

    record_scope_key = str(record_scope.get("scope_key") or "")
    if record_scope_key:
        if not scope_key_matches_query(record_scope_key, query_scope, explicit_keys):
            return False            # <- before the loop below
        ...
    for key, value in query_scope.items():
        ...
        if key in {"session_id", "session_hash"}:
            if "session_id" not in explicit_keys or session_scope_mode(query_scope) == "prefer":
                continue            # <- the only place `prefer` is honoured

So `prefer` is honoured in the field-by-field loop and nowhere else, and any record with a packed
key never reaches it. Measured, same record written in session 333 and queried from session 999:

    record access scope             prefer    only
    scope_key only (compacted)      False     False
    scope_key + scope fields        False     False
    no scope_key, fields only       True      False

`prefer` differs from `only` in exactly one row -- the record shape that carries no packed key.

THE SAME FUNCTION HAS A SECOND DEFINITION THAT DOES IMPLEMENT IT. `matrixark_mcp_core_identity`'s
copy is byte-for-byte the live one plus:

    if session_scope_mode(query_scope) == "prefer":
        return True

and it returns True on the inputs where the live copy returns False. `matrixark_mcp_access_scope`
binds `matrixark_mcp_identity`'s copy -- the one without it.

WHY THIS MATTERS ON A LIVE PATH. `retrieval_session_scope` defaults to `"prefer"` in
`matrixark_mcp_retrieve_planning`, `matrixark_local_adapter_retrieve` and
`matrixark_temporal_direct_read`; each builds `{**scope, "_session_scope": retrieval_session_scope}`
and passes it to `scope_matches` through `recovered_scope_matches`. And a compacted record's
`candidate_access_scope` is exactly `{"scope_key": ...}`, because `compact_record_scope` sets the
key and pops `scope`. So the default cross-session mode cannot match a compacted record.

THIS FILE DOES NOT ASSERT THAT `prefer` WORKS, because it does not. It RECORDS the behaviour and
the divergence in BOTH directions, so a new divergence fails here and a fix fails here too. It is
deliberately not a fix: adding the branch to the live copy WIDENS which records a query can see,
which is an access-scope decision and not a cleanup.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_access_scope as access_scope_module
import matrixark_mcp_core_compact as compact_module
import matrixark_mcp_core_identity as core_identity_module
import matrixark_mcp_identity as identity_module

RECORD_SESSION_HASH = 333
QUERY_SESSION_HASH = 999
TENANT_HASH, USER_HASH, AGENT_HASH = 111, 222, 444

EXPLICIT = ["tenant_id", "user_id", "session_id"]


def _record_key():
    return identity_module.scope_key_from_hashes(
        TENANT_HASH, USER_HASH, RECORD_SESSION_HASH, AGENT_HASH)


def _query(mode):
    return {"tenant_hash": TENANT_HASH, "user_hash": USER_HASH,
            "session_hash": QUERY_SESSION_HASH, "agent_hash": AGENT_HASH,
            "_session_scope": mode, "_explicit_scope_keys": list(EXPLICIT)}


def _record_scopes():
    key = _record_key()
    fields = {"tenant_hash": TENANT_HASH, "user_hash": USER_HASH,
              "session_hash": RECORD_SESSION_HASH, "agent_hash": AGENT_HASH}
    return {
        "scope_key only": {"scope_key": key},
        "scope_key and fields": {"scope_key": key, **fields},
        "no scope_key": dict(fields),
    }


class PreferModeCannotMatchAPackedScopeKey(unittest.TestCase):

    def test_the_fixture_really_asks_for_prefer(self) -> None:
        """A floor on the FIXTURE.

        session_scope_mode falls back to "only" for anything it does not recognise, so a typo in
        the mode would make every row below agree for the most boring possible reason.
        """
        self.assertEqual("prefer", identity_module.session_scope_mode(_query("prefer")))
        self.assertEqual("only", identity_module.session_scope_mode(_query("only")))
        self.assertNotEqual(
            RECORD_SESSION_HASH, QUERY_SESSION_HASH,
            "the record and the query must be in different sessions or there is nothing to match")

    def test_prefer_changes_the_answer_only_when_there_is_no_packed_key(self) -> None:
        """The measured table, recorded exactly."""
        expected = {
            "scope_key only": (False, False),
            "scope_key and fields": (False, False),
            "no scope_key": (True, False),
        }
        for label, record_scope in _record_scopes().items():
            with self.subTest(record_scope=label):
                got = (access_scope_module.scope_matches(record_scope, _query("prefer")),
                       access_scope_module.scope_matches(record_scope, _query("only")))
                self.assertEqual(
                    expected[label], got,
                    "scope_matches changed for a %s record: prefer/only is now %s, recorded as %s. "
                    "If prefer now works for a packed key, that is the fix -- strike this file and "
                    "say so." % (label, got, expected[label]))

    def test_the_two_packed_key_matchers_disagree(self) -> None:
        """Recorded, both directions. The live copy lacks the branch its sibling has."""
        key, explicit = _record_key(), set(EXPLICIT)
        live = identity_module.scope_key_matches_query(key, _query("prefer"), explicit)
        other = core_identity_module.scope_key_matches_query(key, _query("prefer"), explicit)

        self.assertFalse(
            live,
            "matrixark_mcp_identity.scope_key_matches_query now honours prefer. That WIDENS which "
            "records a query can see; strike this file and record the decision.")
        self.assertTrue(
            other,
            "matrixark_mcp_core_identity.scope_key_matches_query stopped honouring prefer, so the "
            "two copies now agree -- the split is resolved in the other direction.")

    def test_access_scope_binds_the_copy_without_the_branch(self) -> None:
        """Which copy is live is the whole point; assert it rather than trusting the import."""
        bound = access_scope_module.scope_key_matches_query
        # Compared on the last segment: the same file is imported both as `matrixark_mcp_identity`
        # and as `tools.matrixark_mcp_identity` depending on how the suite is invoked, and those
        # are different module objects with different __module__ strings.
        self.assertEqual(
            "matrixark_mcp_identity", bound.__module__.rsplit(".", 1)[-1],
            "matrixark_mcp_access_scope now binds %s's copy. If that is core_identity's, prefer "
            "just started working on the retrieval path." % bound.__module__)

    def test_a_compacted_record_presents_only_its_packed_key(self) -> None:
        """The link that makes the rows above reachable from a real record.

        If a compacted record's access scope ever carries the scope fields too, it lands on the
        second row rather than the first -- which this table shows makes no difference, but the
        reasoning above would need restating.
        """
        record = compact_module.compact_record_scope(
            {"record_type": "context_event", "event_id_hash": 4242,
             "scope": {"tenant_hash": TENANT_HASH, "user_hash": USER_HASH,
                       "session_hash": RECORD_SESSION_HASH, "agent_hash": AGENT_HASH,
                       "session_id": "sess-abc"},
             "payload": "hello"})
        candidate = access_scope_module.candidate_access_scope(record)

        self.assertEqual(
            ["scope_key"], sorted(candidate),
            "a compacted context_event no longer presents only its packed key; its access scope is "
            "%s" % sorted(candidate))
        self.assertFalse(
            access_scope_module.scope_matches(candidate, _query("prefer")),
            "a compacted record is now visible to a cross-session prefer query")


if __name__ == "__main__":
    unittest.main()
