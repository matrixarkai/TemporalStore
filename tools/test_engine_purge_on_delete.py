#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""forget and reset must reach the ENGINE, not only the serving view.

On a native backend `/v1/retrieve` does not go through the Python serving pipeline at all: the
engine assembles the context pack itself, and it has no idea a memory tombstone exists. So a
tombstone-only forget left `get_all` reporting a clean wipe while retrieve went on serving the
subject's memories verbatim:

    before forget:  get_all=2, retrieve contains the subject's secret = True
    forget:         http 200, removed_count 54
    after forget:   get_all=0, retrieve contains the secret = STILL True

Deleting data has to mean deleting it on every read path. The engine has had
`matrixark_forget_scope` all along -- it removes the subject's records, refuses an
under-specified scope, and clears its own scan caches -- and nothing called it.

These pin the wiring and, more importantly, the scope handed to it: too wide and a forget wipes
somebody else.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_temporal_adapters as adapters
    from tools.matrixark_mcp_core_identity import identity_hashes
except ImportError:  # run from tools/ dir
    import matrixark_mcp_temporal_adapters as adapters
    from matrixark_mcp_core_identity import identity_hashes


class _Client:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def matrixark_forget_scope(self, *, count_key, record_hash_key, shard_size, scope):
        self.calls.append({"count_key": count_key, "record_hash_key": record_hash_key,
                           "shard_size": shard_size, "scope": scope})
        if self.fail:
            raise RuntimeError("engine unavailable")
        return {"matrixark_forget_records_removed": 7, "matrixark_forget_fields_deleted": 2,
                "matrixark_forget_fields_rewritten": 1, "matrixark_forget_shards_scanned": 3}


def _adapter(client, base_result=None):
    a = object.__new__(adapters.MatrixArkTemporalStoreDirectAdapter)
    a._client = client
    a._count_key = "matrixark:mcp:record_count"
    a._record_hash_key = "matrixark:mcp:records"
    a._shard_size = 1024
    a._drop_direct_record_cache = lambda: None  # type: ignore[assignment]
    a._resolve_subject_hashes = lambda scope: (
        int(scope.get("tenant_hash") or 0), int(scope.get("user_hash") or 0))
    return a


SCOPE = {"account_id": "acct", "tenant_id": "t1", "user_id": "alice",
         **identity_hashes("acct", "t1", user_id="alice")}


class ForgetReachesEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self._forget = adapters.MatrixArkLocalAdapter.forget
        self._reset = adapters.MatrixArkLocalAdapter.reset
        adapters.MatrixArkLocalAdapter.forget = lambda s, a, h=None: {"status": "ok", "removed_count": 2}
        adapters.MatrixArkLocalAdapter.reset = lambda s, a, h=None: {"status": "ok", "removed_count": 5}
        self.addCleanup(lambda: setattr(adapters.MatrixArkLocalAdapter, "forget", self._forget))
        self.addCleanup(lambda: setattr(adapters.MatrixArkLocalAdapter, "reset", self._reset))

    def test_forget_purges_the_subject_in_the_engine(self) -> None:
        client = _Client()
        out = _adapter(client).forget({"scope": dict(SCOPE), "confirm": "alice"})
        self.assertEqual(1, len(client.calls), "the engine purge must be issued")
        self.assertTrue(out["engine_purge"]["ok"])
        self.assertEqual(7, out["engine_purge"]["records_removed"])

    def test_forget_hands_the_engine_the_subjects_own_hashes(self) -> None:
        """A scope carrying the CALLER's user_hash would purge the wrong subject."""
        caller = dict(SCOPE)
        caller.update(identity_hashes("acct", "t1", user_id="someone_else"))
        caller["user_id"] = "alice"
        client = _Client()
        _adapter(client).forget({"scope": caller, "confirm": "alice"})
        sent = client.calls[0]["scope"]
        self.assertEqual(identity_hashes("acct", "t1", user_id="alice")["user_hash"],
                         sent["user_hash"])

    def test_forget_keeps_the_subject_dimension(self) -> None:
        """Dropping user_id would widen a forget into a tenant wipe."""
        client = _Client()
        _adapter(client).forget({"scope": dict(SCOPE), "confirm": "alice"})
        self.assertEqual("alice", client.calls[0]["scope"]["user_id"])

    def test_an_engine_failure_is_reported_not_swallowed(self) -> None:
        """The tombstone still stands, so the serving view is right -- but the caller must not be
        told the data is gone from the engine when it is not."""
        client = _Client(fail=True)
        out = _adapter(client).forget({"scope": dict(SCOPE), "confirm": "alice"})
        self.assertFalse(out["engine_purge"]["ok"])
        self.assertIn("engine unavailable", out["engine_purge"]["error"])

    def test_a_client_without_the_op_degrades_rather_than_raising(self) -> None:
        class _Old:
            pass
        out = _adapter(_Old()).forget({"scope": dict(SCOPE), "confirm": "alice"})
        self.assertFalse(out["engine_purge"]["ok"])


class ResetReachesEngineTest(ForgetReachesEngineTest):
    def test_reset_purges_the_whole_tenant(self) -> None:
        client = _Client()
        out = _adapter(client).reset({"scope": dict(SCOPE), "confirm": "RESET"})
        self.assertEqual(1, len(client.calls))
        self.assertTrue(out["engine_purge"]["ok"])

    def test_reset_drops_the_user_dimension_but_keeps_the_tenant(self) -> None:
        """Tenant-wide is the point; still carrying user_id would reset only one user, and
        dropping the tenant hash would match everything."""
        client = _Client()
        _adapter(client).reset({"scope": dict(SCOPE), "confirm": "RESET"})
        sent = client.calls[0]["scope"]
        self.assertNotIn("user_id", sent)
        self.assertNotIn("user_hash", sent)
        self.assertEqual(SCOPE["tenant_hash"], sent["tenant_hash"])

    def test_reset_without_a_resolvable_tenant_does_not_purge(self) -> None:
        """No tenant hash means no safe scope to hand the engine -- purging anyway could match
        every record in the store."""
        client = _Client()
        out = _adapter(client).reset({"scope": {"user_id": "alice"}, "confirm": "RESET"})
        self.assertEqual([], client.calls)
        self.assertNotIn("engine_purge", out)


class _DeleteClient:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def matrixark_delete_records(self, *, count_key, record_hash_key, shard_size, record_ids):
        self.calls.append({"record_ids": record_ids, "shard_size": shard_size})
        if self.fail:
            raise RuntimeError("engine unavailable")
        return {"matrixark_delete_records_removed": 4, "matrixark_delete_fields_deleted": 1,
                "matrixark_delete_fields_rewritten": 2, "matrixark_delete_ids_requested": len(record_ids)}


class DeleteReachesEngineTest(unittest.TestCase):
    """`delete` had the same hole as forget: get_all dropped while retrieve still served it.

    The identity set is NOT re-derived here. Which records a delete covers is subtle -- the
    addressed event, its single-source derivatives, and the embeddings/postings pointing at any of
    them, while MULTI-source derivatives are demoted rather than removed -- so the inherited
    implementation decides it once and this passes that set through.
    """

    def setUp(self) -> None:
        self._orig = adapters.MatrixArkLocalAdapter.delete_memory
        self.addCleanup(lambda: setattr(adapters.MatrixArkLocalAdapter, "delete_memory", self._orig))

    def _with_result(self, result):
        adapters.MatrixArkLocalAdapter.delete_memory = lambda s, a, h=None: dict(result)

    def test_passes_the_closure_through_to_the_engine(self) -> None:
        self._with_result({"deleted": True, "closure_ref_ids": ["11", "22", "33"]})
        client = _DeleteClient()
        out = _adapter(client).delete_memory({"memory_id": "11"})
        self.assertEqual(["11", "22", "33"], client.calls[0]["record_ids"])
        self.assertTrue(out["engine_purge"]["ok"])
        self.assertEqual(4, out["engine_purge"]["records_removed"])

    def test_does_not_re_derive_the_closure(self) -> None:
        """Whatever the inherited implementation decided is what the engine is told -- two copies
        of that rule in two languages is how they drift apart."""
        self._with_result({"deleted": True, "closure_ref_ids": ["only-this-one"]})
        client = _DeleteClient()
        _adapter(client).delete_memory({"memory_id": "something-else"})
        self.assertEqual(["only-this-one"], client.calls[0]["record_ids"])

    def test_an_empty_closure_purges_nothing(self) -> None:
        """A delete that matched nothing must not turn into a wildcard removal."""
        self._with_result({"deleted": False, "closure_ref_ids": []})
        client = _DeleteClient()
        out = _adapter(client).delete_memory({"memory_id": "nope"})
        self.assertEqual([], client.calls)
        self.assertNotIn("engine_purge", out)

    def test_an_engine_failure_is_reported_not_swallowed(self) -> None:
        self._with_result({"deleted": True, "closure_ref_ids": ["11"]})
        out = _adapter(_DeleteClient(fail=True)).delete_memory({"memory_id": "11"})
        self.assertFalse(out["engine_purge"]["ok"])
        self.assertIn("engine unavailable", out["engine_purge"]["error"])

    def test_a_client_without_the_op_degrades_rather_than_raising(self) -> None:
        class _Old:
            pass
        self._with_result({"deleted": True, "closure_ref_ids": ["11"]})
        out = _adapter(_Old()).delete_memory({"memory_id": "11"})
        self.assertFalse(out["engine_purge"]["ok"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
