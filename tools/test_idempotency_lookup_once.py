#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""One idempotency lookup per dispatch, not two.

`_idempotent_replay_response` looks the key up at the start of a dispatch; if it finds a record the
call ends there with a replay. So by the time `_finalize_write_response` runs, that same key has
already been proven absent -- and it used to look it up again. An ingest that finalizes is two
dispatches, so that was two redundant lookups per ingest on the request thread.

The risk in skipping a check is skipping it when it was never actually made, so that is what most
of these cover: the note is per key and per call, and anything that did not go through the replay
path still looks.
"""
from __future__ import annotations

import unittest

try:
    from tools import matrixark_mcp_server_request_policy as policy
except ImportError:  # run from tools/ dir
    import matrixark_mcp_server_request_policy as policy


class _Adapter:
    def __init__(self, record=None):
        self.record = record
        self.lookups = 0
        self.appended = []

    def find_idempotency_record(self, key_hash):
        self.lookups += 1
        return self.record

    def append_idempotency_record(self, **kw):
        self.appended.append(kw)


class _Access:
    def append_audit(self, *a, **kw):
        pass


class _Policy(policy.MatrixArkServerRequestPolicyMixin):
    IDEMPOTENT_WRITE_TOOLS = {"matrixark_ingest"}

    def _raw_idempotency_key(self, args, hook):
        return str(args.get("key") or "")

    def _idempotency_key_hash(self, name, raw_key, identity):
        return hash((name, raw_key))


def _policy(record=None):
    p = object.__new__(_Policy)
    p.adapter = _Adapter(record)
    p.access = _Access()
    return p


IDENT = {"tenant_id": "t"}


class IdempotencyLookedUpOncePerCallTest(unittest.TestCase):
    def test_finalize_does_not_look_up_again_after_replay_found_nothing(self) -> None:
        p = _policy(record=None)
        args = {"key": "k1"}
        self.assertIsNone(p._idempotent_replay_response("matrixark_ingest", args, IDENT, None))
        self.assertEqual(1, p.adapter.lookups)
        p._finalize_write_response("matrixark_ingest", args, IDENT, None, {"ok": True})
        self.assertEqual(1, p.adapter.lookups, "the same key must not be looked up twice in a call")
        self.assertEqual(1, len(p.adapter.appended), "and the record must still be stored")

    def test_finalize_still_looks_when_replay_never_ran(self) -> None:
        """A tool that finalizes without going through the replay path has proven nothing."""
        p = _policy(record=None)
        p._finalize_write_response("matrixark_ingest", {"key": "k1"}, IDENT, None, {"ok": True})
        self.assertEqual(1, p.adapter.lookups)

    def test_the_note_is_per_key(self) -> None:
        """Proving key A absent says nothing about key B."""
        p = _policy(record=None)
        args = {"key": "k1"}
        p._idempotent_replay_response("matrixark_ingest", args, IDENT, None)
        self.assertEqual(1, p.adapter.lookups)
        args["key"] = "k2"          # same call object, different key
        p._finalize_write_response("matrixark_ingest", args, IDENT, None, {"ok": True})
        self.assertEqual(2, p.adapter.lookups, "a different key must be looked up")

    def test_a_replay_hit_short_circuits_and_never_reaches_finalize(self) -> None:
        p = _policy(record={"response": {"replayed": True}})
        out = p._idempotent_replay_response("matrixark_ingest", {"key": "k1"}, IDENT, None)
        self.assertIsNotNone(out)
        self.assertTrue(out.get("idempotent_replay"))
        self.assertEqual([], p.adapter.appended)

    def test_a_call_without_a_key_is_untouched(self) -> None:
        p = _policy(record=None)
        args = {}
        self.assertIsNone(p._idempotent_replay_response("matrixark_ingest", args, IDENT, None))
        self.assertEqual(0, p.adapter.lookups)
        self.assertNotIn("_matrixark_idempotency_absent", args)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
