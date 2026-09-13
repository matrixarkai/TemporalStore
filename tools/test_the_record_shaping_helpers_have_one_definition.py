#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The record-shaping pipeline is defined twice, and the compact copy writes a poorer record.

`materialize_serving_records` runs each record through

    compact_storage_record(attach_context_event_time_key(attach_storage_route(record)))
    compact_record_lifecycle_fields(compact_record_scope(record))

and those helpers exist in BOTH `matrixark_mcp_core_compact` and `matrixark_mcp_serving_records`.
Neither module imports the other's copy; each calls its own. So which fields a record ends up with
depends on which module the caller reached:

    matrixark_context_backfill          -> matrixark_mcp_core_compact's copies
    matrixark_temporal_direct_backend   -> matrixark_mcp_core_compact's copies
    matrixark_mcp_serving_records       -> its own

Measured by running both copies on the same record:

    compact_record_scope, a context_event whose scope carries session_id
        serving_records  lifts session_id onto the record before popping `scope`
        core_compact     pops `scope` and the session id is gone entirely

    attach_storage_route, storage_options on the record
        serving_records  adds storage_route, storage_record_kind, storage_part
        core_compact     adds storage_route only

    attach_storage_route, storage_options only in the envelope
        serving_records  adds storage_options, storage_route, storage_record_kind, storage_part
        core_compact     adds storage_route only

The direction is the same in every case: `matrixark_mcp_core_compact` is behind, and the two live
callers that bind it are the backfill and the direct backend.

THIS FILE DOES NOT ASSERT THAT THE COPIES AGREE, because they do not, and a guard that fails on the
day it is written tells nobody anything. It RECORDS the difference exactly, in both directions, so
a new divergence fails here and a resolved one fails here too. Making the compact copy match would
add fields to records the store already holds, which changes what is written on a serving path -- a
decision rather than a cleanup, and dedup identities have been sensitive to added fields before.

One thing that is NOT a finding, recorded so nobody re-derives it: `attach_storage_route` in
`matrixark_mcp_serving_records` assigns a local `envelope` and never reads it. That looks like a
lost envelope fallback but is not -- the fallback lives inside `storage_options_for_record`, which
that copy calls, and the measurement above shows it does pick the envelope up. The local is dead
code only.
"""
from __future__ import annotations

import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_core_compact as compact_module
import matrixark_mcp_serving_records as serving_module

#: The record every case below is shaped from. A context_event whose scope keys on HASHES, which is
#: what canonical_scope_key needs to produce a scope_key at all -- an id-only scope returns "" and
#: every assertion here would pass over an untouched record.
SCOPE = {"tenant_hash": 111, "user_hash": 222, "session_hash": 333, "agent_hash": 444,
         "session_id": "sess-abc"}


def _event_record():
    return {"record_type": "context_event", "event_id_hash": 4242,
            "scope": dict(SCOPE), "payload": "hello"}


def _added(module, function_name, record):
    """The field names `function_name` adds to `record`, as that module defines it."""
    before = set(record)
    after = getattr(module, function_name)(dict(record))
    return sorted(set(after) - before), after


class TheRecordShapingHelpersHaveOneDefinition(unittest.TestCase):

    def test_both_modules_really_define_the_helpers(self) -> None:
        """A floor. Every comparison below passes trivially if a name moved away."""
        for name in ("attach_storage_route", "compact_record_scope",
                     "materialize_serving_records"):
            for module in (compact_module, serving_module):
                with self.subTest(helper=name, module=module.__name__):
                    self.assertTrue(
                        callable(getattr(module, name, None)),
                        "%s no longer defines %s; if it now delegates to the other copy, the split "
                        "is resolved and this file should be struck"
                        % (module.__name__, name))

    def test_the_scope_key_is_actually_produced(self) -> None:
        """A floor on the FIXTURE, not the code.

        compact_record_scope returns the record untouched when canonical_scope_key gives "", so a
        scope of the wrong shape makes every assertion below vacuous. The fixture failed exactly
        that way the first time it was written, with an id-only scope.
        """
        for module in (compact_module, serving_module):
            with self.subTest(module=module.__name__):
                out = module.compact_record_scope(_event_record())
                self.assertTrue(
                    str(out.get("scope_key") or ""),
                    "%s produced no scope_key for the fixture, so it took the early return and "
                    "nothing below is being compared" % module.__name__)
                self.assertNotIn(
                    "scope", out,
                    "%s kept `scope`, so it did not reach the compaction branch" % module.__name__)

    def test_the_compact_copy_drops_the_session_id_the_serving_copy_keeps(self) -> None:
        """Recorded, both directions. The consequence that makes the split matter."""
        serving = serving_module.compact_record_scope(_event_record())
        compact = compact_module.compact_record_scope(_event_record())

        self.assertEqual(
            SCOPE["session_id"], serving.get("session_id"),
            "matrixark_mcp_serving_records stopped lifting session_id out of the scope before "
            "popping it. If that was deliberate, the session id is now lost on every path.")
        self.assertIsNone(
            compact.get("session_id"),
            "matrixark_mcp_core_compact now keeps session_id too -- the split is resolved. Strike "
            "this test and say which copy won; the backfill and direct backend bind this one.")

    def test_the_compact_copy_attaches_fewer_storage_fields(self) -> None:
        """Recorded, both directions, for both places storage_options can live."""
        on_record = {"record_type": "context_event", "storage_options": {"tier": "hot"},
                     "payload": "x"}
        in_envelope = {"record_type": "context_event",
                       "envelope": {"storage_options": {"tier": "hot"}}, "payload": "x"}

        for label, record, serving_expected in (
                ("storage_options on the record", on_record,
                 ["storage_part", "storage_record_kind", "storage_route"]),
                ("storage_options only in the envelope", in_envelope,
                 ["storage_options", "storage_part", "storage_record_kind", "storage_route"])):
            with self.subTest(case=label):
                compact_added, _ = _added(compact_module, "attach_storage_route", record)
                serving_added, _ = _added(serving_module, "attach_storage_route", record)

                self.assertEqual(
                    ["storage_route"], compact_added,
                    "matrixark_mcp_core_compact.attach_storage_route now attaches %s. If it caught "
                    "up with the serving copy, strike this file." % serving_added)
                self.assertEqual(
                    serving_expected, serving_added,
                    "matrixark_mcp_serving_records.attach_storage_route changed what it attaches; "
                    "it is the richer copy and the serving path depends on these fields")

    def test_neither_module_binds_the_others_copy(self) -> None:
        """Why the split persists: each module calls its own, so the caller decides.

        If one ever binds the other's, the two stop being able to drift and this file is done.
        """
        for module in (compact_module, serving_module):
            for name in ("attach_storage_route", "compact_record_scope"):
                with self.subTest(module=module.__name__, helper=name):
                    bound = getattr(module, name)
                    self.assertEqual(
                        module.__name__, bound.__module__,
                        "%s.%s is now defined in %s -- the copies were consolidated, so strike this "
                        "file and record which behaviour won"
                        % (module.__name__, name, bound.__module__))


if __name__ == "__main__":
    unittest.main()
