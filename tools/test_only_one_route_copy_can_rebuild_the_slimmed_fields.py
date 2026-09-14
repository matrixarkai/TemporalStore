#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The slimmer drops ten fields as recoverable. Only one of the two live copies can rebuild them.

`slim_persisted_storage_options` removes ten names from every stored `storage_options` block
because `canonical_storage_route` can rebuild them from what is kept.
`test_matrixark_the_derived_storage_options_are_not_stored` holds that line, and holds it well --
over a matrix of shapes, not one fixture. It imports `canonical_storage_route` from
`matrixark_mcp_storage_options`.

There are two definitions of that function, both reachable from production:

    matrixark_mcp_core.canonical_storage_route
    matrixark_mcp_storage_options.canonical_storage_route

and they are not interchangeable. Measured here over every option block
`normalize_storage_options` will build from the permitted vocabularies, core's copy fails to
rebuild `replica_read` in **all** of them -- it does not emit the name at all -- while the
storage_options copy rebuilds all ten exactly.

The live writers of the `storage_route` field are split between the two. Counting only modules
`reachable_from_production()` reports as live, three reach core's copy --
`matrixark_local_adapter_session_commit:707,783`, `matrixark_mcp_core:2915`, and the pinned import
in `matrixark_mcp_core_compact.attach_storage_route:205` -- and one reaches the other,
`matrixark_mcp_serving_records:224`. So `storage_route` does not have one shape on stored records:
it has the shape of whichever module wrote it.

(`matrixark_mcp_session_runtime:1262,1298` writes the field with core's copy too and is NOT counted
above -- the module is unreachable from any production entry point. Its call sites look exactly like
live ones in a grep, which is the whole reason `reachable_from_production()` exists.)

WHY THIS IS A GUARD AND NOT A FIX
---------------------------------
Nothing in production re-derives. The ten fields are dropped at write and no read path rebuilds
them, so no record loses a value today; recoverability is the *justification* for the drop rather
than something the tree exercises. And the choice of copy at `attach_storage_route` is deliberate
and documented -- switching it would alter `storage_route` on records the store already holds,
which is not a test's call to make.

What is missing is that the justification is narrower than the tree. "These fields are
recoverable" is true of one copy. Whoever eventually writes the rehydrate must reach for
`matrixark_mcp_storage_options`, and nothing says so -- the nearest copy to three of the four
writers is the one that cannot do it.

So this records the relationship in BOTH directions, which is what stops it drifting:

  * the storage_options copy rebuilds all ten, exactly -- if that stops being true the slimming has
    lost its justification and this fails;
  * core's copy does NOT rebuild `replica_read` -- if that stops being true, because someone
    widened core's copy or narrowed the dropped set, this fails too and the note above is stale.

A one-directional version of this test would pass forever after someone made the two copies agree,
and the reader would never learn that the distinction had been resolved.
"""
from __future__ import annotations

import itertools
import unittest

try:
    from tools.matrixark_mcp_core import canonical_storage_route as core_route
    from tools.matrixark_mcp_storage_options import (
        canonical_storage_route as options_route,
        normalize_storage_options,
    )
    from tools.matrixark_mcp_temporal_append import _OPTIONS_KEYS_DERIVED_FROM_THE_REST
except ImportError:  # run from tools/
    from matrixark_mcp_core import canonical_storage_route as core_route
    from matrixark_mcp_storage_options import (
        canonical_storage_route as options_route,
        normalize_storage_options,
    )
    from matrixark_mcp_temporal_append import _OPTIONS_KEYS_DERIVED_FROM_THE_REST

DROPPED = tuple(_OPTIONS_KEYS_DERIVED_FROM_THE_REST)

#: The name core's copy does not produce. Spelled out so a reader knows which field is at stake
#: without running anything, and so widening core's copy fails a named assertion.
NOT_REBUILT_BY_CORE = "replica_read"

#: Taken from `_STORAGE_OPTION_ALLOWED_VALUES`. Values outside these are rejected by
#: `normalize_storage_options` outright, and a matrix built from invented spellings measures a
#: function no caller can reach -- an earlier pass at this used "remote" and "temporalstore" as
#: storage modes and every block it built raised instead of being scored.
STORAGE_MODES = ("default", "local", "single_node", "multi_node", "shared_store", "raft")
DURABILITIES = ("default", "async", "sync")
WRITE_MODES = ("default", "async", "sync")
READ_PREFERENCES = ("default", "primary", "replica", "replica_preferred")


def _blocks():
    """Every option block the normaliser will build from the permitted vocabularies.

    A value the normaliser has stopped permitting is skipped rather than raised, so that moving
    one of these vocabularies is reported by the floor in `setUp` -- which says what went wrong --
    instead of by a validation error raised out of a helper, which says only that a spelling was
    rejected and leaves the reader to work out that nothing was measured.
    """
    for mode, durability, write_mode, read_preference in itertools.product(
        STORAGE_MODES, DURABILITIES, WRITE_MODES, READ_PREFERENCES
    ):
        try:
            options = normalize_storage_options(
                {
                    "storage_options": {
                        "storage_mode": mode,
                        "durability": durability,
                        "write_mode": write_mode,
                        "read_preference": read_preference,
                    }
                }
            )
        except Exception:  # noqa: BLE001 - a rejected spelling is a skipped row, not a failure
            continue
        if options:
            yield options


def _rebuild(route, options):
    """What `route` fails to recover when the dropped names are taken away.

    `absent` and `differs` are kept apart deliberately. A field that rebuilds to the wrong value is
    caught by any comparison a reader makes; a field that is simply not there survives `dict.get`
    with a default and reads as "not configured". Collapsing the two would let a copy that had been
    half-repaired -- emitting the name, with the wrong value -- look unchanged here.
    """
    kept = {name: value for name, value in options.items() if name not in DROPPED}
    rebuilt = route(kept)
    missing = []
    for name in DROPPED:
        if name not in options:
            continue
        if name not in rebuilt:
            missing.append((name, "absent"))
        elif rebuilt[name] != options[name]:
            missing.append((name, "differs"))
    return missing


class OnlyOneRouteCopyCanRebuildTheSlimmedFields(unittest.TestCase):
    def setUp(self):
        self.blocks = list(_blocks())
        # A matrix that stopped producing blocks would make every assertion below vacuous, and a
        # vacuous pass here reads exactly like a clean one. Floor the SCAN.
        self.assertGreater(
            len(self.blocks),
            100,
            "the option matrix built %d blocks; normalize_storage_options has stopped accepting "
            "the vocabularies this test is written against, so nothing below was measured"
            % len(self.blocks),
        )

    def test_the_two_copies_are_distinct_definitions(self):
        """Both are live. If they ever became one object this whole question is settled."""
        self.assertIsNot(
            core_route,
            options_route,
            "matrixark_mcp_core and matrixark_mcp_storage_options now share one "
            "canonical_storage_route. That is a resolution of the split this file records -- "
            "delete this test and the note in matrixark_mcp_core_compact with it.",
        )

    def test_the_storage_options_copy_rebuilds_every_dropped_field(self):
        """The invariant the slimming rests on, stated against the copy that satisfies it."""
        failures = []
        for options in self.blocks:
            for name, why in _rebuild(options_route, options):
                failures.append("%s: rebuilt %s" % (name, why))
        self.assertEqual(
            [],
            sorted(set(failures)),
            "slim_persisted_storage_options drops %d fields from every stored record because "
            "matrixark_mcp_storage_options.canonical_storage_route can rebuild them. Over %d "
            "option blocks it now cannot, so the slimming is dropping values nothing can recover."
            % (len(DROPPED), len(self.blocks)),
        )

    def test_core_copy_cannot_rebuild_one_of_them(self):
        """The other direction: the copy three of the four writers reach is not sufficient.

        Recorded as `{field: why}`, not as a set of names, so that repairing core's copy fails here
        whichever way it is repaired -- deriving the value correctly empties the mapping, and
        emitting the name with a wrong value turns `absent` into `differs`.
        """
        unrebuilt = {}
        for options in self.blocks:
            for name, why in _rebuild(core_route, options):
                unrebuilt.setdefault(name, set()).add(why)
        self.assertEqual(
            {NOT_REBUILT_BY_CORE: {"absent"}},
            {name: whys for name, whys in unrebuilt.items()},
            "this test records that matrixark_mcp_core's canonical_storage_route fails to rebuild "
            "exactly %r, by not emitting it at all. That has changed to %s. If core's copy now "
            "rebuilds everything, the two are interchangeable for this purpose and the note in "
            "matrixark_mcp_core_compact is stale; if it emits the name with a different value, a "
            "rehydrate written against it would return a wrong answer rather than no answer; and "
            "if it has lost more fields, those go silently too."
            % (NOT_REBUILT_BY_CORE,
               {name: sorted(whys) for name, whys in sorted(unrebuilt.items())} or "nothing"),
        )

    def test_the_field_core_omits_is_one_the_slimmer_drops(self):
        """Without this the pairing is a coincidence, not a hazard."""
        self.assertIn(
            NOT_REBUILT_BY_CORE,
            DROPPED,
            "%r is no longer among the fields slim_persisted_storage_options removes, so core's "
            "copy omitting it costs a rehydrate nothing. This file has nothing left to guard."
            % NOT_REBUILT_BY_CORE,
        )

    def test_core_omits_the_field_rather_than_deriving_it_differently(self):
        """Absent and wrong are different failures, and only one of them is silent on read.

        A value that rebuilds to something else would be caught by any comparison. A name that is
        simply not there survives `dict.get` with a default and reads as "not configured".
        """
        sample = next(iter(self.blocks))
        kept = {name: value for name, value in sample.items() if name not in DROPPED}
        self.assertNotIn(
            NOT_REBUILT_BY_CORE,
            core_route(kept),
            "core's copy now emits %r; update NOT_REBUILT_BY_CORE and the note above."
            % NOT_REBUILT_BY_CORE,
        )
        self.assertIn(
            NOT_REBUILT_BY_CORE,
            options_route(kept),
            "the storage_options copy has stopped emitting %r, so neither copy can rebuild it and "
            "the slimmer is dropping a field nothing recovers." % NOT_REBUILT_BY_CORE,
        )


if __name__ == "__main__":
    unittest.main()
