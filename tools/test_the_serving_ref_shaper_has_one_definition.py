#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The ContextPack ref shaper is defined twice, and BOTH copies run on one serving path.

`compact_context_pack_ref` turns a retrieved candidate into the item the prompt actually sees. It
exists in `matrixark_mcp_context_pack` and in `matrixark_mcp_core_context_pack`, and the two
bodies differ. What makes this worth a file of its own is that neither copy shadows the other --
they are reached by two DIFFERENT routes out of the same module, and a served pack can pass
through either.

    route A   an adapter calls compact_context_pack_refs
              -> matrixark_mcp_core_context_pack's copy

    route B   the MCP entrypoint re-compacts a pack an adapter already shaped
              core_context_pack.compact_context_pack_for_serving  (a 3-statement delegator)
              -> matrixark_mcp_context_pack.compact_context_pack_for_serving
              -> compact_prebuilt_serving_groups
              -> matrixark_mcp_context_pack's copy

Route B exists on purpose: the comment at that branch says "some adapters already return the
serving shape. Preserve it so a second compaction pass in the MCP entrypoint does not erase refs."
The pack-level compactor WAS consolidated, by delegation. The ref-level one under it was not, so
the delegation lands in a module whose ref shaper is the other one.

MEASURED BY CALLING BOTH, not by reading them. Each row below is an input this file runs through
both copies; the differing field is the assertion.

    ref                                       context_pack's       core_context_pack's
    ----------------------------------------  -------------------  ---------------------
    event_type="pending_async", no debug      memory_layer =       memory_layer =
                                              "pending_async_      "context_memory"
                                              event"
    source_memory_layers=[...]  (debug)       dropped              kept
    source_memory_layer_counts={...} (debug)  dropped              kept
    source_session_count=5      (debug)       dropped              kept
    source_hook_type_counts={"a":1,"z":0}     {"a": 1}             {"a": 1, "z": 0}
    final_session_boundary="yes" (debug)      True                 "yes"

The first row is the one that costs something on a DEFAULT retrieve: `include_debug` is false on
the live serving call, and the two copies still put a different `memory_layer` on the same
candidate. It comes from a nested helper that is itself duplicated -- `_memory_layer_for_ref`
against `memory_layer_for_serving_ref` -- where one returns the literal "pending_async_event" and
the other asks `candidate_memory_layer_name`. That helper pair is part of this divergence and is
covered here through the shaper's output rather than separately, because the shaper's output is
what a client receives.

The rest only appear when a caller asks for debug lineage, and there they run in opposite
directions: core_context_pack's keeps three lineage fields context_pack's never lists, and
context_pack's compacts zero-valued entries out of a count map that core_context_pack's passes
through whole. Neither copy is a superset of the other, which is why this file does not pick one.

THIS FILE DOES NOT ASSERT THAT THEY AGREE. They do not, and a guard that fails the day it is
written tells nobody anything. It RECORDS the divergence in both directions: a new diverged pair
across these two modules fails here, and a recorded pair that stops diverging fails here too,
because somebody choosing a winner is a serving-behaviour decision that should be visible rather
than silent. Choosing here would change which fields a served pack carries and what `memory_layer`
a pending-async candidate is labelled with.

WHAT IS RECORDED AND IS NOT DRIFT. `compact_context_pack_for_serving` also has two bodies across
these modules, and it is in the list below saying so -- because core_context_pack's is the
3-statement delegator described above, which is consolidation rather than drift. Leaving it out on
the grounds that it is benign would mean the list is not the whole truth about the pair.

WHERE THIS SITS NEXT TO THE CHECKS THAT ALREADY EXIST. `test_a_diverged_cluster_nothing_reaches`
asks what in `matrixark_mcp_context_pack` no production importer reaches, and records ten names.
`compact_context_pack_refs` -- the PLURAL -- is one of them. The singular is not, and that is the
whole point of this file: it is reached, from route B, so it is not an orphan to delete and its
divergence has live consequences.
"""
from __future__ import annotations

import ast
import hashlib
import importlib
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: The copy reached through `compact_prebuilt_serving_groups` on route B.
GROUPED = "matrixark_mcp_context_pack"

#: The copy an adapter reaches directly on route A.
DIRECT = "matrixark_mcp_core_context_pack"

FAMILY = (GROUPED, DIRECT)

#: Keyed the way `_diverged` reports a pair: the defining modules, sorted.
PAIR = tuple(sorted(FAMILY))

#: (function, the two modules that define it) -> what the copies disagree about.
#: Asserted exactly, in both directions.
RECORDED = {
    ("compact_context_pack_ref", PAIR):
        "the serving ref shape itself. On a DEFAULT retrieve the two put a different memory_layer "
        "on a pending_async candidate; under debug lineage core_context_pack's keeps "
        "source_memory_layers, source_memory_layer_counts and source_session_count that "
        "context_pack's never lists, while context_pack's compacts zero-valued count-map entries "
        "that core_context_pack's passes through. Neither is a superset of the other",
    ("compact_context_pack_for_serving", PAIR):
        "NOT drift, recorded so this list is the whole truth about the pair: "
        "core_context_pack's body is a 3-statement delegator to context_pack's, which is how the "
        "pack-level compactor was consolidated. It is also how route B reaches the OTHER copy of "
        "compact_context_pack_ref",
}

#: Floors on the scan. A renamed module or a failed parse must fail loudly rather than empty the
#: comparison and pass.
DEFINITION_FLOOR = {GROUPED: 30, DIRECT: 6}


def _import(stem):
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


def _bodies():
    """(function, module) -> (body digest, parameter names, statement count) across the pair."""
    out = {}
    for stem in FAMILY:
        path = os.path.join(TOOLS, stem + ".py")
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                tree = ast.parse(handle.read())
        except (OSError, SyntaxError):  # pragma: no cover - an unparseable module fails elsewhere
            continue
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                # The docstring is prose. Two copies with identical code and different prose are
                # not drift, and hashing it in has reported one before.
                body = body[1:]
            dumped = ast.dump(ast.Module(body=body, type_ignores=[]))
            out[(node.name, stem)] = (
                hashlib.sha256(dumped.encode("utf-8")).hexdigest()[:12],
                tuple(argument.arg for argument in node.args.args),
                len(body),
            )
    return out


def _diverged():
    """Functions the pair both define, with the same parameters and different bodies."""
    bodies = _bodies()
    by_name = {}
    for (name, stem), value in bodies.items():
        by_name.setdefault(name, []).append((stem, value))
    out = {}
    for name, entries in by_name.items():
        if len(entries) < 2:
            continue
        if len({value[1] for _stem, value in entries}) != 1:
            continue
        if len({value[0] for _stem, value in entries}) == 1:
            continue
        out[(name, tuple(sorted(stem for stem, _value in entries)))] = entries
    return out


def _shapers():
    return _import(GROUPED).compact_context_pack_ref, _import(DIRECT).compact_context_pack_ref


#: A candidate both copies accept, carrying nothing that either one drops. Every fixture below
#: starts from this and adds exactly the field under test.
BASE_REF = {
    "ref_type": "context_memory",
    "text": "a retrieved sentence",
    "citation": "session-1#3",
    "source_role": "user",
}


class TheServingRefShaperHasOneDefinition(unittest.TestCase):

    def test_the_pair_is_there_to_compare(self) -> None:
        """A floor. Every assertion below passes over an empty read."""
        bodies = _bodies()
        for stem in FAMILY:
            with self.subTest(module=stem):
                found = [key for key in bodies if key[1] == stem]
                self.assertGreaterEqual(
                    len(found), DEFINITION_FLOOR[stem],
                    "%s contributed %d top-level functions, under the floor of %d. A renamed, "
                    "moved or unparseable module makes every comparison in this file vacuous"
                    % (stem, len(found), DEFINITION_FLOOR[stem]))
        for stem in FAMILY:
            with self.subTest(module=stem):
                self.assertIn(
                    ("compact_context_pack_ref", stem), bodies,
                    "compact_context_pack_ref is no longer defined in %s -- if it was "
                    "consolidated, strike the record below and say which copy won" % stem)

    def test_the_divergence_is_exactly_what_is_recorded(self) -> None:
        """Asserted in BOTH directions.

        A new pair fails, so the split cannot grow quietly. A recorded pair that stops diverging
        fails too, because a list allowed to rot describes a tree that no longer exists -- and
        here it would hide the fact that somebody had chosen a winner on a serving path.
        """
        found = set(_diverged())
        recorded = set(RECORDED)
        new = sorted(found - recorded)
        gone = sorted(recorded - found)
        self.assertEqual(
            [], new,
            "these now have two definitions across the ContextPack modules that disagree, and "
            "nothing records what they disagree about: %s"
            % ", ".join("%s in %s" % (n, " and ".join(m)) for n, m in new))
        self.assertEqual(
            [], gone,
            "these are recorded as diverged and no longer are -- strike them, and say which copy "
            "won: %s" % ", ".join("%s in %s" % (n, " and ".join(m)) for n, m in gone))

    def test_both_copies_are_reached_by_the_live_pack_compactor(self) -> None:
        """The reason this is not a shadowing record: the same entry point reaches both.

        Route A and route B start from the SAME module. If this ever fails because both routes
        land on one copy, the split is resolved -- strike the record above and delete this test.
        """
        direct = _import(DIRECT)
        candidate = dict(BASE_REF, event_type="pending_async")

        route_a = direct.compact_context_pack_refs([dict(candidate)], include_debug=False)
        self.assertEqual(1, len(route_a), "route A returned no item to compare")
        self.assertEqual(
            BASE_REF["text"], route_a[0].get("text"),
            "the route A fixture did not survive the shaper, so the field compared below is not "
            "this candidate's")

        pack = {"context_pack_id": "pack-1", "groups": [{"items": [dict(candidate)]}]}
        served = direct.compact_context_pack_for_serving(pack, include_debug=False)
        groups = served.get("groups") or []
        self.assertTrue(
            groups and isinstance(groups[0], dict) and groups[0].get("items"),
            "the route B fixture did not reach compact_prebuilt_serving_groups -- a pack with "
            "prebuilt groups and no selected_refs is what takes that branch, and without it this "
            "test compares nothing")
        item = groups[0]["items"][0]
        self.assertEqual(
            BASE_REF["text"], item.get("text"),
            "the route B fixture did not survive the shaper")

        self.assertNotEqual(
            route_a[0].get("memory_layer"), item.get("memory_layer"),
            "both routes now label a pending_async candidate the same way. If the ref shaper was "
            "consolidated, that is the fix -- strike the record above and delete this test")
        self.assertEqual(
            "context_memory", route_a[0].get("memory_layer"),
            "route A no longer labels this candidate through candidate_memory_layer_name")
        self.assertEqual(
            "pending_async_event", item.get("memory_layer"),
            "route B no longer labels this candidate with the literal from _memory_layer_for_ref")

    def test_the_default_serving_shape_already_differs(self) -> None:
        """include_debug is FALSE on the live retrieve, and the copies still disagree there.

        Stated separately from the lineage cases below so that a change confining the divergence
        to debug output fails HERE and nowhere else.
        """
        grouped, direct = _shapers()
        candidate = dict(BASE_REF, event_type="pending_async")
        left = grouped(dict(candidate), include_debug=False)
        right = direct(dict(candidate), include_debug=False)
        self.assertTrue(
            left.get("memory_layer") and right.get("memory_layer"),
            "neither copy labelled a memory_layer, so this fixture never reached the branch under "
            "test -- an event_type of pending_async is what reaches it")
        self.assertNotEqual(
            left.get("memory_layer"), right.get("memory_layer"),
            "the two copies now agree on the default serving shape. Strike the record above.")

    def test_each_copy_keeps_lineage_the_other_drops(self) -> None:
        """Both directions, so neither copy can be called the superset."""
        grouped, direct = _shapers()

        kept_only_by_direct = {
            "source_memory_layers": ["episodic", "profile"],
            "source_memory_layer_counts": {"episodic": 2},
            "source_session_count": 5,
        }
        for field, value in kept_only_by_direct.items():
            with self.subTest(field=field):
                candidate = dict(BASE_REF)
                candidate[field] = value
                left = grouped(dict(candidate), include_debug=True)
                right = direct(dict(candidate), include_debug=True)
                self.assertIn(
                    field, right,
                    "%s is no longer emitted by %s, so this fixture no longer separates the two "
                    "copies" % (field, DIRECT))
                self.assertNotIn(
                    field, left,
                    "%s now survives %s's copy too -- the lineage lists have converged, strike "
                    "the record above" % (field, GROUPED))

        candidate = dict(BASE_REF, source_hook_type_counts={"user_prompt_submit": 3, "unused": 0})
        left = grouped(dict(candidate), include_debug=True)
        right = direct(dict(candidate), include_debug=True)
        self.assertEqual(
            {"user_prompt_submit": 3}, left.get("source_hook_type_counts"),
            "%s's copy no longer compacts a zero-valued count-map entry away" % GROUPED)
        self.assertEqual(
            {"user_prompt_submit": 3, "unused": 0}, right.get("source_hook_type_counts"),
            "%s's copy no longer passes a count map through whole" % DIRECT)

        candidate = dict(BASE_REF, final_session_boundary="yes")
        left = grouped(dict(candidate), include_debug=True)
        right = direct(dict(candidate), include_debug=True)
        self.assertEqual(
            True, left.get("final_session_boundary"),
            "%s's copy no longer coerces final_session_boundary to a bool" % GROUPED)
        self.assertEqual(
            "yes", right.get("final_session_boundary"),
            "%s's copy no longer passes final_session_boundary through unchanged" % DIRECT)

    def test_the_debug_fixtures_are_not_answering_an_empty_question(self) -> None:
        """A control for the test above: with include_debug FALSE, none of those fields survive.

        Without this, a copy that started emitting lineage unconditionally would still satisfy
        every assertion above while changing what a default retrieve returns.
        """
        grouped, direct = _shapers()
        candidate = dict(
            BASE_REF,
            source_memory_layers=["episodic"],
            source_memory_layer_counts={"episodic": 2},
            source_session_count=5,
            source_hook_type_counts={"user_prompt_submit": 3},
            final_session_boundary="yes",
        )
        for stem, shaper in ((GROUPED, grouped), (DIRECT, direct)):
            served = shaper(dict(candidate), include_debug=False)
            for field in ("source_memory_layers", "source_memory_layer_counts",
                          "source_session_count", "source_hook_type_counts",
                          "final_session_boundary"):
                with self.subTest(module=stem, field=field):
                    self.assertNotIn(
                        field, served,
                        "%s emits %s on a DEFAULT retrieve. The lineage record above is written "
                        "about debug output; if it is no longer debug-only that is a bigger "
                        "change than this file records" % (stem, field))


if __name__ == "__main__":
    unittest.main()
