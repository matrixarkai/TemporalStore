#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A module nothing imports and nothing runs has to be listed.

This is not about disk. The condition has already produced a live hazard twice over, and mx#1073 is
the worked example: `matrixark_mcp_local_read` and `matrixark_mcp_local_runtime` were imported by
nothing, and each held a STALE COPY of a live implementation. `read_all` there opened the event log
with `open("r", encoding="utf-8")` and parsed lines, which the live path stopped doing when the log
became a block container. A copy that is both wrong and unreachable is the worst combination: it
cannot fail today, and it is exactly what somebody reaches for tomorrow.

So the set is asserted exactly. A NEW orphan fails here rather than accumulating, and one that stops
being an orphan -- because somebody wired it up or deleted it -- fails too, because a list allowed to
go stale describes a tree that no longer exists.

The entries below are NOT an endorsement. One module is left, and it is here on purpose rather than by oversight, triaged by
whether their definitions still match the live ones. Two of the original 34 are gone: both were
copies whose every function was the live one word for word, so removing them could not lose
anything.

  * The rest have DIVERGED from the live definition of the same name, which is the more misleading
    kind: `matrixark_mcp_registry.list_skills` and the live `matrixark_local_adapter_dashboard` one
    no longer agree, and nothing says which is current.
  * A caveat on that triage, learned removing the first two: it compared bodies with each line's
    whitespace normalised, which does not normalise LINE BREAKS.
    `matrixark_mcp_backend_readiness.adapter_ensure_backend_ready` was reported as diverged and was
    identical -- the copy wrapped its signature across four lines where the live one uses one. Redone
    through `ast.unparse`, which renders both from the tree and is blind to formatting, no entry
    below is a pure copy any more: the two that were are gone.
  * The two schema modules went the same way and took two more with them.
    `matrixark_mcp_admin_schemas` described 18 tools the live module describes among its 43,
    and nothing extra about any of them. `matrixark_mcp_storage_schemas` became redundant once
    the two storage options it alone declared were added to the live schema. Removing the
    admin one then left `matrixark_mcp_auth_schemas` unreferenced, whose three schemas are
    byte-identical to the live ones -- and removing THAT left matrixark_mcp_schema_common,
    whose three schemas are identical too. A chain of four, found one link at a time.
  * The `matrixark_mcp_retrieve_*` modules went together: one family, 47 KB, left behind by a split
    of the retrieve path. Seven had every name confined to themselves -- abandoned rather than moved
    -- and three had their code taken into a live module under the same name (`record_identity` and
    `selected_by_tree` into `matrixark_local_adapter_retrieve`). Removing those ten made TWO MORE
    orphans, `matrixark_mcp_retrieve_embeddings` and `matrixark_mcp_retrieve_node_scores`, which
    only the dead ones had been naming. Orphans come in chains, and this file is what noticed.
  * Two others went for a different reason. `matrixark_mcp_server_metrics` and
    `matrixark_mcp_temporal_audit` each held one MIXIN CLASS, and the class name appeared nowhere
    but its own file -- so nothing inherited them even before the module stopped being imported --
    while every method they defined exists live in `matrixark_mcp_server`, `matrixark_access`,
    `matrixark_mcp_local_adapter` or `matrixark_temporal_direct_write`. Dead twice over.

Removing them is follow-up work and wants reading each one first. What this file does is stop the
set growing while that happens.

An entry point is not an orphan: a module with an `if __name__ == "__main__"` guard is run rather
than imported, and this asks the AST for that rather than for a filename convention. A module whose
TOP LEVEL reads `sys.argv` counts too -- `tools/workspace/parse_aws_jsonl_logs` is nothing but
top-level statements, so it has nowhere to put a guard, and listing it would be reporting its style
rather than its use.
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import unittest
from typing import Dict, List, Set

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: module stem -> what it is, so removing it is a reading task and not a guess.
KNOWN_ORPHANS: Dict[str, str] = {
    "matrixark_mcp_rust_proxy_client":
        "NOT dead by accident, and the reason it is still here. It is the pre-split proxy client, "
        "and it heads a cluster: matrixark_mcp_rust_proxy_cache_mixin and "
        "matrixark_mcp_rust_proxy_coalesce are imported by NOTHING ELSE, so they go with it. "
        "Between them they implement a string cache, a scan-hash cache, a context-pack response "
        "cache, and coalescing for batch hset, batch hget and record append. The live "
        "MatrixArkRustProxyClient in matrixark_mcp_temporal_adapters -- which is the one "
        "matrixark_mcp_server imports -- has NONE of that: no base class, and no member whose "
        "name contains cache or coalesce. The live one is richer where it counts for "
        "correctness (__init__ 69 lines against 23, _record_call_metrics 88 against 2, _call_json "
        "66 against 51) and has no performance layer at all. Removing this makes that gap "
        "permanent; wiring it up is a product decision. Either is a choice somebody should make "
        "on purpose, which is why it is written down rather than deleted.",
}

#: 330 non-test modules under tools/ when this was written.
EXPECTED_MODULE_FLOOR = 250

#: A module every tree has and every tree imports, so a scan that reports IT as unreachable is
#: broken rather than reporting news. Without a positive control an emptied scan reads as a clean
#: tree.
POSITIVE_CONTROL = "matrixark_mcp_local_adapter"

_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


#: This file, relative to the repository. It lists the module names it decides about, which is
#: exactly the shape its own scan counts as a mention -- so once it was committed and became a
#: tracked file, it credited every one of them with being imported and reported a clean tree. The
#: same self-reference that made an earlier guard feed on its own output (mx#910), and the reason
#: for the control below.
_SELF = os.path.join("tools", os.path.basename(__file__))

#: How many module stems a literal collection must hold before the file counts as an INVENTORY of
#: module names rather than a file that happens to mention a few. Three is above anything that
#: occurs by accident in this tree and below the smallest real inventory.
_INVENTORY_THRESHOLD = 3


def _is_a_module_inventory(path: str, stems: Set[str]) -> bool:
    """Does this file EXIST to enumerate module names?

    Excluding only `_SELF` was too narrow, and the way that showed up is worth keeping: a second
    guard was written that lists modules unreachable from production, for the same kind of reason
    this one lists orphans. Listing them made this scan count each as mentioned, the recorded set
    below went stale, and the check failed on a change that altered nothing about the tree. The
    third instance of a guard feeding on a list -- after mx#910 and this file's own `_SELF`.

    So the exclusion is derived: a file qualifies when it assigns a literal collection, to an
    ALL-CAPS name, holding at least `_INVENTORY_THRESHOLD` module stems. It keeps fitting when a
    fourth is written, which a hand-written list of filenames would not.

    Measured, because "fits both guards and nothing else" was the guess and it was wrong: it
    matches THREE files -- the reachability guard this was written for, plus
    `test_latest_state_key_agreement` and
    `test_matrixark_every_admin_operation_is_fenced_by_tenant`, which both tabulate module names
    the same way. Excluding them is the safe direction and that is why it is left alone: dropping
    a file from the mention scan can only make this check report MORE orphans, never hide one. The
    recorded set below did not change when they were excluded, which is the evidence that they
    were not the only thing keeping a module off the list.
    """
    if not path.endswith(".py"):
        return False
    try:
        with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
            tree = ast.parse(handle.read())
    except (OSError, SyntaxError):
        return False
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(isinstance(t, ast.Name) and t.id.isupper() for t in targets):
            continue
        found = {inner.value for inner in ast.walk(node)
                 if isinstance(inner, ast.Constant) and isinstance(inner.value, str)
                 and inner.value in stems}
        if len(found) >= _INVENTORY_THRESHOLD:
            return True
    return False


def _tracked(stems: Set[str] | None = None) -> List[str]:
    listed = subprocess.run(["git", "ls-files"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    paths = [path for path in listed if path != _SELF]
    if stems is None:
        return paths
    return [path for path in paths if not _is_a_module_inventory(path, stems)]


def _modules() -> Dict[str, str]:
    """stem -> path, for the non-test python modules under tools/."""
    found = {}
    for path in _tracked():
        if not path.startswith("tools/") or not path.endswith(".py"):
            continue
        base = os.path.basename(path)
        if base.startswith("test_"):
            continue
        stem = path[len("tools/"):-len(".py")]
        found[stem] = path
    return found


def _named_anywhere(modules: Dict[str, str]) -> Set[str]:
    """Every module stem that any OTHER tracked file mentions.

    One pass over the corpus, tokenised. Asking git per module re-read the tree once per module and
    turned a check into a two-minute one, which is how a guard stops being run.
    """
    stems = {os.path.basename(stem) for stem in modules}
    own = {os.path.basename(stem): path for stem, path in modules.items()}
    seen: Set[str] = set()
    for path in _tracked(stems):
        try:
            with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
                text = handle.read()
        except (OSError, ValueError):
            continue
        for word in set(_WORD.findall(text)):
            if word in stems and own.get(word) != path:
                seen.add(word)
    return seen


def _is_entry_point(path: str) -> bool:
    try:
        with open(os.path.join(REPO, path), encoding="utf-8", errors="replace") as handle:
            tree = ast.parse(handle.read())
    except (OSError, SyntaxError):
        return True
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and isinstance(node.test, ast.Compare):
            left = node.test.left
            if isinstance(left, ast.Name) and left.id == "__name__":
                return True
    # A module whose TOP LEVEL reads sys.argv is a script too. tools/workspace has one that is
    # nothing but top-level statements -- no functions and so nowhere to put a __main__ guard --
    # and calling it unreachable would be reporting its style, not its use.
    for node in tree.body:
        for inner in ast.walk(node):
            if isinstance(inner, ast.Attribute) and inner.attr == "argv":
                return True
    return False


def _orphans() -> Set[str]:
    modules = _modules()
    named = _named_anywhere(modules)
    return {stem for stem, path in modules.items()
            if os.path.basename(stem) not in named and not _is_entry_point(path)}


class NoModuleIsOrphanedQuietlyTest(unittest.TestCase):

    def test_the_inventory_exclusion_matches_inventories_and_not_ordinary_files(self) -> None:
        """Control on the exclusion itself, in both directions.

        Too narrow and this check goes stale the next time somebody writes a guard that lists
        module names -- which is exactly what happened. Too wide and it stops counting real
        mentions, so it reports orphans that are used; that direction is safe here (it can only
        add), but it should still be visible rather than silent.
        """
        modules = _modules()
        stems = {os.path.basename(stem) for stem in modules}
        classified = sorted(path for path in _tracked()
                            if path.endswith(".py") and _is_a_module_inventory(path, stems))

        self.assertIn(
            "tools/test_a_module_only_tests_reach_is_not_live.py", classified,
            "the guard that lists modules unreachable from production is no longer recognised as "
            "an inventory, so its list is being counted as evidence that those modules are used "
            "and the set below will go stale again")

        for ordinary in ("tools/matrixark_mcp_core.py", "tools/matrixark_mcp_local_adapter.py",
                         "tools/matrixark_mcp_server.py"):
            self.assertNotIn(
                ordinary, classified,
                "%s was classed as an inventory of module names. It is live code, and excluding "
                "it from the mention scan hides every module it imports" % ordinary)

        self.assertLess(
            len(classified), len(list(_tracked())) // 20,
            "a twentieth of the tree is now classed as a module inventory, which means the rule "
            "matches something ordinary rather than the two or three files it was derived for")


    def test_the_scan_still_sees_the_tree(self) -> None:
        modules = _modules()
        self.assertGreaterEqual(
            len(modules), EXPECTED_MODULE_FLOOR,
            "found %d modules under tools/, expected at least %d -- if the listing changed, every "
            "assertion below runs on an empty set" % (len(modules), EXPECTED_MODULE_FLOOR))

    def test_a_module_everything_imports_is_not_called_an_orphan(self) -> None:
        """Catches a scan that has become too STRICT and calls live code unreachable."""
        self.assertIn(POSITIVE_CONTROL, _modules(), "the control module moved")
        self.assertNotIn(
            POSITIVE_CONTROL, _orphans(),
            "%s is imported all over this tree and the scan called it unreachable, so the scan is "
            "broken and the list below means nothing" % POSITIVE_CONTROL)

    def test_the_scan_can_tell_a_used_name_from_an_unused_one(self) -> None:
        """Catches the opposite of the control above, which it cannot see.

        A scan that matches NOTHING passes every other assertion here: the new-orphan list is empty
        and the module everything imports is not in it. That is how this file broke itself once --
        committing it made it a tracked file, its own dict keys read as uses of all 34 names, and it
        reported a clean tree.

        This asks the mechanism directly instead of naming a module that is only an orphan until
        somebody removes it: a name this tree really does use must be seen, and a name it cannot
        possibly use must not be. Both halves fail if the matching goes blind in either direction,
        and neither depends on the list above having anything in it.
        """
        named = _named_anywhere(_modules())
        self.assertIn(
            "matrixark_mcp_core", named,
            "the scan cannot see that matrixark_mcp_core is used, though this tree imports it "
            "everywhere -- so it would call live modules unreachable")
        self.assertNotIn(
            "zz_no_module_is_called_this", named,
            "the scan reports a name nothing could contain as used, so it would call every orphan "
            "reachable and report a clean tree whatever the truth")

    def test_no_new_module_is_orphaned(self) -> None:
        new = sorted(_orphans() - set(KNOWN_ORPHANS))
        self.assertEqual(
            [], new,
            "these modules are imported by nothing and run by nothing: %s\nThat is how a stale "
            "copy of a live implementation survives -- see mx#1073, where an orphan read the event "
            "log as text long after the log became a container. Wire it up, delete it, or list it "
            "above with what it holds." % new)

    def test_a_listed_module_that_is_no_longer_orphaned_is_struck_off(self) -> None:
        stale = sorted(set(KNOWN_ORPHANS) - _orphans())
        self.assertEqual(
            [], stale,
            "these are listed as orphaned and are not any more, because they were wired up or "
            "removed: %s. Strike them off." % stale)

    def test_every_listed_module_says_what_it_holds(self) -> None:
        thin = sorted(stem for stem, why in KNOWN_ORPHANS.items() if len(why.strip()) < 15)
        self.assertEqual([], thin, "listed without saying what they hold: %s" % thin)


if __name__ == "__main__":
    unittest.main()
