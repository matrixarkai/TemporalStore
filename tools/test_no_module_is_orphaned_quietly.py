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

The entries below are NOT an endorsement. They are 32 modules that were already here, triaged by
whether their definitions still match the live ones. Two of the original 34 are gone: both were
copies whose every function was the live one word for word, so removing them could not lose
anything.

  * The rest have DIVERGED from the live definition of the same name, which is the more misleading
    kind: `matrixark_mcp_registry.list_skills` and the live `matrixark_local_adapter_dashboard` one
    no longer agree, and nothing says which is current.
  * A caveat on that triage, learned removing the two: it compares bodies with each line's
    whitespace normalised, which does not normalise LINE BREAKS.
    `matrixark_mcp_backend_readiness.adapter_ensure_backend_ready` was reported as diverged and was
    identical -- the copy wrapped its signature across four lines where the live one uses one. Read
    the pair before believing "diverged".

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
    "matrixark_mcp_local_backend":
        "all three have diverged -- backend_metrics, ensure_backend_ready, observe_model_latency",
    "matrixark_mcp_registry":
        "list_skills, list_resources, update_skill and latest_skill_controls have all diverged "
        "from matrixark_local_adapter_dashboard",
    "matrixark_mcp_retrieve_fallback":
        "deadline_fallback_pack has diverged from matrixark_mcp_deadline_pack",
    "matrixark_mcp_local_cache":
        "11 KB, eight read-cache and latest-entity helpers; named by nothing at all",
    "matrixark_mcp_native_pack":
        "10 KB, build_native_context_pack_request and its verbose contract. Easy to "
        "believe this one is live: matrixark_mcp_server_request_policy imports "
        "matrixark_mcp_native_pack_POLICY, and a substring search credits this module "
        "with that import. The word boundary is what tells them apart",
    "matrixark_mcp_admin_schemas": "18 KB of schema data, no functions",
    "matrixark_mcp_storage_schemas": "schema data, no functions",
    "matrixark_document_assembly": "six functions no live module defines",
    "matrixark_mcp_backend_metrics": "sixteen functions, eight of them unique here",
    "matrixark_mcp_hook_validation": "three functions, one unique here",
    "matrixark_mcp_native_retrieve": "one function no live module defines",
    "matrixark_mcp_resource_import_runtime": "seven functions, all unique here",
    "matrixark_mcp_retrieve_audit": "one function unique here",
    "matrixark_mcp_retrieve_event_scan": "one function unique here",
    "matrixark_mcp_retrieve_identity": "two functions, one unique here",
    "matrixark_mcp_retrieve_pack_policy": "two functions, both unique here",
    "matrixark_mcp_retrieve_resource_skill_scan": "one function unique here",
    "matrixark_mcp_retrieve_resources": "one function unique here",
    "matrixark_mcp_retrieve_scan_state": "five functions, all unique here",
    "matrixark_mcp_retrieve_segment_scan": "one function unique here",
    "matrixark_mcp_retrieve_temporal_window": "one function unique here",
    "matrixark_mcp_retrieve_tree_filter": "seven functions, five unique here",
    "matrixark_mcp_rust_direct_client": "25 definitions, all also defined live",
    "matrixark_mcp_rust_proxy_client": "29 KB, 48 definitions, fourteen unique here",
    "matrixark_mcp_server_metrics": "four functions, one unique here",
    "matrixark_mcp_temporal_audit": "five functions, one unique here",
    "matrixark_mcp_temporal_proxy_readiness": "one function, also defined live",
    "matrixark_mcp_temporal_readiness": "two functions, both unique here",
    "matrixark_mcp_time_compression_runtime": "21 KB, six functions, one unique here",
    "matrixark_mcp_visibility": "three functions, one unique here",
    "run_matrixark_scale_hostpath": "eleven functions and no entry-point guard",
    "run_matrixark_scale_resource": "six functions and no entry-point guard",
}

#: 330 non-test modules under tools/ when this was written.
EXPECTED_MODULE_FLOOR = 250

#: A module every tree has and every tree imports, so a scan that reports IT as unreachable is
#: broken rather than reporting news. Without a positive control an emptied scan reads as a clean
#: tree.
POSITIVE_CONTROL = "matrixark_mcp_local_adapter"

#: The other control. A module this tree really does not import, asserted to STAY found, so a scan
#: that quietly stops matching fails instead of reporting that all is well.
NEGATIVE_CONTROL = "matrixark_mcp_registry"

_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


#: This file, relative to the repository. It lists the module names it decides about, which is
#: exactly the shape its own scan counts as a reference -- so once it was committed and became a
#: tracked file, it credited every one of them with being imported and reported a clean tree. The
#: same self-reference that made an earlier guard feed on its own output (mx#910), and the reason
#: for the control below.
_SELF = os.path.join("tools", os.path.basename(__file__))


def _tracked() -> List[str]:
    listed = subprocess.run(["git", "ls-files"], cwd=REPO,
                            capture_output=True, text=True).stdout.split()
    return [path for path in listed if path != _SELF]


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
    for path in _tracked():
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

    def test_a_module_nothing_imports_is_still_found(self) -> None:
        """Catches the opposite, which the control above cannot see.

        A scan that finds NOTHING passes every assertion here: the new-orphan list is empty and the
        control module is not in it. That is how this file broke itself -- committing it made it a
        tracked file, its dict keys read as references to all 34, and it reported a clean tree.

        So one known orphan is asserted to still be found. It fails if the scan goes blind, whatever
        made it blind.
        """
        found = _orphans()
        self.assertTrue(found, "the scan found no orphans at all, which this tree is not")
        self.assertIn(
            NEGATIVE_CONTROL, found,
            "%s is imported by nothing and the scan no longer says so. Either it was wired up -- in "
            "which case strike it off the list -- or the scan has stopped seeing, and every "
            "assertion here now passes for the wrong reason" % NEGATIVE_CONTROL)

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
