#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A deduplicated helper keeps every name it resolved under, and gains one implementation.

This repository is Apache-2.0 and forked. A top-level name is importable, so it is public surface
whether or not anything HERE imports it -- and that is not a hypothetical: when two modules both
defined `compact_local_context_refs`, consolidating them to one implementation left
`matrixark_mcp_budget_pack.compact_local_context_refs` resolving ONLY because the module
re-exports it. Deleting that re-export instead breaks an importer outside this repository and
**fails no test in it**. Measured: removing the re-export, and removing one of the two names from
it, both leave the whole suite green.

So the rule this file enforces is the one the dedup work runs on:

    one IMPLEMENTATION, never one IMPORT PATH

Each entry below records a name, the module whose body is the single implementation, and every
other module the name must still resolve from. The assertions are that the name resolves from all
of them, and that they are all the SAME function object -- because two names resolving is only
half the promise; if they resolved to two bodies again the duplication would be back.

WHY A LIST AND NOT A SCAN. A scan would ask "which names are re-exported", and answer with
whatever the tree currently does -- which cannot distinguish a re-export somebody removed from one
that was never there. The list is the promise; the tree is checked against it.

WHAT THIS FILE CANNOT DO. It cannot know what a fork imports. It pins the paths that resolved when
each consolidation was made, which is the most this repository can honestly assert on a stranger's
behalf. A name never published here was never public, and is not listed.
"""
from __future__ import annotations

import importlib
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

#: Importing this first resolves the cycle these modules sit in.
#:
#: They cannot be imported standalone -- measured: `import matrixark_mcp_core_packing` on its own
#: raises ImportError on a partially initialised `matrixark_mcp_core`. The obvious fix is to put
#: the repository root on `sys.path` so the `tools.` package spelling resolves, and that is the
#: wrong fix: `sys.path` is process-wide, so it would make the package spelling resolve for every
#: OTHER test in the run too, and a module that only works under the package path would stop
#: failing. Measured while writing this file -- a mutation that removed a bare-spelling fallback
#: elsewhere went from caught to missed once this file widened the path. Importing the parent
#: costs nothing and changes nothing outside this module.
CYCLE_PARENT = "matrixark_mcp_core"

#: Names published through their home module's `__all__` when they were consolidated.
#:
#: RECORDED, not read back. The obvious spelling of this check -- "if the name is in `__all__`,
#: assert it is in `__all__`" -- skips the one case it exists for, because a name that has been
#: dropped fails the condition and is never asserted on. That is a test that cannot fail.
PUBLISHED_THROUGH_ALL = frozenset({
    "compact_local_context_refs",
    "local_context_refs_for_pack",
    "_cloud_resource_bucket",
})

#: name -> (module holding the one implementation, other modules it must resolve from)
CONSOLIDATED = {
    "compact_local_context_refs": (
        "matrixark_mcp_core_packing", ("matrixark_mcp_budget_pack",)),
    "local_context_refs_for_pack": (
        "matrixark_mcp_core_packing", ("matrixark_mcp_budget_pack",)),
    # A LEADING UNDERSCORE IS A CONVENTION, NOT A SCOPE, and here it is overruled explicitly:
    # `_cloud_resource_bucket` is listed in matrixark_mcp_core_resource_io.__all__, so it is
    # published on purpose despite the underscore. Both modules defined it identically; the
    # published one is the home.
    "_cloud_resource_bucket": (
        "matrixark_mcp_core_resource_io", ("matrixark_mcp_resources",)),
}


def _import(stem):
    """Import under whichever spelling this run is using, with the cycle resolved first.

    The parent is imported before the module, because these modules cannot be imported standalone.
    Both spellings are then tried, because `tools.X` and bare `X` are different module objects and
    which one exists depends on how the process was started.
    """
    importlib.import_module(CYCLE_PARENT)
    try:
        return importlib.import_module("tools." + stem)
    except ImportError:  # Direct script execution from tools/.
        return importlib.import_module(stem)


class AConsolidatedHelperStillResolvesEverywhere(unittest.TestCase):

    def test_the_record_is_not_empty(self) -> None:
        """A floor. Every assertion below passes over an empty list."""
        self.assertTrue(
            CONSOLIDATED,
            "nothing is recorded, so this file asserts nothing. A consolidation that removed its "
            "own entry rather than keeping the promise would look exactly like this")

    def test_every_recorded_name_still_resolves_from_every_module(self) -> None:
        """The promise: a name that resolved before still resolves."""
        for name, (home, others) in sorted(CONSOLIDATED.items()):
            for stem in (home,) + tuple(others):
                with self.subTest(name=name, module=stem):
                    module = _import(stem)
                    self.assertTrue(
                        hasattr(module, name),
                        "%s.%s no longer resolves. If the implementation moved, the name still "
                        "has to be re-exported from here -- deleting it breaks an importer "
                        "outside this repository and fails nothing inside it" % (stem, name))

    def test_every_recorded_name_is_one_implementation(self) -> None:
        """The other half: resolving from two modules must not mean two bodies again."""
        for name, (home, others) in sorted(CONSOLIDATED.items()):
            implementation = getattr(_import(home), name)
            for stem in others:
                with self.subTest(name=name, module=stem):
                    self.assertIs(
                        implementation, getattr(_import(stem), name),
                        "%s.%s is not the same object as %s.%s. A second body has come back, "
                        "which is the duplication this was consolidated to remove"
                        % (stem, name, home, name))

    def test_the_implementation_lives_where_the_record_says(self) -> None:
        """Compared on the last dot-segment: the same file loads as `tools.X` and as bare `X`,
        and those are different module objects with different `__module__` strings."""
        for name, (home, _others) in sorted(CONSOLIDATED.items()):
            with self.subTest(name=name):
                owner = getattr(_import(home), name).__module__.rsplit(".", 1)[-1]
                self.assertEqual(
                    home, owner,
                    "%s is recorded as implemented in %s and is implemented in %s"
                    % (name, home, owner))


    def test_a_name_published_through_all_stays_published(self) -> None:
        """`__all__` is not decoration: it is what `from X import *` re-exports.

        `matrixark_mcp_core` pulls several of these modules in with `import *`, so a name dropped
        from a home module's `__all__` stops resolving from core even though the home still
        defines it. That is a public-surface change no import of the home module itself would
        reveal, and nothing else here catches it -- measured.
        """
        for name in sorted(PUBLISHED_THROUGH_ALL):
            home = CONSOLIDATED[name][0]
            with self.subTest(name=name, module=home):
                exported = getattr(_import(home), "__all__", None)
                self.assertIsNotNone(
                    exported,
                    "%s no longer declares __all__, so %s is no longer published through it"
                    % (home, name))
                self.assertIn(
                    name, exported,
                    "%s was published through %s.__all__ and no longer is. Anything reaching it "
                    "through a star-import of that module loses it" % (name, home))

    def test_every_published_name_is_recorded(self) -> None:
        """The control on the list above: it must name only consolidated helpers."""
        unknown = sorted(PUBLISHED_THROUGH_ALL - set(CONSOLIDATED))
        self.assertEqual(
            [], unknown,
            "%s are recorded as published but are not consolidated helpers, so nothing else in "
            "this file says where they live" % ", ".join(unknown))


if __name__ == "__main__":
    unittest.main()
