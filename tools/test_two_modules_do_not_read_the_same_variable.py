#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""matrixark_mcp_core and matrixark_mcp_runtime_config define 77 of the same constants, identically.

Each of those is one environment variable read twice, at two import times, into two names that
happen to agree. They agree today because the text is character-for-character the same in both
files -- which is not a mechanism, it is a coincidence maintained by hand, and the cost lands on
whoever edits one of them.

TWO OF THE 77 WERE WORSE THAN DUPLICATED, THEY WERE SHADOWED. matrixark_mcp_core imports
CONTEXT_PACK_DEBUG_REFS and AUDIT_DEBUG_PAYLOAD from matrixark_mcp_runtime_config and then, fifteen
lines later, re-read both from the environment. The imported values were overwritten before
anything could use them, so the import was dead and a change to either definition in
runtime_config could never have reached core. Both are removed; this file is what stops a third.

WHY A RATCHET AND NOT A SWEEP. Rewriting all 77 at once is the mechanical rewrite this tree has
been burned by: every guard that reads "module X reads flag Y" changes answer on the same commit,
and a real divergence hiding among 77 identical-looking moves is invisible in the diff. The number
may only go DOWN, one reading at a time, and the one that is NOT identical is named below so it is
never swept in with the rest.
"""
from __future__ import annotations

import ast
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

CORE = "matrixark_mcp_core.py"
RUNTIME = "matrixark_mcp_runtime_config.py"

#: Constants defined identically in both modules. May only go DOWN.
#:
#: 77 -> 75 when matrixarkai#1577 removed the two shadowed re-reads, then -> 43 when the
#: thirty-two PURE LITERALS moved to one definition. Those thirty-two were separated on a
#: rule, not by taste: no environment read anywhere in the expression, so no guard that asks
#: "which module reads flag X" could change answer over them -- which is the objection this
#: file raises against sweeping, and it does not apply to a constant that reads no flag.
#:
#: The remaining 43 DO read the environment, and they stay one at a time.
RECORDED_DUPLICATED = 31

#: Defined in both and NOT identical, with the reason. `matrixark_mcp_core` binds
#: DEFAULT_MAX_CONTEXT_TOKENS to the value it imported from runtime_config under an alias, which is
#: the shape the other 77 should take -- the one place the pattern is already right.
DELIBERATELY_UNLIKE = {
    "DEFAULT_MAX_CONTEXT_TOKENS":
        "core assigns it from _RUNTIME_DEFAULT_MAX_CONTEXT_TOKENS, the aliased import. Not a "
        "second read -- the pattern the other 77 are missing",
}


def _constants(name):
    """NAME -> source text, for every module-scope assignment to an ALL-CAPS name."""
    src = io.open(os.path.join(TOOLS, name), encoding="utf-8").read()
    lines = src.splitlines(True)
    out = {}
    for node in ast.parse(src).body:
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = getattr(node.targets[0], "id", None)
        elif isinstance(node, ast.AnnAssign):
            target = getattr(node.target, "id", None)
        if target and target.isupper():
            out[target] = "".join(lines[node.lineno - 1:node.end_lineno]).strip()
    return out


def _shared():
    """(identical, different) constant names defined by both modules."""
    core, runtime = _constants(CORE), _constants(RUNTIME)
    same, other = [], []
    for name in sorted(set(core) & set(runtime)):
        (same if core[name] == runtime[name] else other).append(name)
    return same, other


def _shadowed_imports():
    """Names core imports from runtime_config and then re-assigns at module scope."""
    src = io.open(os.path.join(TOOLS, CORE), encoding="utf-8").read()
    tree = ast.parse(src)
    imported = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "runtime_config" in node.module:
            for alias in node.names:
                imported.setdefault(alias.asname or alias.name, node.lineno)
    out = []
    for node in tree.body:
        target = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = getattr(node.targets[0], "id", None)
        elif isinstance(node, ast.AnnAssign):
            target = getattr(node.target, "id", None)
        if target in imported and node.lineno > imported[target]:
            out.append((target, imported[target], node.lineno))
    return out


class TwoModulesDoNotReadTheSameVariableTest(unittest.TestCase):

    def test_the_scan_finds_both_modules(self) -> None:
        """Control on the input. A parse that returns nothing satisfies every check below."""
        core, runtime = _constants(CORE), _constants(RUNTIME)
        self.assertGreater(len(core), 50, "only %d constants parsed out of %s" % (len(core), CORE))
        self.assertGreater(len(runtime), 50,
                           "only %d constants parsed out of %s" % (len(runtime), RUNTIME))

    def test_no_import_from_the_runtime_config_is_overwritten(self) -> None:
        """A name imported and then re-read is an import nothing can use.

        Both instances of this read the same variable with the same default as the definition they
        overwrote, so they agreed -- which is what kept it invisible. The failure it sets up is a
        change to runtime_config that silently does not reach core.
        """
        shadowed = _shadowed_imports()
        self.assertEqual(
            [], shadowed,
            "matrixark_mcp_core imports these from matrixark_mcp_runtime_config and then assigns "
            "over them, so the import is dead: %s"
            % ", ".join("%s (imported line %d, overwritten line %d)" % row for row in shadowed))

    def test_the_duplicated_constants_do_not_increase(self) -> None:
        """The ratchet. One reading at a time; a sweep of 77 is unreviewable."""
        same, _other = _shared()
        self.assertLessEqual(
            len(same), RECORDED_DUPLICATED,
            "%d constants are now defined identically in both modules, up from %d. Each is one "
            "environment variable read twice into two names that agree by hand. Names now: %s"
            % (len(same), RECORDED_DUPLICATED, ", ".join(same)))

    def test_every_non_identical_shared_name_has_a_reason(self) -> None:
        """The ones that differ are the dangerous ones: same name, two behaviours, no note.

        Listing them by name is safe here in a way it is not elsewhere -- this file compares the
        SOURCE TEXT of two named modules, so it cannot feed itself the way a scan that counts
        mentions across the tree can.
        """
        _same, other = _shared()
        for name in other:
            with self.subTest(name=name):
                self.assertIn(
                    name, DELIBERATELY_UNLIKE,
                    "%s is defined differently in %s and %s and nothing says which is current"
                    % (name, CORE, RUNTIME))
        for name in DELIBERATELY_UNLIKE:
            with self.subTest(recorded=name):
                self.assertIn(name, other,
                              "%s is recorded as deliberately unlike and the two definitions now "
                              "agree, or one is gone -- drop the entry" % name)


if __name__ == "__main__":
    unittest.main()
