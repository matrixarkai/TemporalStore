#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The module named for access scope holds the POORER copy of the access-scope builder.

`candidate_access_scope` exists twice. `matrixark_mcp_core`'s copy recovers a scope from `node_path`
segments -- `tenant:`, `user:`, `session:`. `matrixark_mcp_access_scope`'s copy has no such branch
and falls through to the envelope.

That is not a cosmetic difference. On a record carrying BOTH a scoped node_path and an envelope
scope, the two copies name DIFFERENT TENANTS for the same record:

    {"node_path": ["tenant:acme"], "envelope": {"scope": {"tenant_id": "other"}}}
        core   -> {"tenant_id": "acme"}
        access -> {"tenant_id": "other"}

And on a record with only a scoped node_path, core recovers the scope while the other returns {} --
which `scope_matches` answers False for, so the poorer copy fails closed and the record disappears.

NOTHING IS BROKEN TODAY, and this file is a tripwire rather than a bug report. Measured across the
tree: every live caller of `access_scope_matches_before_scoring` -- the only caller of the poorer
`candidate_access_scope` -- imports it from `matrixark_mcp_core`, directly or by star-import. The
pair inside `matrixark_mcp_access_scope` is a closed island: one function calling the other, with no
importer.

WHY IT IS WORTH A GUARD ANYWAY. The island sits in a module that IS live -- `scope_matches`,
`session_continuity_status`, `sharing_scope_from_candidate`, `session_continuity_boost` and
`cross_session_rerank_adjustment` are all imported from it -- and it is named
`matrixark_mcp_access_scope`, which is exactly where someone looking for an access-scope builder
would import from. Taking `candidate_access_scope` from the module whose name promises it is the
natural mistake, and it silently narrows visibility or reattributes a record to another tenant.

Core's own re-export block records why this pair was left behind: it was built for functions that
were "an identical second copy of each", and re-exporting those is "a no-op rather than a swap".
These two are not identical, so the consolidation that fixed the others skipped them -- the very
criterion that made it safe is what left this pair in place.

This file changes no behaviour.
"""
from __future__ import annotations

import ast
import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

try:
    from tools import matrixark_mcp_access_scope as access  # noqa: F401
    from tools import matrixark_mcp_core as core
except ImportError:  # run from tools/
    import matrixark_mcp_access_scope as access
    import matrixark_mcp_core as core

#: The two names that must not be imported out of the access-scope module while its copies differ.
ISLAND = ("candidate_access_scope", "access_scope_matches_before_scoring")
ISLAND_MODULE = "matrixark_mcp_access_scope"

#: A record with a scoped node_path and nothing else.
NODE_PATH_ONLY = {
    "record_type": "context_event",
    "node_path": ["tenant:acme", "user:dana", "session:s-1"],
}
#: The sharp one: node_path and envelope name different tenants.
NODE_PATH_AND_ENVELOPE = {
    "record_type": "context_event",
    "node_path": ["tenant:acme"],
    "envelope": {"scope": {"tenant_id": "other"}},
}
#: A record both copies agree on, so a disagreement below is not just "they always differ".
EXPLICIT_SCOPE = {"access_scope": {"tenant_id": "acme"}, "node_path": ["tenant:zzz"]}


def _imports_of(name: str):
    """Modules that import `name` out of the access-scope module."""
    offenders = []
    for path in sorted(TOOLS.glob("*.py")):
        if path.name.startswith("test_") or path.stem == ISLAND_MODULE:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if name not in text:
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            if node.module.split(".")[-1] != ISLAND_MODULE:
                continue
            if any(alias.name == name or alias.name == "*" for alias in node.names):
                offenders.append((path.stem, node.lineno))
    return offenders


class TheAccessScopeModuleHoldsThePoorerCopy(unittest.TestCase):

    def test_the_two_copies_are_still_two(self):
        """The floor. If they became one object there is nothing left to trip over."""
        self.assertIsNot(
            core.candidate_access_scope, access.candidate_access_scope,
            "the two copies are now the same object -- if they were consolidated, delete this file "
            "rather than leaving a tripwire for a hazard that no longer exists")

    def test_the_copies_disagree_about_a_scoped_node_path(self):
        """The positive control, and the reason the tripwire exists.

        Also asserts they AGREE on a record with an explicit scope, so the disagreement below is
        specific rather than the two copies simply having drifted apart everywhere.
        """
        self.assertEqual(
            core.candidate_access_scope(EXPLICIT_SCOPE),
            access.candidate_access_scope(EXPLICIT_SCOPE),
            "the copies now disagree even when the record carries an explicit access_scope, so "
            "they differ by more than the node_path branch and this file understates the problem")
        recovered = core.candidate_access_scope(NODE_PATH_ONLY)
        self.assertEqual(
            {"tenant_id": "acme", "user_id": "dana", "session_id": "s-1"}, recovered,
            "matrixark_mcp_core no longer recovers a scope from node_path; it returned %r. That is "
            "the richer behaviour every live caller depends on" % (recovered,))
        self.assertEqual(
            {}, access.candidate_access_scope(NODE_PATH_ONLY),
            "matrixark_mcp_access_scope now recovers a node_path scope too. If the branch was added "
            "deliberately the copies agree and this file should go; check the tenant case below "
            "first")

    def test_the_copies_can_name_different_tenants_for_one_record(self):
        """Why this is worse than 'one is narrower'."""
        from_core = core.candidate_access_scope(NODE_PATH_AND_ENVELOPE)
        from_access = access.candidate_access_scope(NODE_PATH_AND_ENVELOPE)
        self.assertEqual({"tenant_id": "acme"}, from_core)
        self.assertEqual({"tenant_id": "other"}, from_access)
        self.assertNotEqual(
            from_core.get("tenant_id"), from_access.get("tenant_id"),
            "the two copies now agree on the tenant for a record whose node_path and envelope "
            "disagree; the sharpest reason for this guard has gone and it should be re-read")

    def test_an_empty_scope_fails_closed(self):
        """Which direction the poorer copy errs in, asserted rather than assumed.

        If `scope_matches({}, query)` ever became True, the poorer copy would WIDEN visibility
        instead of narrowing it, which is a different and much more serious situation.
        """
        query = {"tenant_id": "acme", "user_id": "dana"}
        self.assertFalse(
            core.scope_matches({}, query),
            "an empty access scope now MATCHES a query scope. The poorer copy returns {} for a "
            "record with a scoped node_path, so it would now widen visibility rather than hide the "
            "record -- re-read this file's conclusion before relying on it")
        self.assertTrue(
            core.scope_matches({"tenant_id": "acme", "user_id": "dana"}, query),
            "a matching scope no longer matches, so the check above proves nothing")

    def test_nothing_imports_the_island_out_of_the_access_scope_module(self):
        """The tripwire itself."""
        offenders = []
        for name in ISLAND:
            offenders.extend((name, module, line) for module, line in _imports_of(name))
        self.assertEqual(
            [], offenders,
            "%d module(s) now import an access-scope name out of %s, which serves the copy with no "
            "node_path branch: %s. That copy returns {} for a record with a scoped node_path and "
            "can attribute a record to a different tenant. Import these from matrixark_mcp_core, "
            "or consolidate the two copies -- see the next test for the condition that makes "
            "consolidating safe."
            % (len(offenders), ISLAND_MODULE,
               "; ".join("%s <- %s:%d" % (n, m, line) for n, m, line in offenders)))

    def test_the_condition_that_would_make_consolidating_safe_still_holds(self):
        """Recorded so a future consolidation does not have to re-derive it.

        A body is only as identical as the names it calls. Both copies call
        `scope_from_serving_record`, and core's also calls nothing else of consequence; while those
        resolve to the SAME object from both modules, moving the node_path branch across changes
        only the branch. If they ever diverge, consolidating stops being a no-op.
        """
        self.assertIs(
            core.scope_from_serving_record, access.scope_from_serving_record,
            "scope_from_serving_record no longer resolves to the same object from both modules "
            "(core: %s, access: %s), so consolidating the two candidate_access_scope copies would "
            "now change behaviour for core's callers as well as moving the branch"
            % (getattr(core.scope_from_serving_record, "__module__", "?"),
               getattr(access.scope_from_serving_record, "__module__", "?")))
        self.assertIs(
            core.scope_matches, access.scope_matches,
            "scope_matches no longer resolves to the same object from both modules")


if __name__ == "__main__":
    unittest.main()
