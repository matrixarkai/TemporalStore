#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A helper defined inside a function or a class has one copy too.

test_there_is_one_copy_of_each_helper walks `tree.body`, so every duplicate it can see is a
module-level def. A helper nested inside a function, or a method on a class, is invisible to it --
and what that hides is not small: the largest duplicate in this tree today is a pair of 22- and
17-statement METHODS, byte-identical, one of them on a mixin nothing inherits.

The recorded pairs below may only SHRINK. A pair that gets consolidated fails
test_no_recorded_pair_has_quietly_been_fixed until it is removed from the list, so the list cannot
be padded to go green -- and a NEW pair fails test_no_new_nested_duplicate, which is the point.

Two things are deliberately NOT treated as defects here:

  * a pair where both modules are unreachable -- consolidating dead code moves nothing, and the
    reachability record already covers those modules;
  * a pair split across two backends, where the divergence may be the intent rather than an
    accident. None of the pairs below is in that category, but the reason field is where that
    would be recorded.
"""
from __future__ import annotations

import ast
import collections
import os
import pathlib
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent

#: The existing guard's floor, kept deliberately: a body this small is not worth sharing, and
#: forcing it into a helper costs more than the copy does.
MIN_BODY_STATEMENTS = 4

#: {frozenset(module stems): reason}. One entry per duplicated body.
RECORDED_PAIRS = {
    frozenset({"matrixark_local_adapter_retrieval", "matrixark_local_adapter_retrieve"}):
        "Two copies of the node-path scope recovery (tenant/user/session) and of the "
        "profile-summary match, one of each nested inside a method. Both modules are live. A "
        "third copy of the same LOOP is embedded in matrixark_mcp_core.candidate_access_scope, "
        "which body-level matching cannot group because the surrounding function differs. Scope "
        "recovery decides which tenant a record belongs to, so drift between these is worth more "
        "than the consolidation costs.",
    frozenset({"matrixark_mcp_async_ingest", "matrixark_mcp_session_runtime"}):
        "add_count, 9 statements. BOTH modules are recorded unreachable, so this is dead-vs-dead: "
        "consolidating it would move nothing and would promote one dead copy over another.",
}


def _body_statements(node):
    return [s for s in node.body
            if not (isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant))]


def duplicate_groups(sources):
    """{body text: [(module stem, function name, line, nested)]} for bodies seen more than once."""
    groups = collections.defaultdict(list)
    for stem, tree in sources.items():
        module_level = {id(n) for n in tree.body}
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = _body_statements(node)
            if len(body) < MIN_BODY_STATEMENTS:
                continue
            text = "\n".join(ast.unparse(s) for s in body)
            groups[text].append((stem, node.name, node.lineno, id(node) not in module_level))
    return {text: where for text, where in groups.items() if len(where) > 1}


def _production_sources():
    sources = {}
    for entry in sorted(os.listdir(TOOLS)):
        if not entry.endswith(".py") or entry.startswith(("test_", "run_", "validate_")):
            continue
        try:
            sources[pathlib.Path(entry).stem] = ast.parse(
                (TOOLS / entry).read_text(encoding="utf-8"))
        except (SyntaxError, OSError, UnicodeDecodeError):
            continue
    return sources


def same_module_duplicates():
    """{module stem: [(name, name, ...)]} for identical bodies that never leave one file.

    The two duplicate-body guards in this tree both require the copies to sit in two different
    modules -- this file groups by `frozenset(module stems)` and refuses a group of one, and
    `test_there_is_one_copy_of_each_helper` does the same. The comment there calls a pair inside
    one module "a different fault", and it is, but nothing was reading it.

    `matrixark_pipeline_task_slim` held `_task_scope_key` and `_audit_scope_key`: one function
    under two names, five statements each, byte for byte the same, one caller apiece thirty lines
    apart.
    """
    found = {}
    for stem, tree in _production_sources().items():
        groups = collections.defaultdict(list)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            body = _body_statements(node)
            if len(body) < MIN_BODY_STATEMENTS:
                continue
            groups["\n".join(ast.unparse(s) for s in body)].append(node.name)
        pairs = sorted(tuple(sorted(set(names))) for names in groups.values() if len(names) > 1)
        if pairs:
            found[stem] = pairs
    return found


def nested_duplicate_pairs():
    """{frozenset(module stems): [names]} for duplicates with at least one NESTED copy.

    A duplicate whose copies are all module-level belongs to the older guard, not this one.
    """
    found = collections.defaultdict(set)
    for _text, where in duplicate_groups(_production_sources()).items():
        if not any(nested for *_rest, nested in where):
            continue
        modules = frozenset(stem for stem, _n, _l, _nested in where)
        if len(modules) < 2:
            continue
        for _stem, name, _line, _nested in where:
            found[modules].add(name)
    return found


class ANestedHelperHasOneCopyToo(unittest.TestCase):

    def test_the_scan_reaches_nested_functions_at_all(self):
        """The floor. This whole file is about functions the older guard cannot see, so a scan
        that only reached module-level defs would report a clean tree for the wrong reason."""
        sources = _production_sources()
        self.assertGreater(len(sources), 100,
                           "found %d production modules, expected the whole tools tree"
                           % len(sources))
        nested = 0
        for _stem, tree in sources.items():
            module_level = {id(n) for n in tree.body}
            for node in ast.walk(tree):
                if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and id(node) not in module_level):
                    nested += 1
        self.assertGreater(nested, 500,
                           "found only %d nested functions in the tree -- the scan is not "
                           "descending into functions and classes" % nested)

    def test_the_matcher_separates_bodies_that_differ(self):
        """The negative control. A matcher that grouped everything would report every pair as a
        duplicate and every recorded reason as still true."""
        same = ast.parse(
            "def a():\n    x = 1\n    y = 2\n    z = 3\n    return x + y + z\n"
            "def b():\n    x = 1\n    y = 2\n    z = 3\n    return x + y + z\n")
        different = ast.parse(
            "def a():\n    x = 1\n    y = 2\n    z = 3\n    return x + y + z\n"
            "def b():\n    x = 1\n    y = 2\n    z = 4\n    return x + y + z\n")
        self.assertEqual(1, len(duplicate_groups({"same": same})),
                         "two identical bodies did not group, so this guard sees no duplicates")
        self.assertEqual({}, duplicate_groups({"different": different}),
                         "two bodies differing by one statement grouped, so every pair would look "
                         "like a duplicate")

    def test_no_new_nested_duplicate(self):
        found = nested_duplicate_pairs()
        unrecorded = sorted(
            "%s (%s)" % (" + ".join(sorted(modules)), ", ".join(sorted(names)))
            for modules, names in found.items() if modules not in RECORDED_PAIRS)
        self.assertEqual(
            [], unrecorded,
            "these helpers are defined more than once, with at least one copy nested where the "
            "module-level guard cannot see it: %s" % "; ".join(unrecorded))

    def test_no_recorded_pair_has_quietly_been_fixed(self):
        """Tighten both ways: a pair that no longer duplicates must come off the list."""
        found = nested_duplicate_pairs()
        stale = sorted(" + ".join(sorted(modules))
                       for modules in RECORDED_PAIRS if modules not in found)
        self.assertEqual(
            [], stale,
            "these are recorded as duplicated but no longer are -- take them out, so the list "
            "cannot carry a name that hides the next one: %s" % "; ".join(stale))

    def test_the_older_latest_state_variant_does_not_come_back(self):
        """The seven latest context-state methods have one home, and it is the backend.

        `LatestContextStateAdapterMixin` used to carry a second copy of all seven, extracted and
        never adopted -- named exactly once in the repository, its own class line. This entry
        used to read "do not resolve this by adopting the mixin", because two of the seven bodies
        agreed and on the other five the backend carried logic the mixin did not: the mixin's
        `latest_context_state_payload` serialised the whole record where the backend serialises
        `slim_persisted_record(record)` first, and `_with_latest_context_state_records` and
        `_split_compacted_latest_context_state` both run on the retrieval hot path.

        So the copy went instead of being shared, and this is what keeps it gone. A guard that
        only DESCRIBES dead code also keeps it reachable to a word scan, which is how twelve
        definitions stayed off the reachability list next door.
        """
        sources = _production_sources()
        carrying = sorted(
            stem for stem, tree in sources.items()
            if any(isinstance(node, ast.ClassDef) and node.name == "LatestContextStateAdapterMixin"
                   for node in ast.walk(tree)))
        self.assertEqual(
            [], carrying,
            "%s is defined again in %s. The backend's seven methods are the variant that ships; "
            "a second copy of them is not a shared home, it is the older one."
            % ("LatestContextStateAdapterMixin", ", ".join(carrying)))

    def test_no_module_holds_the_same_body_twice(self):
        """A copy that never leaves its file is still a copy, and was the one nobody looked for.

        Asserted EMPTY rather than ratcheted: the class had exactly one member when this was
        written and it was consolidated in the same change, so there is nothing to record and a
        new one should fail rather than be listed.
        """
        found = same_module_duplicates()
        self.assertEqual(
            {}, found,
            "these modules define one body under more than one name. Neither duplicate-body guard "
            "sees this -- both need the copies in two different modules -- so it is worth fixing "
            "at the first instance rather than recording: %r" % (found,))

    def test_the_same_module_scan_can_find_something(self):
        """A positive control. Empty is also what a scan that stopped parsing reports."""
        sources = _production_sources()
        self.assertGreater(len(sources), 100,
                           "the production corpus came back nearly empty, so the check above "
                           "passes over nothing")

    def test_every_recorded_pair_says_why(self):
        thin = sorted(" + ".join(sorted(modules)) for modules, reason in RECORDED_PAIRS.items()
                      if len(reason.strip()) < 80)
        self.assertEqual(
            [], thin,
            "a recorded duplicate with no reason beside it is a skip list: %s" % "; ".join(thin))


if __name__ == "__main__":
    unittest.main()
