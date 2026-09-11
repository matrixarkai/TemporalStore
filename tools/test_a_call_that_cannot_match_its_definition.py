#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A call whose arguments cannot match the definition it names.

This tree has shipped this exact shape before and not noticed for months: `identity_hashes` is
called with one dict where its signature takes two required parameters, the `TypeError` lands in
an `except Exception: pass`, and the hashed user alias it was meant to produce **has never once
been produced**. Nothing failed, nothing logged, and the feature simply was not there.

A type checker would find these. There is no type checker in this repository's gates, and adding
one means triaging thousands of pre-existing annotations before anything else can land. This asks
the narrowest version of the same question -- can this call possibly bind? -- over the whole tree
in a few seconds, and records the two that cannot.

WHAT IS REPORTED, and why each restriction is here
--------------------------------------------------
Only a call where all of these hold:

  * the calling module DEFINES the name at its own top level, or IMPORTS it by name from a module
    that does. A first pass resolved a bare name against any top-level definition anywhere in
    `tools/` and matched `first_value` in `matrixark_local_adapter_retrieve` against a definition
    in an unrelated inspection script -- 80 findings, 78 of them that mistake.
    `test_a_name_is_not_resolved_across_unrelated_modules` pins it.
  * nothing in the calling module SHADOWS the name -- no nested `def`, no method, no local
    binding. An underscore or a nesting level is not a scope this scan may ignore.
  * the definition takes no `*args` and no `**kwargs`, so its arity is a fixed range.
  * the call passes no `*splat` and no `**splat`.

Anything else is skipped. Over-reporting here costs more than under-reporting: a wrong finding
sends someone to a call that is fine, and teaches the next reader to wave this test through.

TEST MODULES ARE NOT SCANNED. A test may call a function wrongly on purpose, to prove it raises.
"""
from __future__ import annotations

import ast
import io
import os
import subprocess
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

#: Calls that cannot bind, recorded exactly. A new one fails here rather than joining them, and
#: one that is fixed fails too, because a list allowed to go stale describes a tree that no longer
#: exists.
CANNOT_MATCH = {
    # The recorded bug. `identity_hashes(scope, kind)` is called with the scope alone, so every
    # call raises TypeError into an `except Exception: pass` and the hashed user alias has never
    # been produced. Kept here as the positive control as well: a scan that stops finding this has
    # stopped working.
    ("matrixark_tenant_policy.py", "identity_hashes"): "fills 1 of 2 required parameters",
    # An unreachable module calling the wrong one of two same-named builders.
    # `matrixark_mcp_ingest_resource_summary` imports `context_index_posting_record` from
    # `matrixark_mcp_core`, whose copy has a required `data_model` and no `capability` at all --
    # the `capability` spelling belongs to the `matrixark_mcp_indexing` copy. Verified by calling
    # it: "got an unexpected keyword argument 'capability'". Both that module and its only
    # importer are in the `ingest` cluster of `test_a_module_only_tests_reach_is_not_live`, so
    # this is not live -- it is evidence that the cluster cannot run, not only that it does not.
    ("matrixark_mcp_ingest_resource_summary.py", "context_index_posting_record"):
        "passes unknown keyword(s) ['capability']",
}


def _modules():
    """The TRACKED non-test modules under tools/, parsed.

    Tracked rather than listed: an untracked scratch module in this directory otherwise becomes
    part of the tree this scan reasons about -- it can add a finding, and it can supply a
    definition that changes how a call resolves. There is no fallback when git is absent; the
    empty result fails the floor in `test_the_scan_resolves_enough_calls`, which is the honest
    outcome.
    """
    listed = subprocess.run(["git", "ls-files", "-z", "tools/*.py"], cwd=REPO,
                            capture_output=True, text=True).stdout.split("\0")
    trees = {}
    for relative in listed:
        name = os.path.basename(relative)
        if not name.endswith(".py") or name.startswith("test_"):
            continue
        try:
            with io.open(os.path.join(REPO, relative), encoding="utf-8", errors="replace") as handle:
                trees[name] = ast.parse(handle.read())
        except (OSError, SyntaxError):
            continue
    return trees


def _top_level_functions(tree):
    return {node.name: node for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _bound_anywhere(tree) -> set:
    """Every name bound anywhere in the module -- nested defs, classes, parameters, assignments."""
    bound = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                args = node.args
                for arg in args.posonlyargs + args.args + args.kwonlyargs:
                    bound.add(arg.arg)
                if args.vararg:
                    bound.add(args.vararg.arg)
                if args.kwarg:
                    bound.add(args.kwarg.arg)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                for sub in ast.walk(target):
                    if isinstance(sub, ast.Name):
                        bound.add(sub.id)
        elif isinstance(node, (ast.For, ast.comprehension)):
            for sub in ast.walk(node.target):
                if isinstance(sub, ast.Name):
                    bound.add(sub.id)
    return bound


def _imported_from(tree) -> dict:
    """name -> defining module file, for `from X import name`, any of the try/except spellings."""
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            source = node.module.split(".")[-1] + ".py"
            for alias in node.names:
                if alias.asname is None and alias.name != "*":
                    out[alias.name] = source
    return out


def _arity(node):
    args = node.args
    if args.vararg or args.kwarg:
        return None
    positional = [arg.arg for arg in args.posonlyargs + args.args]
    required = len(positional) - len(args.defaults)
    keyword_only = {arg.arg for arg in args.kwonlyargs}
    keyword_required = {arg.arg for arg, default in zip(args.kwonlyargs, args.kw_defaults)
                        if default is None}
    return required, positional, keyword_only, keyword_required


def _problem(call, shape):
    required, positional, keyword_only, keyword_required = shape
    given_positional = len(call.args)
    given_keywords = {keyword.arg for keyword in call.keywords}
    filled = given_positional + len(given_keywords & set(positional))
    if given_positional > len(positional):
        return "passes %d positional where at most %d are accepted" % (
            given_positional, len(positional))
    if filled < required:
        return "fills %d of %d required parameters" % (filled, required)
    unknown = given_keywords - set(positional) - keyword_only
    if unknown:
        return "passes unknown keyword(s) %s" % sorted(unknown)
    missing = keyword_required - given_keywords
    if missing:
        return "omits required keyword-only %s" % sorted(missing)
    return None


def _scan():
    trees = _modules()
    top_level = {name: _top_level_functions(tree) for name, tree in trees.items()}
    found = {}
    resolved = 0
    for module, tree in trees.items():
        bound = _bound_anywhere(tree)
        imports = _imported_from(tree)
        own = top_level[module]
        definitions_here = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                definitions_here[node.name] = definitions_here.get(node.name, 0) + 1
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                continue
            name = node.func.id
            if any(isinstance(arg, ast.Starred) for arg in node.args):
                continue
            if any(keyword.arg is None for keyword in node.keywords):
                continue
            if name in own:
                target = own[name]
            elif (name in imports and imports[name] in top_level
                  and name in top_level[imports[name]]):
                if name in bound:
                    continue            # a local binding shadows the imported name
                target = top_level[imports[name]][name]
            else:
                continue
            if definitions_here.get(name, 0) > (1 if name in own else 0):
                continue                # a nested def or method of the same name shadows it
            shape = _arity(target)
            if shape is None:
                continue
            resolved += 1
            problem = _problem(node, shape)
            if problem:
                found[(module, name)] = problem
    return found, resolved, len(trees)


class ACallThatCannotMatchItsDefinitionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.found, cls.resolved, cls.modules = _scan()

    def test_the_scan_resolves_enough_calls(self):
        """Zero findings is also what a scan that resolved nothing prints."""
        self.assertGreaterEqual(self.modules, 250,
                                "only %d modules parsed, against roughly 290 non-test modules in "
                                "tools/. `git ls-files` returning little or nothing is what a "
                                "missing repository looks like, and this scan has no fallback on "
                                "purpose" % self.modules)
        self.assertGreaterEqual(self.resolved, 6000,
                                "only %d calls resolved to an unshadowed definition, against "
                                "roughly 8,000; the resolution rules have stopped matching the "
                                "tree" % self.resolved)

    def test_the_scan_still_catches_the_recorded_one(self):
        """`identity_hashes` is a real defect this scan must keep finding.

        It is called with one argument where two are required, the TypeError is swallowed, and the
        feature behind it has never run. If this stops being reported, the scan is broken -- not
        the tree fixed, which would show up as a failure below instead.
        """
        self.assertIn(("matrixark_tenant_policy.py", "identity_hashes"), self.found)

    def test_a_name_is_not_resolved_across_unrelated_modules(self):
        """The false positive that made the first version of this useless.

        `matrixark_local_adapter_retrieve` calls `first_value` sixteen times; a definition of that
        name exists in `inspect_matrixark_codex_hook_records`, which it does not import. Resolving
        across modules by name alone reported all sixteen.
        """
        self.assertNotIn(("matrixark_local_adapter_retrieve.py", "first_value"), self.found)

    def test_the_set_is_exactly_what_is_recorded(self):
        self.assertEqual(
            {key: value for key, value in sorted(CANNOT_MATCH.items())},
            {key: value for key, value in sorted(self.found.items())},
            "a call that cannot bind was added, or one on the list can bind now. A call on this "
            "list does not raise at import -- it raises when it runs, and in this tree that has "
            "meant landing in an `except Exception: pass` and taking a feature with it.")


if __name__ == "__main__":
    unittest.main()
