#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Four candidate caps are offered to every deployment and bind on only one retrieval path.

`retrieve` has two assemblies. The native one builds a request, and if the backend answers it
returns:

    matrixark_local_adapter_retrieve.py:994   native_pack = self.native_context_pack({...})
    :1074                                     if native_pack is not None:
    :1146                                         return ...          <- last statement of the block

The four caps are read at 1609-1633, after that block. So a request the native path answers never
reads them, and the native request does not carry them either -- `min_score` is the only retrieval
bound inside it, wired separately.

WHICH PATH RUNS IS NOT AN EDGE CASE. `default_mcp_backend()` returns `temporalstore-direct` under a
production profile and `temporalstore-rust` whenever a Rust CLI is configured; the comment beside it
says the JSONL `local` backend "stays the default only for pure dev". So on a deployed gateway these
four are offered on the portal, documented as ceilings on retrieval cost, and do not bind.

The help text on each now says so. This file asserts the STRUCTURE that makes it true, so the help
cannot quietly go stale:

  * the native block still returns before the caps are read, and
  * the native request still carries none of them.

If either stops being true -- somebody wires a cap into the native request, or moves the reads above
the branch -- this fails and the help has to be revisited. It is deliberately not a behaviour
change: putting a candidate cap on the native path changes what is retrieved, which is a decision
about recall rather than a correction.
"""
from __future__ import annotations

import ast
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

RETRIEVE = "matrixark_local_adapter_retrieve.py"
CAPS = ("top_k_per_layer", "max_candidates_per_node", "max_global_candidates",
        "max_children_scored_per_parent")
CAP_ENVS = ("MATRIXARK_TOP_K_PER_LAYER", "MATRIXARK_MAX_CANDIDATES_PER_NODE",
            "MATRIXARK_MAX_GLOBAL_CANDIDATES", "MATRIXARK_MAX_CHILDREN_SCORED_PER_PARENT")


def _tree():
    with open(os.path.join(TOOLS, RETRIEVE), encoding="utf-8", errors="replace") as handle:
        return ast.parse(handle.read())


def _retrieve_function(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "retrieve":
            return node
    return None


def _native_block(fn):
    """The `if native_pack is not None:` block."""
    for node in ast.walk(fn):
        if not isinstance(node, ast.If):
            continue
        test = ast.unparse(node.test)
        if "native_pack is not None" in test:
            return node
    return None


def _first_cap_read_line(fn):
    """The earliest line where one of the caps is bound as a local."""
    lines = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in CAPS:
                    lines.append(node.lineno)
    return min(lines) if lines else None


class ACapSaysWhichPathItBinds(unittest.TestCase):

    def setUp(self):
        self.tree = _tree()
        self.fn = _retrieve_function(self.tree)
        self.assertIsNotNone(self.fn, "%s no longer defines retrieve" % RETRIEVE)

    def test_the_structure_is_there_to_check(self) -> None:
        """A floor. Every assertion below is vacuous if either half went missing."""
        self.assertIsNotNone(_native_block(self.fn),
                             "retrieve no longer has an `if native_pack is not None` block")
        self.assertIsNotNone(_first_cap_read_line(self.fn),
                             "retrieve no longer binds any of %s as a local" % (CAPS,))

    def test_the_native_block_returns_before_the_caps_are_read(self) -> None:
        """The reason the caps do not bind on that path."""
        block = _native_block(self.fn)
        first_cap = _first_cap_read_line(self.fn)
        block_end = getattr(block, "end_lineno", block.lineno)

        self.assertLess(
            block_end, first_cap,
            "the native block now extends past the first cap read (%d), so the caps may bind on "
            "that path after all -- revisit the help text on all four" % first_cap)
        self.assertIsInstance(
            block.body[-1], ast.Return,
            "the native block no longer ends in a return, so a request it handles can fall through "
            "to the cap reads. That would change which deployments the caps bind on.")

    def test_the_native_request_carries_none_of_the_caps(self) -> None:
        """The other half: not read after, and not sent before."""
        for node in ast.walk(self.fn):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = getattr(func, "attr", getattr(func, "id", ""))
            if name != "native_context_pack":
                continue
            # The keys are read out of the dict, not matched in the unparsed text. The first
            # version of this searched `ast.unparse(node)` for '"cap"' with DOUBLE quotes, and
            # ast.unparse emits SINGLE quotes -- so it could never fire. A mutation that added a
            # cap to the request passed it.
            keys = set()
            for argument in list(node.args) + [kw.value for kw in node.keywords]:
                for sub in ast.walk(argument):
                    if isinstance(sub, ast.Dict):
                        keys.update(k.value for k in sub.keys
                                    if isinstance(k, ast.Constant) and isinstance(k.value, str))
            self.assertTrue(
                keys, "the native request carries no literal keys at all, so this comparison is "
                      "reading the wrong call")
            for cap in CAPS:
                with self.subTest(cap=cap):
                    self.assertNotIn(
                        cap, keys,
                        "the native request now carries %s. If the engine honours it, the help "
                        "saying it binds only on the Python path is wrong." % cap)
            return
        self.fail("retrieve no longer calls native_context_pack")

    def test_every_cap_tells_an_operator_which_path_it_binds(self) -> None:
        """The operator-facing half. A ceiling that does not bind is worse than no ceiling."""
        import matrixark_gateway_config as config_module
        by_env = {s.env: s for s in config_module.SETTINGS if s.env}
        for env in CAP_ENVS:
            with self.subTest(env=env):
                setting = by_env.get(env)
                self.assertIsNotNone(setting, "%s is no longer offered" % env)
                self.assertIn(
                    "PYTHON retrieval path", setting.help,
                    "%s does not say which path it binds, so an operator reading it on a "
                    "temporalstore deployment is told it is a ceiling when it is not" % env)


if __name__ == "__main__":
    unittest.main()
