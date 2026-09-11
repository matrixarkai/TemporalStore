#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""`A or B` and `B or A` are the same code until both are set, and then they are two answers.

When two environment variables appear in one fallback chain, the ORDER is the precedence rule
between them. It has to be the same at every site, or one deployment resolves one way in one module
and the other way in another -- and only when BOTH are set, which is exactly the configuration
nobody tests.

The instance this was written for:

    matrixark_mcp_embeddings (x3), matrixark_mcp_core, context_minilm_embed_server
        MATRIXARK_EMBEDDING_MODEL_PATH or MATRIXARK_EMBEDDING_MODEL     <- loads the encoder
    matrixark_resource_parser.encoder_window_tokens
        MATRIXARK_EMBEDDING_MODEL or MATRIXARK_EMBEDDING_MODEL_PATH     <- sizes its input

Five to one, and the one was the outlier. `encoder_window_tokens` matches the model NAME against a
table to find the encoder's token window, and that window is the ceiling on how much text is fed to
the encoder. With both variables set to models of different window sizes, the ceiling described the
model that does not load: a 512 read off the name in MODEL, applied to the 384-token model actually
loaded from MODEL_PATH, silently truncating the tail of every chunk. That is the same failure
test_matrixark_windows_follow_the_encoder was written about, arriving through the flags instead of
through a hard-coded constant.

The check is general rather than a list of pairs: every `or` chain in the tree that reads two or
more environment variables contributes its order, and a set of names appearing in more than one
order fails. Pairs that legitimately differ can be added to ORDER_MAY_DIFFER with a reason -- there
are none today, and an empty allow-list that has to be edited is the point.
"""
from __future__ import annotations

import ast
import collections
import glob
import io
import os
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

GETTERS = {"os.environ.get", "os.getenv", "environ.get"}

#: Sets of variable names allowed to appear in more than one order, each with a reason.
#: Empty today. Anything added here is a claim that the precedence genuinely differs by caller.
ORDER_MAY_DIFFER: dict[frozenset, str] = {}

#: Floors. A check that stops FINDING multi-variable chains passes vacuously.
CHAINS_FLOOR = 25
SETS_FLOOR = 15


def live_modules() -> list:
    return sorted(path for path in glob.glob(os.path.join(TOOLS, "*.py"))
                  if not os.path.basename(path).startswith("test_"))


def env_name(node):
    """The variable this expression reads, seen through .strip()/.lower()/str() wrappers."""
    current = node
    for _ in range(5):
        if isinstance(current, ast.Call):
            callee = ast.unparse(current.func)
            if (callee in GETTERS and current.args
                    and isinstance(current.args[0], ast.Constant)
                    and isinstance(current.args[0].value, str)):
                return current.args[0].value
            if isinstance(current.func, ast.Attribute):
                current = current.func.value
                continue
            if callee in ("str", "int", "float", "bool") and current.args:
                current = current.args[0]
                continue
            return None
        if isinstance(current, ast.Attribute):
            current = current.value
            continue
        if (isinstance(current, ast.Subscript)
                and ast.unparse(current.value) in ("os.environ", "environ")
                and isinstance(current.slice, ast.Constant)
                and isinstance(current.slice.value, str)):
            return current.slice.value
        return None
    return None


def chain_orders(sources: dict) -> dict:
    """frozenset(names) -> {(ordered names): [(module, line), ...]}"""
    found: dict = collections.defaultdict(lambda: collections.defaultdict(list))
    for path, text in sources.items():
        stem = os.path.basename(path)[:-3]
        try:
            tree = ast.parse(text)
        except SyntaxError:  # pragma: no cover - the tree parses
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or)):
                continue
            names = [name for name in (env_name(v) for v in node.values) if name]
            if len(names) < 2 or len(set(names)) != len(names):
                continue
            found[frozenset(names)][tuple(names)].append((stem, node.lineno))
    return found


def _sources() -> dict:
    out = {}
    for path in live_modules():
        with io.open(path, encoding="utf-8", errors="replace") as handle:
            out[path] = handle.read()
    return out


def disagreements(sources: dict) -> list:
    out = []
    for names, by_order in chain_orders(sources).items():
        if len(by_order) > 1 and names not in ORDER_MAY_DIFFER:
            out.append((sorted(names), {order: sites for order, sites in by_order.items()}))
    return out


class OneChainOneOrderTest(unittest.TestCase):

    def test_no_variable_pair_is_read_in_two_orders(self) -> None:
        found = disagreements(_sources())
        if not found:
            return
        lines = []
        for names, by_order in found:
            lines.append("{%s}" % ", ".join(names))
            for order, sites in sorted(by_order.items()):
                where = ", ".join("%s:%d" % site for site in sites)
                lines.append("    %s   at %s" % (" -> ".join(order), where))
        self.fail("these variables are read in more than one precedence order, so a deployment "
                  "that sets both resolves differently per module:\n" + "\n".join(lines))

    def test_the_check_still_finds_chains_to_compare(self) -> None:
        """The floor. Nothing above can fail once this stops finding chains."""
        orders = chain_orders(_sources())
        chains = sum(len(sites) for by_order in orders.values()
                     for sites in by_order.values())
        self.assertGreaterEqual(
            chains, CHAINS_FLOOR,
            "found %d multi-variable chains, expected at least %d -- this check has gone blind"
            % (chains, CHAINS_FLOOR))
        self.assertGreaterEqual(
            len(orders), SETS_FLOOR,
            "found %d distinct variable sets, expected at least %d" % (len(orders), SETS_FLOOR))

    def test_the_encoder_window_follows_the_variable_that_loads_the_encoder(self) -> None:
        """The instance, named. The window is a ceiling on what is fed to the LOADED encoder."""
        orders = chain_orders(_sources())
        pair = frozenset({"MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_MODEL_PATH"})
        self.assertIn(pair, orders, "this pair is gone; re-aim this check")
        for order in orders[pair]:
            self.assertEqual(
                "MATRIXARK_EMBEDDING_MODEL_PATH", order[0],
                "MODEL_PATH is what the five encoder-loading sites resolve first, so the window "
                "must read it first too, or it describes a model that does not load")


class AReversedPairIsCaughtTest(unittest.TestCase):
    """The floor that matters: reverse a real chain in a copy and require the check to say so."""

    def test_reversing_one_site_is_caught(self) -> None:
        sources = _sources()
        target = os.path.join(TOOLS, "matrixark_resource_parser.py")
        self.assertIn(target, sources, "the module moved; re-aim this check")

        fixed = ('os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH")\n'
                 '            or os.environ.get("MATRIXARK_EMBEDDING_MODEL")')
        reversed_ = ('os.environ.get("MATRIXARK_EMBEDDING_MODEL")\n'
                     '            or os.environ.get("MATRIXARK_EMBEDDING_MODEL_PATH")')
        self.assertIn(fixed, sources[target],
                      "the fixed order is gone; this control is not testing the fix")

        sources[target] = sources[target].replace(fixed, reversed_)
        found = disagreements(sources)
        pairs = [names for names, _by_order in found]
        self.assertIn(
            ["MATRIXARK_EMBEDDING_MODEL", "MATRIXARK_EMBEDDING_MODEL_PATH"], pairs,
            "the check did not catch a reversed chain, so its verdict on the real tree is not "
            "evidence")

    def test_an_agreeing_tree_is_not_flagged(self) -> None:
        """The other error: this must not fire on chains that already agree."""
        self.assertEqual([], disagreements(_sources()))


if __name__ == "__main__":
    unittest.main()
