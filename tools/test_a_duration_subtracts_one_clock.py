#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A duration subtracts two readings of the SAME clock.

`time.time()` is wall-clock seconds since the epoch. `time.perf_counter()` and `time.monotonic()`
count from an arbitrary origin that means nothing outside the process. Subtracting one family
from the other is not a duration:

    t0 = time.perf_counter()        # ~1.2e4 on a box up for a few hours
    ...
    elapsed = time.time() - t0      # ~1.7e9  -- fifty-four years

This tree uses both deliberately and correctly: `perf_counter` for how long something took,
`time.time()` for when it happened and for anything stored or compared across processes. 112
subtractions in production resolve to a clock on both sides, and every one of them matches.

WHY IT IS WORTH A CHECK WHEN THE ANSWER IS ZERO. The mistake is easy to write -- the two locals
are often assigned pages apart and both are called `t0`, `start` or `began` -- and the result is
not a crash. It is a number that flows into a latency metric, a budget check or a stored record.
A retention average once sat at 1.4 in this tree for exactly that kind of reason: an arithmetic
result nobody re-read because nothing raised.

The scan follows one local-variable hop inside a function, because the realistic shape assigns
each reading to a name first. A name reassigned from anything else is dropped rather than
guessed at, so a variable that sometimes holds a clock and sometimes a caller's argument is not
reported.
"""
from __future__ import annotations

import ast
import io
import os
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

WALL_CLOCKS = frozenset({"time.time", "time.time_ns"})
MONOTONIC_CLOCKS = frozenset({"time.perf_counter", "time.monotonic",
                              "time.perf_counter_ns", "time.monotonic_ns"})

#: A floor on the SCAN. 112 subtractions in production resolve to a clock on both sides today;
#: well under that catches a scan that stopped recognising the calls, which would make the
#: finding below zero for the wrong reason.
MINIMUM_RESOLVED_SUBTRACTIONS = 60


def _clock_family(node) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    try:
        name = ast.unparse(node.func)
    except Exception:                       # pragma: no cover - unparse is total here
        return None
    if name in WALL_CLOCKS:
        return "wall"
    if name in MONOTONIC_CLOCKS:
        return "monotonic"
    return None


def _scope_clock_names(scope) -> dict:
    """Clock origins for names assigned DIRECTLY in this scope, not in nested functions.

    Walking a whole module and every function separately double-counts anything inside a
    function, and lets one function's `t0` decide another's. Each scope is read once, on its own.
    """
    origin, rebound = {}, set()

    def visit(node) -> None:
        # IN SOURCE ORDER. A stack processes the body backwards, and then
        # `started = time.perf_counter()` followed by `started = supplied` reads as a clock that
        # was never overwritten -- the rebound case silently stops being detected.
        if isinstance(node, ast.Assign) and len(node.targets) == 1            and isinstance(node.targets[0], ast.Name):
            target = node.targets[0].id
            family = _clock_family(node.value)
            if family:
                if target in origin and origin[target] != family:
                    rebound.add(target)
                origin[target] = family
            elif target in origin:
                # Reassigned from something that is not a clock; the name no longer says which
                # one it holds, so it is not evidence either way.
                rebound.add(target)
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue                    # its own scope; resolved when we get there
            visit(child)

    for statement in scope.body:
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        visit(statement)
    for target in rebound:
        origin.pop(target, None)
    return origin


def mixed_clock_subtractions(root: str, files):
    """(file, line, source) for each `a - b` whose sides read different clock families."""
    hits, resolved = [], 0
    for rel in files:
        try:
            with io.open(os.path.join(root, rel), encoding="utf-8") as handle:
                tree = ast.parse(handle.read())
        except (SyntaxError, OSError):
            continue

        # innermost enclosing scope for every node, so each subtraction is judged exactly once
        scope_of, scopes = {}, []

        def descend(node, current):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    scopes.append(child)
                    scope_of[id(child)] = current
                    descend(child, child)
                else:
                    scope_of[id(child)] = current
                    descend(child, current)

        scopes.append(tree)
        descend(tree, tree)
        names = {id(s): _scope_clock_names(s) for s in scopes}

        for node in ast.walk(tree):
            if not (isinstance(node, ast.BinOp) and isinstance(node.op, ast.Sub)):
                continue
            here = scope_of.get(id(node), tree)
            local = names.get(id(here), {})
            module = names.get(id(tree), {})

            def family_of(side):
                direct = _clock_family(side)
                if direct:
                    return direct
                if isinstance(side, ast.Name):
                    return local.get(side.id) or module.get(side.id)
                return None

            left, right = family_of(node.left), family_of(node.right)
            if not (left and right):
                continue
            resolved += 1
            if left != right:
                hits.append((rel, node.lineno, ast.unparse(node)[:90]))
    return resolved, hits


def _production_files(root: str):
    return sorted(f for f in os.listdir(root)
                  if f.endswith(".py") and not f.startswith("test_"))


class ADurationSubtractsOneClockTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.resolved, cls.hits = mixed_clock_subtractions(TOOLS, _production_files(TOOLS))

    def test_the_scan_still_recognises_the_clocks(self) -> None:
        """Vacuity floor on the scan: no resolved subtractions means no mixed ones either."""
        self.assertGreater(
            self.resolved, MINIMUM_RESOLVED_SUBTRACTIONS,
            "only %d subtractions resolved to a clock on both sides, below the floor of %d; the "
            "scan has stopped matching the calls"
            % (self.resolved, MINIMUM_RESOLVED_SUBTRACTIONS))

    def test_no_duration_mixes_two_clocks(self) -> None:
        self.assertEqual(
            [], self.hits,
            "this subtracts a monotonic reading from a wall-clock one, or the reverse. The "
            "result is not a duration -- it is the gap between the epoch and the process's "
            "arbitrary origin, and nothing raises. Read the same clock on both sides:\n  "
            + "\n  ".join("%s:%d  %s" % row for row in self.hits))

    def test_the_scan_catches_the_two_locals_shape(self) -> None:
        """The control, through the same function, on the shape the mistake actually takes.

        Both readings assigned to names first, often pages apart. A detector that only saw the
        inline form would report zero on a tree that had this.
        """
        with tempfile.TemporaryDirectory() as root:
            def write(name: str, body: str) -> None:
                with io.open(os.path.join(root, name), "w", encoding="utf-8") as handle:
                    handle.write(body)

            write("defect_two_locals.py",
                  "import time\n\n\ndef f():\n    started = time.time()\n"
                  "    ended = time.monotonic()\n    return ended - started\n")
            write("fine_same_clock.py",
                  "import time\n\n\ndef f():\n    started = time.perf_counter()\n"
                  "    return time.perf_counter() - started\n")
            write("fine_rebound.py",
                  "import time\n\n\ndef f(supplied):\n    started = time.perf_counter()\n"
                  "    started = supplied\n    return time.time() - started\n")

            resolved, hits = mixed_clock_subtractions(root, _production_files(root))
            self.assertEqual(1, len(hits), hits)
            self.assertIn("defect_two_locals.py", hits[0][0])
            self.assertGreater(resolved, 1, "the fixture's own denominator collapsed")


if __name__ == "__main__":
    unittest.main()
