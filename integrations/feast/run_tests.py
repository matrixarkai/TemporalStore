#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run every offline test in this directory, with no third-party test runner.

WHY THIS FILE EXISTS. The two providers here were written in different styles:
``feast_temporalstore_command`` uses ``unittest`` and carries its own runner,
while ``feast_temporalstore``'s ``tests/test_wire.py`` is a set of bare
``test_*`` functions in the pytest style and has no runner at all. A bare
function in a module nothing imports is not a test -- it is a source file that
is as green as a correct one -- and that is exactly what the wire tests were:
no CI job ran them, and no job could, because the job has no pytest.

So this runner is deliberately standard-library only. Adding pytest to a
gating job to execute nine assertions would make the job depend on a package
download, and the whole point of the wire tier is that it needs nothing.

WHAT IT ASSERTS, AND WHY A COUNT RATHER THAN A RESULT. Each tier declares a
floor on the number of tests that actually EXECUTED. A runner that reports
success over a suite it failed to collect is the failure this guards against:
``unittest`` collects a class decorated with ``skipUnless`` whether or not the
condition holds, so a count of collected tests cannot tell a run with Feast
from a run without it, and neither can tell either from a run where a whole
tier silently stopped being discovered. The floors are on executed counts, and
they differ by whether Feast is importable.
"""
from __future__ import annotations

import importlib.util
import inspect
import os
import sys
import traceback
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
# Both packages are imported by name, so this directory has to be importable.
if HERE not in sys.path:
    sys.path.insert(0, HERE)

try:
    import feast  # noqa: F401

    FEAST_AVAILABLE = True
except Exception:
    FEAST_AVAILABLE = False

# The wire tier is pure standard library and must execute in every environment,
# so its floor does not move with Feast. The command tier's online-store tests
# need Feast and are skipped without it, which is why it has two floors.
# These are the counts READ FROM a run, not guessed: a first draft of this file
# put the wire floor at 9 against a real 7, and the floor refused the run --
# which is the behaviour wanted, on the wrong value.
WIRE_FLOOR = 7
COMMAND_FLOOR_WITH_FEAST = 29
COMMAND_FLOOR_WITHOUT_FEAST = 12

# The conformance tier needs a running data node, so a skip is legitimate where
# there is none -- this runner is standard-library-only on purpose and must not
# come to require a server. But a skip that leaves the final line reading
# "PASS: 7 wire + 28 command" is INVISIBLE, and an invisible skip is how a tier
# comes to have never executed while every signal stays green. Measured, not
# supposed: with the node unreachable the tier reported `executed 0, skipped 1`
# and this script still exited 0 with a PASS line that never mentioned it.
#
# Two things change. The conformance count is now always in the PASS line, and
# FEAST_CONFORMANCE_REQUIRED=1 turns a skip into a failure for the one caller
# that knows a node should be reachable. The floor is a count a real run
# produced, not a guess.
CONFORMANCE_FLOOR_WHEN_REQUIRED = 5


def _conformance_required() -> bool:
    """Whether a skipped conformance tier is a failure.

    A function so the truthiness rule lives in one place: 1/true/yes/on,
    case-insensitively, and nothing else. An UNSET variable must not read as
    required, or every environment without a node starts failing.
    """
    raw = os.environ.get("FEAST_CONFORMANCE_REQUIRED", "")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _load_module(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def run_function_style(path: str, name: str) -> tuple[int, int]:
    """Execute every zero-argument ``test_*`` function in a module.

    Returns ``(executed, failed)``. A function that takes arguments would be a
    pytest fixture request, which this runner cannot satisfy -- it is reported
    as a failure rather than quietly skipped, because a test this runner cannot
    execute is a test that is not running.
    """
    module = _load_module(path, name)
    executed = 0
    failed = 0
    for attr in sorted(dir(module)):
        if not attr.startswith("test_"):
            continue
        function = getattr(module, attr)
        if not callable(function):
            continue
        parameters = inspect.signature(function).parameters
        if parameters:
            print(f"FAIL {name}.{attr}: takes arguments, cannot be run without fixtures")
            failed += 1
            continue
        try:
            function()
        except Exception:
            print(f"FAIL {name}.{attr}")
            traceback.print_exc()
            failed += 1
        else:
            print(f"ok   {name}.{attr}")
        executed += 1
    return executed, failed


def run_unittest_style(path: str, name: str) -> tuple[int, int]:
    """Execute a ``unittest`` module and return ``(executed, failed)``."""
    module = _load_module(path, name)
    suite = unittest.TestLoader().loadTestsFromModule(module)
    collected = suite.countTestCases()
    if collected == 0:
        print(f"FAIL {name}: collected 0 tests")
        return 0, 1
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    # CLAMPED AT ZERO, because this subtraction goes NEGATIVE on a class-level
    # skip: when `setUpClass` raises `SkipTest`, `testsRun` is 0 while
    # `skipped` carries one entry for the whole class, giving -1. Measured
    # here, not reasoned about -- this printed "executed -1" and then
    # "-1 test(s) executed against a live node", and every `executed == 0`
    # guard downstream silently did not fire. The same arithmetic was wrong in
    # two places; it is fixed here, where the count is produced, rather than at
    # each caller.
    executed = max(0, result.testsRun - len(result.skipped))
    failed = len(result.failures) + len(result.errors)
    print(
        f"{name}: collected {collected}, executed {executed}, "
        f"skipped {len(result.skipped)}"
    )
    return executed, failed


def main() -> int:
    print(f"feast_available={FEAST_AVAILABLE}")
    problems: list[str] = []

    print("\n--- feast_temporalstore wire tier (standard library only) ---")
    wire_executed, wire_failed = run_function_style(
        os.path.join(HERE, "tests", "test_wire.py"), "test_wire"
    )
    print(f"test_wire: executed {wire_executed}, failed {wire_failed}, floor {WIRE_FLOOR}")
    if wire_failed:
        problems.append(f"{wire_failed} wire test(s) failed")
    if wire_executed < WIRE_FLOOR:
        problems.append(
            f"wire tier executed {wire_executed} tests, expected at least {WIRE_FLOOR}"
        )

    print("\n--- feast_temporalstore_command tier ---")
    command_floor = (
        COMMAND_FLOOR_WITH_FEAST if FEAST_AVAILABLE else COMMAND_FLOOR_WITHOUT_FEAST
    )
    command_executed, command_failed = run_unittest_style(
        os.path.join(HERE, "feast_temporalstore_command", "tests", "test_online_store.py"),
        "test_online_store",
    )
    print(
        f"test_online_store: executed {command_executed}, "
        f"failed {command_failed}, floor {command_floor}"
    )
    if command_failed:
        problems.append(f"{command_failed} command-surface test(s) failed")
    if command_executed < command_floor:
        problems.append(
            f"command tier executed {command_executed} tests, "
            f"expected at least {command_floor}"
        )

    # -- the conformance tier, which is ALLOWED to skip -----------------------
    # It drives the provider through Feast's public API against a live data
    # node. Included here rather than left as a separate command because a file
    # no runner ever touches is as green as a correct one -- this at least
    # imports and collects it on every run, and executes it whenever a node is
    # reachable. It has no floor: "no node" is a legitimate skip, and turning
    # that into a failure would make this script need a server. What it will
    # not do is report a skipped run as a pass -- the count is printed either
    # way.
    print("\n--- feast_temporalstore_command conformance tier (needs a node) ---")
    # Bound BEFORE the branch: previously assigned only on the path that ran, so
    # the final line could not report it and a skip left no trace at all.
    conf_executed = 0
    conformance_required = _conformance_required()
    conformance_path = os.path.join(
        HERE, "feast_temporalstore_command", "tests", "test_feast_api_conformance.py"
    )
    if not FEAST_AVAILABLE:
        print("conformance: SKIPPED, feast is not installed")
        if conformance_required:
            problems.append(
                "the conformance tier was skipped because feast is not installed, "
                "while FEAST_CONFORMANCE_REQUIRED is set"
            )
    elif not os.path.isfile(conformance_path):
        problems.append("the conformance test file is missing")
    else:
        conf_executed, conf_failed = run_unittest_style(
            conformance_path, "test_feast_api_conformance"
        )
        if conf_failed:
            problems.append(f"{conf_failed} conformance test(s) failed")
        if conf_executed == 0:
            print(
                "conformance: SKIPPED ENTIRELY -- no data node was reachable, so "
                "NOTHING was verified through Feast's own API. Not a failure "
                "here, and not a pass either."
            )
            if conformance_required:
                problems.append(
                    "the conformance tier executed 0 tests while "
                    "FEAST_CONFORMANCE_REQUIRED is set -- a node was expected to be "
                    "reachable and was not, so NOTHING was verified through Feast's "
                    "own API"
                )
        else:
            print(f"conformance: {conf_executed} test(s) executed against a live node")
            if conformance_required and conf_executed < CONFORMANCE_FLOOR_WHEN_REQUIRED:
                problems.append(
                    f"the conformance tier executed {conf_executed} tests, expected "
                    f"at least {CONFORMANCE_FLOOR_WHEN_REQUIRED}"
                )

    print()
    if problems:
        for problem in problems:
            print(f"FAIL: {problem}", file=sys.stderr)
        return 1
    # THE CONFORMANCE COUNT IS IN THE SUCCESS LINE, ALWAYS. Its absence is what
    # let a run that verified nothing through Feast's API read as a complete pass.
    conformance_note = (
        f"{conf_executed} conformance"
        if conf_executed
        else "0 conformance (SKIPPED -- nothing verified through Feast's own API)"
    )
    print(
        f"PASS: {wire_executed} wire + {command_executed} command + "
        f"{conformance_note} tests executed "
        f"(feast_available={FEAST_AVAILABLE}, "
        f"conformance_required={conformance_required})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
