#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run the python test suite and fail only if it got WORSE than a recorded baseline.

The suite is not currently run by CI, which is how nine test files that could not even import
sat here unnoticed -- every test in them errored on import and nothing reported it.

Turning the suite into a blocking gate today would mean fixing 135 pre-existing failures first.
This is the middle path: record what fails now, and fail the build only when a change ADDS a
failure. Existing failures stay visible without demanding a cleanup before anything else can
land, and the baseline shrinks as they are fixed.

The baseline is recorded for the CI RUNNER, not for a developer machine. Which tests fail
depends on what is installed -- a machine with an old node fails the browser-facing tests that
CI passes, and one with torch installed passes model tests that CI fails. So a local run will
usually show a handful of differences in both directions; that is expected, and only CI's result
gates a merge.

Usage:
    python3 tools/run_python_suite_ratchet.py            # check against the baseline
    python3 tools/run_python_suite_ratchet.py --record   # rewrite the baseline
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

BASELINE = Path(__file__).with_name("python_suite_baseline.json")
NAME = re.compile(r"^(?:FAIL|ERROR): (\S+)")
# Verbose output names each test before its outcome. With a docstring the outcome lands on the
# NEXT line ("name (Class)" / "the docstring ... skipped"), so the name is remembered rather than
# read off the outcome line.
STARTED = re.compile(r"^(\S+) \(")


def suite_result() -> tuple[set[str], set[str]]:
    """(failing names, skipped names) from one full run.

    Skipped names are collected because a test that stops RUNNING also stops being reported as
    failing. Without them `baseline - failing` cannot tell "someone fixed it" from "it no longer
    runs here", and the difference matters: banking the second kind removes a test from the gate
    permanently while it sits unexercised. Eleven names read as fixed on 2026-09-03 and every one
    was a skip -- five on a module absent from the repo, six on a native proxy binary CI does not
    build.
    """
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-v", "-s", ".", "-p", "test_*.py"],
        cwd=str(Path(__file__).parent),
        capture_output=True,
        text=True,
    )
    failing: set[str] = set()
    skipped: set[str] = set()
    started = ""
    for line in (result.stderr + result.stdout).splitlines():
        stripped = line.strip()
        found = NAME.match(stripped)
        if found:
            failing.add(found.group(1))
            continue
        begun = STARTED.match(stripped)
        if begun:
            started = begun.group(1)
        if " ... skipped" in stripped and started:
            skipped.add(started)
    return failing, skipped


def failing_names() -> set[str]:
    return suite_result()[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--record", action="store_true", help="rewrite the baseline")
    args = parser.parse_args()

    current, skipped = suite_result()

    if args.record:
        BASELINE.write_text(
            json.dumps({"failing": sorted(current)}, indent=2) + "\n", encoding="utf-8"
        )
        print("recorded %d failing tests" % len(current))
        return 0

    try:
        known = set(json.loads(BASELINE.read_text(encoding="utf-8"))["failing"])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        print("no usable baseline at %s -- run with --record" % BASELINE.name)
        return 1

    added = sorted(current - known)
    if added:
        # Confirm on a second full run before failing. Several tests here fail in one run and
        # pass in the next -- order-dependent, or racing background work -- and a ratchet that
        # trips on those is a ratchet nobody keeps green.
        #
        # A second FULL run rather than re-running the names alone: filtering with -k still
        # imports every module, so unrelated import errors appear in the output and there is no
        # reliable way to tell "this test failed" from "the suite could not load". Intersecting
        # two full runs needs no such judgement, and costs a second pass only when something new
        # actually showed up.
        print("\n%d new failure(s); confirming on a second run..." % len(added))
        added = sorted(set(added) & failing_names())
    # A name can leave the failing set two ways, and only one of them is good news.
    gone = known - current
    stopped = sorted(gone & skipped)
    fixed = sorted(gone - skipped)

    print("failing now: %d   baseline: %d   skipped: %d" % (len(current), len(known), len(skipped)))
    if fixed:
        print("\nfixed since the baseline (%d):" % len(fixed))
        for name in fixed[:20]:
            print("   %s" % name)
        print("\n   ...rerun with --record to bank these.")
    if stopped:
        print("\nno longer FAILING because they no longer RUN (%d):" % len(stopped))
        for name in stopped[:20]:
            print("   %s" % name)
        print("\n   These are skipped, not fixed. Banking them drops them from the gate while")
        print("   they sit unexercised -- make them run, or leave them in the baseline.")
    if added:
        print("\nNEW failures (%d):" % len(added))
        for name in added:
            print("   %s" % name)
        # A test that is merely flaky will trip this. That is deliberate: a name that comes and
        # goes is a defect in the test, and silently tolerating it is how the suite got here.
        return 1
    print("\nno new failures.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
