#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A module whose every test is `#[ignore]`d contributes no running tests, and `RAN=1` says it did.

`crates/temporalstore-rust/src/engine/tests/upsert_deltas.rs` was 67 lines holding exactly one
test, and that test was `#[ignore]`d and read a store prefix out of `PROBE_DIR`. It could not run
in any environment this repository provides. `upsert_deltas` was nonetheless named as recovery
coverage in verdict after verdict, because the signal being read was whether the module's name
appeared in `cargo test` output. It appears either way: a module full of ignored tests is listed,
counted, and reported, and the line that says so ends in `ignored` rather than `ok`. A count
derived from "the name appeared" cannot tell the two apart, which is the whole defect -- a check
that cannot fail looks exactly like one that passed.

So this guard counts what cargo would actually RUN, from the attribute pair, and holds two things:

  * the SCANNER separates ran from ignored, rather than collapsing them into a presence count, and
  * no module contributes zero running tests from behind a BARE `#[ignore]`.

WHY THE RULE IS "BARE" AND NOT "ALL-IGNORED". Six files in this tree consist solely of ignored
tests. Five are deliberate, and they say so on every one of their ignores:

    #[ignore = "seeds four stores up to 40,000 records each; run by name"]
    #[ignore = "requires TS_SNAPSHOT_AWS_BUCKET and AWS CLI credentials"]

Those are heavy measurement studies and an external smoke test -- real tests, run by name, whose
module headers describe a cost or a refutation rather than coverage. A rule forbidding all-ignored
modules outright would fail all six on the day it landed, five of them correctly-written, and the
only way to land it would be a five-entry exemption list that nobody would revisit. An exemption
list is a hiding place. A rule keyed instead to a hand-written roster of "the recovery suites"
goes stale the moment a suite is renamed, and nothing fails when it does.

The discriminator the evidence actually produced needs neither. Of the six, exactly one carried a
BARE `#[ignore]`: the one that got mis-reported as coverage. A reason string is the difference
between a module that declares it contributes nothing by default and one that is silently empty,
it is derived from the attribute rather than from a list of this project's names, and on the tree
as it stands the rule has no exemptions at all.

THE MATCHER IS `CONTAINS`, NOT `IS`. An earlier pass of this scan matched the literal `#[test]`
and reported five all-ignored files. A crude `grep -c` over the same tree counted one more ignore
line than the scan had accounted for, and that one line was a `#[tokio::test]` in
`temporalstore-snapshot` -- a sixth all-ignored file the scan could not see. Every `::test`
spelling is matched here, and `test_the_matcher_sees_a_namespaced_test_attribute` is that hole
written down so it cannot reopen.
"""
from __future__ import annotations

import io
import os
import re
import shutil
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)
CRATES = os.path.join(REPO, "crates")

#: `#[test]`, `#[tokio::test]`, `#[tokio::test(flavor = "multi_thread")]`, `#[async_std::test]`.
#: CONTAINS, not IS: matching the bare literal missed an all-ignored file in this very tree.
_TEST_ATTR = re.compile(r"^#\[(?:[a-z_][a-z0-9_]*::)*test\b")
#: `#[ignore]` is bare; `#[ignore = "why"]` declares itself. The `=` is the whole distinction.
_IGNORE_ATTR = re.compile(r"^#\[ignore\s*(=)?")
_FN = re.compile(r"^\s*(?:pub(?:\([a-z]+\))?\s+)?(?:async\s+)?fn\s+([a-z_][A-Za-z0-9_]*)")

#: Floors. Every assertion below passes on an empty scan, so the size of the scan is asserted
#: too. Measured on the tree this landed against: 200 files, 3,096 tests, 290 ignored.
FILES_WITH_TESTS_FLOOR = 180
TEST_FUNCTION_FLOOR = 2_900
IGNORED_FLOOR = 250
#: The five deliberate all-ignored modules. If this class empties, the rule below is deciding
#: nothing and a real offender would pass just as quietly as these five do.
ALL_IGNORED_FLOOR = 5


def _rust_sources(root):
    for base, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d != "target"]
        for name in sorted(files):
            if name.endswith(".rs"):
                yield os.path.join(base, name)


def _read(path):
    with io.open(path, encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _tests_in(text):
    """[(name, ignored, reasoned)] for every test function in `text`.

    `ignored` comes from the attribute block, never from a name appearing somewhere. An
    `#[ignore]` may sit above or below the test attribute, and both are read.
    """
    lines = text.split("\n")
    found = []
    for index, line in enumerate(lines):
        if not _TEST_ATTR.match(line.strip()):
            continue
        ignored = reasoned = False
        name = None
        probe = index + 1
        while probe < len(lines):
            stripped = lines[probe].strip()
            if stripped.startswith("#["):
                match = _IGNORE_ATTR.match(stripped)
                if match:
                    ignored, reasoned = True, bool(match.group(1))
                probe += 1
                continue
            if stripped == "" or stripped.startswith("//"):
                probe += 1
                continue
            named = _FN.match(lines[probe])
            if named:
                name = named.group(1)
            break
        probe = index - 1
        while probe >= 0:
            stripped = lines[probe].strip()
            if not stripped.startswith("#["):
                break
            match = _IGNORE_ATTR.match(stripped)
            if match:
                ignored, reasoned = True, bool(match.group(1))
            probe -= 1
        if name:
            found.append((name, ignored, reasoned))
    return found


def scan(root):
    """{relative path: {"ran": [...], "ignored": [...], "bare": [...]}} for files holding tests.

    `ran` is what `cargo test` would execute. It is a separate list from `ignored` on purpose:
    one number covering both is the signal that reported an un-runnable module as covered.
    """
    report = {}
    for path in _rust_sources(root):
        text = _read(path)
        tests = _tests_in(text)
        if not tests:
            continue
        report[os.path.relpath(path, root).replace(os.sep, "/")] = {
            "ran": [n for n, ignored, _ in tests if not ignored],
            "ignored": [n for n, ignored, _ in tests if ignored],
            "bare": [n for n, ignored, reasoned in tests if ignored and not reasoned],
        }
    return report


def offenders(report):
    """Files contributing zero running tests with at least one undeclared `#[ignore]`."""
    return sorted(
        "%s (%d ignored, bare: %s)" % (path, len(entry["ignored"]), ", ".join(entry["bare"]))
        for path, entry in report.items()
        if not entry["ran"] and entry["bare"]
    )


def all_ignored(report):
    return sorted(path for path, entry in report.items() if not entry["ran"])


class _Fixture:
    """A throwaway tree the scanner is pointed at, so no control ever plants a live failure.

    A worked example inside a test file is not inert -- a guard in this repository once named a
    real flag in a docstring and reclassified it. Every name written below is invented, and the
    files live in a temporary directory that the scan over `crates/` never walks.
    """

    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="ignored-suite-control-")

    def write(self, name, body):
        path = os.path.join(self.root, name)
        with io.open(path, "w", encoding="utf-8") as handle:
            handle.write(body)
        return path

    def close(self):
        shutil.rmtree(self.root, ignore_errors=True)


class ASuiteOfOneIgnoredTestTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.report = scan(CRATES)
        cls.files = len(cls.report)
        cls.tests = sum(len(e["ran"]) + len(e["ignored"]) for e in cls.report.values())
        cls.ignored = sum(len(e["ignored"]) for e in cls.report.values())

    # ---- the scan is the right size ------------------------------------------------------

    def test_the_scan_still_sees_the_crate_tree(self):
        """Three floors. An empty scan satisfies every rule below it."""
        self.assertGreaterEqual(
            self.files, FILES_WITH_TESTS_FLOOR,
            "%d files hold tests, expected at least %d -- the scan stopped matching"
            % (self.files, FILES_WITH_TESTS_FLOOR))
        self.assertGreaterEqual(
            self.tests, TEST_FUNCTION_FLOOR,
            "%d test functions, expected at least %d" % (self.tests, TEST_FUNCTION_FLOOR))
        self.assertGreaterEqual(
            self.ignored, IGNORED_FLOOR,
            "%d ignored tests, expected at least %d -- if the ignore attribute stopped being "
            "recognised the rule below would pass on every file"
            % (self.ignored, IGNORED_FLOOR))

    def test_the_all_ignored_class_is_still_visible(self):
        """The deliberate all-ignored modules are the denominator of the rule below. With this
        class empty the rule decides nothing, and it would report exactly the same green."""
        found = all_ignored(self.report)
        self.assertGreaterEqual(
            len(found), ALL_IGNORED_FLOOR,
            "found %d modules contributing zero running tests, expected at least %d -- the "
            "ran/ignored split has stopped working: %s"
            % (len(found), ALL_IGNORED_FLOOR, found))

    # ---- the rule -----------------------------------------------------------------------

    def test_no_module_contributes_zero_running_tests_behind_a_bare_ignore(self):
        self.assertEqual(
            [], offenders(self.report),
            "these modules run nothing and do not say why: every test in them is `#[ignore]`d "
            "and at least one ignore carries no reason. Either give the module a test that "
            "runs, or write the reason into the attribute -- `#[ignore = \"why, and how to run "
            "it\"]` -- so a verdict naming the module cannot read it as coverage: %s"
            % offenders(self.report))

    # ---- controls: each one is a way this guard could have been vacuous -----------------

    def setUp(self):
        self.fixture = _Fixture()
        self.addCleanup(self.fixture.close)

    def test_the_positive_control_fires(self):
        """A guard never shown to fail is indistinguishable from a comment."""
        self.fixture.write("only_a_bare_ignore.rs", "\n".join([
            "#[test]",
            "#[ignore]",
            "fn a_control_that_should_be_caught() {",
            "    assert!(true);",
            "}",
            "",
        ]))
        report = scan(self.fixture.root)
        self.assertEqual({"only_a_bare_ignore.rs"}, set(report))
        self.assertEqual([], report["only_a_bare_ignore.rs"]["ran"], "nothing here runs")
        self.assertEqual(1, len(offenders(report)), "the rule must catch a bare-ignore module")

    def test_a_reasoned_ignore_is_not_an_offender(self):
        """The negative control. If this fired, the rule would need the exemption list it was
        designed to avoid, and five correctly-written modules would be its first victims."""
        self.fixture.write("reasoned.rs", "\n".join([
            "#[test]",
            '#[ignore = "seeds a large store; run by name"]',
            "fn a_control_that_declares_itself() {}",
            "",
        ]))
        report = scan(self.fixture.root)
        self.assertEqual([], report["reasoned.rs"]["ran"])
        self.assertEqual([], offenders(report), "a declared ignore is not the defect")

    def test_one_running_neighbour_clears_the_module(self):
        """A module that runs something is covered ground, whatever else it also holds."""
        self.fixture.write("mixed.rs", "\n".join([
            "#[test]",
            "#[ignore]",
            "fn a_control_that_is_skipped() {}",
            "",
            "#[test]",
            "fn a_control_that_actually_runs() {}",
            "",
        ]))
        report = scan(self.fixture.root)
        self.assertEqual(["a_control_that_actually_runs"], report["mixed.rs"]["ran"])
        self.assertEqual([], offenders(report))

    def test_the_matcher_sees_a_namespaced_test_attribute(self):
        """The hole this scan actually had. Matching the literal `#[test]` missed an all-ignored
        file in `temporalstore-snapshot`, and only a crude count of ignore lines disagreeing with
        the scan's own total brought it out."""
        self.fixture.write("namespaced.rs", "\n".join([
            "#[tokio::test]",
            "#[ignore]",
            "async fn a_control_behind_a_namespaced_attribute() {}",
            "",
        ]))
        report = scan(self.fixture.root)
        self.assertEqual(
            1, len(offenders(report)),
            "a namespaced test attribute must be read as a test, or a module of them scans as "
            "holding no tests at all and passes every rule here")

    def test_an_ignore_above_the_test_attribute_is_still_an_ignore(self):
        """Attribute order is the author's choice and changes nothing about what runs."""
        self.fixture.write("above.rs", "\n".join([
            "#[ignore]",
            "#[test]",
            "fn a_control_with_its_attributes_swapped() {}",
            "",
        ]))
        report = scan(self.fixture.root)
        self.assertEqual([], report["above.rs"]["ran"], "this does not run either")
        self.assertEqual(1, len(offenders(report)))

    def test_a_helper_is_not_counted_as_a_test(self):
        """Counting plain functions would inflate the floors above into rubber stamps."""
        self.fixture.write("helpers.rs", "\n".join([
            "fn a_control_helper() {}",
            "",
            "#[test]",
            "fn a_control_test_beside_it() {}",
            "",
        ]))
        report = scan(self.fixture.root)
        self.assertEqual(["a_control_test_beside_it"], report["helpers.rs"]["ran"])
        self.assertEqual([], report["helpers.rs"]["ignored"])


if __name__ == "__main__":
    unittest.main()
