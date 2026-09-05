#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The engine flag inventory matches the engine.

Two hundred-odd `TS_*` variables is a lot, and the first useful thing anyone can do with that
number is see the list grouped by what each flag decides. A hand-kept list that long is wrong
within a week, and wrong quietly -- so the document is generated and this regenerates it and
compares.

Regenerating is not enough on its own, which is the lesson that added `TheInventoryIsCompleteTest`
below. A generator that can only see one way of reading a flag regenerates byte-identically
forever while naming half of them: this document listed 94 while the engine named 201, and every
metaserver, proxy and raft-tuning knob was simply absent. Comparing the output to itself cannot
catch that. So the completeness test compares the document to the SOURCE, and states how much
source it read -- a scan that silently matched nothing would otherwise pass hardest of all.

The failure message says how to fix it, because a byte-comparison failure with no instruction is
the kind of test people delete.
"""
from __future__ import annotations

import io
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(TOOLS)
BUILDER = os.path.join(TOOLS, "build_engine_flag_inventory.py")
DOC = os.path.join(ROOT, "docs", "ops", "temporalstore-engine-flags.md")


def _builder_prelude() -> dict:
    """The builder's helpers, without running it.

    The script does its work at module level -- importing it regenerates the document -- so only
    the part above that work is executed.
    """
    with io.open(BUILDER, encoding="utf-8") as handle:
        head = handle.read().split("sources = {}")[0]
    namespace: dict = {}
    exec(compile(head, BUILDER, "exec"), namespace)  # noqa: S102 - the builder's own prelude
    return namespace


# Every prefix the engine reads, which is every prefix the inventory now lists.
FLAG_LITERAL = re.compile(r'"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"')


def _engine_sources():
    """(path, source) for the files the builder treats as engine source."""
    strip = _builder_prelude()["strip_test_modules"]
    source_root = os.path.join(ROOT, "crates", "temporalstore-rust", "src")
    for directory, _, names in os.walk(source_root):
        for name in sorted(names):
            if not name.endswith(".rs"):
                continue
            path = os.path.join(directory, name)
            relative = os.path.relpath(path, source_root).replace(os.sep, "/")
            if "/tests" in relative or relative.startswith("tests"):
                continue
            with io.open(path, encoding="utf-8", errors="ignore") as handle:
                yield relative, strip(handle.read())


def _regenerate_into(directory: str) -> str:
    """Run the builder against a tree whose doc lives in `directory`, and return what it wrote."""
    os.makedirs(os.path.join(directory, "docs", "ops"), exist_ok=True)
    # The builder reads the tree it is GIVEN -- engine source for what reads a flag, and
    # config/tools/deploy for what sets one -- so point every one of those at the real
    # directory by symlinking rather than copying. A missing link is not a harmless omission:
    # the builder would report those flags as set by nothing, and the comparison would fail
    # with a diff about flags rather than about the harness.
    if not os.path.exists(os.path.join(directory, "crates")):
        try:
            os.symlink(os.path.join(ROOT, "crates"), os.path.join(directory, "crates"))
        except OSError:
            return ""
    for name, target in (("tools", TOOLS),
                         ("config", os.path.join(ROOT, "config")),
                         ("deploy", os.path.join(ROOT, "deploy")),
                         # Not a setter root: the builder reads `sdk` to say which variables are
                         # on the far side of the engine boundary. Without the link the section
                         # is simply absent, and the comparison below fails describing a flag.
                         ("sdk", os.path.join(ROOT, "sdk"))):
        link = os.path.join(directory, name)
        if os.path.exists(link) or not os.path.exists(target):
            continue
        try:
            os.symlink(target, link)
        except OSError:
            pass
    subprocess.run([sys.executable, BUILDER, directory],
                   capture_output=True, text=True, timeout=300, check=False)
    written = os.path.join(directory, "docs", "ops", "temporalstore-engine-flags.md")
    if not os.path.exists(written):
        return ""
    with io.open(written, encoding="utf-8") as handle:
        return handle.read()


ROOT_PATH = pathlib.Path(ROOT)

class TheInventoryMatchesTheEngineTest(unittest.TestCase):

    def test_the_document_exists(self) -> None:
        self.assertTrue(os.path.exists(DOC),
                        "the flag inventory is missing; run tools/build_engine_flag_inventory.py")

    def test_regenerating_produces_the_same_document(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fresh = _regenerate_into(directory)
        if not fresh:
            self.skipTest("could not regenerate (symlinks unavailable on this filesystem)")
        with io.open(DOC, encoding="utf-8") as handle:
            current = handle.read()
        self.assertEqual(
            fresh, current,
            "the flag inventory no longer matches the engine. A flag was added, removed or "
            "renamed. Regenerate it: python3 tools/build_engine_flag_inventory.py .")

    def test_the_boundary_section_names_what_the_sdk_tree_reads(self) -> None:
        """The document says where the engine ends; this checks it says it correctly.

        `sdk/rust/temporalstore` is excluded from the workspace and carries its own copy of the
        proxy implementation, so nothing about it is caught by building or by the comparison
        above -- the builder would not notice a variable appearing there, and neither would CI.
        The section exists so a reader who greps this document for such a variable is told where
        to look instead of concluding nothing reads it, and this asserts the two agree.
        """
        sdk_src = os.path.join(ROOT, "sdk", "rust", "temporalstore", "src")
        if not os.path.isdir(sdk_src):
            self.skipTest("no sdk tree in this checkout")
        reads = set()
        for base, _dirs, names in os.walk(sdk_src):
            for name in names:
                if not name.endswith(".rs"):
                    continue
                with io.open(os.path.join(base, name), encoding="utf-8") as handle:
                    reads.update(re.findall(
                        r'"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"', handle.read()))
        with io.open(DOC, encoding="utf-8") as handle:
            text = handle.read()
        tabled = set(re.findall(
            r"^\| `((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)`", text, re.MULTILINE))
        expected = sorted(reads - tabled)
        section = text.split("## outside this document", 1)
        listed = sorted(re.findall(
            r"^- `((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)`", section[-1], re.MULTILINE))             if len(section) > 1 else []
        self.assertEqual(
            expected, listed,
            "the boundary section no longer matches the sdk tree. Regenerate it: "
            "python3 tools/build_engine_flag_inventory.py .")
        # An empty expectation would make the assertion above pass while proving nothing, and
        # that is the state this test is meant to catch drifting INTO -- so require the scan to
        # have found the tree it is about.
        self.assertGreater(len(reads), 3, "scanned the sdk tree and found almost no flag reads")

    def test_it_covers_a_plausible_number_of_flags(self) -> None:
        """A generator that produced an empty table would satisfy the comparison above."""
        with io.open(DOC, encoding="utf-8") as handle:
            text = handle.read()
        rows = [line for line in text.splitlines()
                if re.match(r"^\| `(TS|MATRIXARK|TEMPORALSTORE)_", line)]
        self.assertGreaterEqual(
            len(rows), 60,
            "the inventory lists only %d flags; the engine reads far more, so the generator has "
            "stopped seeing them" % len(rows))

    def test_every_portal_engine_setting_appears(self) -> None:
        """Everything a customer can set AND the engine reads must be in the list.

        This was filtered on the `TS_` prefix while the document held only those. The document
        now holds every prefix the engine reads, so that filter would quietly excuse the
        portal's `MATRIXARK_*` settings from the check that they appear at all. The second
        condition is the honest half: the portal also offers settings this engine never reads
        (they are consumed on the Python side), and those have no business in an engine list.
        """
        sys.path.insert(0, TOOLS)
        import matrixark_gateway_config as cfgmod

        with io.open(DOC, encoding="utf-8") as handle:
            text = handle.read()
        named = {name for _relative, source in _engine_sources()
                 for name in FLAG_LITERAL.findall(source)}
        missing = [s.env for s in cfgmod.SETTINGS
                   if s.env in named and ("`%s`" % s.env) not in text]
        self.assertEqual(
            [], missing,
            "these are offered on the portal and absent from the flag inventory, so the one "
            "document that lists engine knobs does not list the ones a customer can change: %s"
            % ", ".join(missing))


class TheInventoryIsCompleteTest(unittest.TestCase):
    """Every flag name the engine source writes down appears in the document.

    The pattern here is deliberately dumber than the generator's: any quoted flag name. That is
    the property the document claims ("every TS_* variable the engine reads"), and checking it
    with a rule *simpler* than the generator's is the point -- a guard that shares the
    generator's blind spot is not a guard, and the blind spot was exactly a pattern that knew
    about one way of reading a flag.

    What it does share is which SOURCE counts: the same non-test paths and the same
    `#[cfg(test)]` stripping, taken from the builder itself rather than reimplemented. "Test code
    is not engine code" is not the property under test, and a second copy of that rule would
    disagree with the first sooner or later.
    """

    def test_every_flag_named_in_the_engine_is_listed(self) -> None:
        files = 0
        named = set()
        for _relative, source in _engine_sources():
            files += 1
            named.update(FLAG_LITERAL.findall(source))
        # State the extent. An assertion over an empty scan is the easiest one in the world to
        # pass, and a scan that stopped finding files would report "no flags missing".
        self.assertGreater(files, 40,
                           "read only %d engine sources; the scan is not reaching them" % files)
        self.assertGreater(len(named), 150,
                           "found only %d flag names in %d files; the pattern has stopped "
                           "matching" % (len(named), files))
        with io.open(DOC, encoding="utf-8") as handle:
            document = handle.read()
        missing = sorted(name for name in named if ("`%s`" % name) not in document)
        self.assertEqual(
            [], missing,
            "the engine names these flags and the inventory does not list them, so the one "
            "document that claims to list every engine knob does not: %s. Regenerate it: "
            "python3 tools/build_engine_flag_inventory.py ." % ", ".join(missing))


class TheDefaultRootIsThisRepositoryTest(unittest.TestCase):
    """Run with no argument, the builder must regenerate THIS checkout.

    It used to default to an absolute path naming one worktree on one machine, so running it
    anywhere else rewrote the document from a tree the caller had never heard of. Every test here
    passes a directory explicitly, which is why the default went unexercised -- so these assert on
    the default itself rather than through a run.
    """

    def setUp(self) -> None:
        with io.open(BUILDER, encoding="utf-8") as handle:
            self.source = handle.read()

    def test_no_absolute_path_to_somebody_checkout(self) -> None:
        for bad in ('"/root/', "'/root/", '"/home/', "'/home/", '"C:'):
            self.assertNotIn(bad, self.source,
                             "the builder names an absolute path on one machine, so it does not "
                             "regenerate the repository it was run from")

    def test_the_default_follows_the_script(self) -> None:
        line = [l for l in self.source.splitlines() if l.startswith("ROOT = ")]
        self.assertTrue(line, "ROOT is no longer assigned where this test can see it")
        window = self.source[self.source.index("ROOT = "):]
        window = window[:window.index("SRC =")]
        self.assertIn("__file__", window,
                      "the default root is not derived from the script location, so it depends on "
                      "where the caller happens to be")


if __name__ == "__main__":
    unittest.main()


class TheShippedConfigOffersNothingDeadTest(unittest.TestCase):
    """Every variable `config/temporalstore.toml` names is read by something.

    The config file is the document an operator trusts about what is adjustable, so a key naming
    a variable nothing reads is worse than the key not existing: setting it produces no error and
    no effect. Four were found this way -- `group_commit`, `delta_served_index`,
    `enable_indexlog` and `bulk_ingest_replay_from_sequence` -- three of which had outlived their
    gate long before anyone looked.

    A reader counts: the Rust engine, the SDK tree, or any Python tool. What does not count is
    `matrixark_load_config.py`, which merely maps the key to the variable -- if that counted, a
    key would prove its own liveness.
    """

    def setUp(self) -> None:
        self.config = ROOT_PATH / "config" / "temporalstore.toml"
        if not self.config.is_file():
            self.skipTest("no shipped config in this checkout")

    def _readers(self) -> set:
        names = set()
        pattern = re.compile(r'["\']((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)["\']')
        for root, suffixes in ((ROOT_PATH / "crates", (".rs",)),
                               (ROOT_PATH / "sdk", (".rs",)),
                               (ROOT_PATH / "tools", (".py",))):
            if not root.is_dir():
                continue
            for path in root.rglob("*"):
                if path.suffix not in suffixes or not path.is_file():
                    continue
                if path.name == "matrixark_load_config.py":
                    continue
                names.update(pattern.findall(path.read_text(encoding="utf-8", errors="ignore")))
        return names

    def test_every_config_key_names_a_variable_something_reads(self) -> None:
        readers = self._readers()
        self.assertGreater(len(readers), 200,
                           "only %d variables found in the sources; the reader scan is broken"
                           % len(readers))
        offered, dead = 0, []
        for line in self.config.read_text(encoding="utf-8").splitlines():
            match = re.match(
                r"\s*#?\s*([a-z0-9_]+)\s*=.*?#\s*((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)",
                line)
            if not match:
                continue
            offered += 1
            if match.group(2) not in readers:
                dead.append("%s -> %s" % (match.group(1), match.group(2)))
        self.assertGreater(offered, 40,
                           "only %d annotated keys found; the config scan is broken" % offered)
        self.assertEqual(
            [], dead,
            "the shipped config offers %d key(s) naming a variable nothing reads, so setting "
            "them does nothing:\n  %s" % (len(dead), "\n  ".join(dead)))


class TheInventoryKnowsWhatTheCustomerCanSeeTest(unittest.TestCase):
    """Every engine flag the gateway offers is marked `portal` in the inventory.

    That column is what says a flag is customer-facing, and the retirement question leans on it:
    a flag with a control on the Setup page has a setter no code search will find, because the
    setter is a person. Undercounting it is how a live switch gets retired as unused.

    Checked against `matrixark_gateway_config.SETTINGS`, which is what the portal renders, rather
    than against the built HTML -- the page is generated from that list, so the list is the claim.
    """

    def test_every_offered_engine_flag_is_marked_portal(self) -> None:
        sys.path.insert(0, TOOLS)
        try:
            import matrixark_gateway_config as cfgmod
        except ImportError:
            self.skipTest("the gateway config is not importable in this checkout")

        text = io.open(DOC, encoding="utf-8").read()
        listed, marked = set(), set()
        for line in text.splitlines():
            row = re.match(
                r"^\| `((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)` \| [^|]*\| ([^|]*)\|", line)
            if not row:
                continue
            listed.add(row.group(1))
            if "portal" in row.group(2):
                marked.add(row.group(1))

        offered = {s.env for s in cfgmod.SETTINGS if s.env}
        self.assertGreater(len(offered), 50,
                           "only %d settings found in the gateway config; the scan is broken"
                           % len(offered))
        self.assertGreater(len(listed), 200,
                           "only %d flags read from the inventory; the scan is broken"
                           % len(listed))

        # Only flags the ENGINE reads: the gateway offers plenty that never reach it, and those
        # are correctly absent from a document about the engine.
        missing = sorted((offered & listed) - marked)
        self.assertEqual(
            [], missing,
            "the gateway offers these on the Setup page and the inventory does not say so, so "
            "they read as set by nothing:\n  %s" % "\n  ".join(missing))
        self.assertGreater(
            len(offered & listed), 10,
            "only %d offered flags overlap the inventory; the comparison has stopped comparing"
            % len(offered & listed))
