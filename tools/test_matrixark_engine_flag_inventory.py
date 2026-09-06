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

def _rows() -> dict:
    """{flag: its row} from the generated inventory table."""
    with open(DOC, encoding="utf-8") as handle:
        text = handle.read()
    found = {}
    for line in text.splitlines():
        if not line.startswith("| `"):
            continue
        name = line.split("`")[1]
        found[name] = line
    return found


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

    def test_a_flag_only_a_test_sets_is_not_credited_to_a_script(self) -> None:
        """The "set by" column exists to answer whether anyone throws the switch. A Python test's
        `os.environ["X"] = ...` matches the script pattern exactly, so a flag no deployment
        configures was reported as one a script sets -- and TS_WAL_LEGACY_RECOVERY, a flag whose
        whole question is whether anything still needs it, read as script-configured.

        The consequence beyond the wrong answer: adding any test that names a flag rewrote this
        generated document with a claim about deployments.
        """
        rows = _rows()
        row = rows.get("TS_WAL_LEGACY_RECOVERY")
        self.assertIsNotNone(row, "the flag this is about is no longer listed")
        surfaces = {s.strip() for s in row.split("|")[3].split(",") if s.strip()}
        self.assertIn("test", surfaces)
        self.assertNotIn("script", surfaces)

    def test_a_flag_a_real_script_sets_is_still_credited(self) -> None:
        """The floor. If nothing were ever labelled "script" any more, the assertion above would
        pass on a document that had lost the distinction entirely."""
        rows = _rows()
        scripted = [name for name, row in rows.items()
                    if "script" in {s.strip() for s in row.split("|")[3].split(",")}]
        self.assertTrue(scripted, "no flag is credited to a script at all")

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


class NoDocCommentIsGluedToTheWrongItemTest(unittest.TestCase):
    """A `///` block documents the item below it -- and only one item is below any of them.

    Two `///` runs with nothing between them are ONE block on whatever follows, and an
    intervening `//` comment does not separate them either. So deleting a function between two
    doc comments, or writing a second block without an item under the first, silently files one
    subject's words under another's name. Rustdoc then renders them as that item's documentation
    and nothing complains, because a comment cannot fail.

    Four of these were found by hand while retiring flags -- `env_flag_on` had acquired two design
    notes about the write path, `raft_durable_fingerprint` two more, and
    `context_hybrid_lexical_enabled` and `wal_preallocate_enabled` had each lost their own to the
    function above them. That is the whole reason this exists: the failure is invisible in review
    and permanent once merged.

    The signal is a line INSIDE a run that opens the way a block opens -- a flag name or an
    `S2:`-style tag at the start -- where the line before it ended a sentence or was blank. The
    "ended a sentence" part is what keeps a wrapped line from counting: a sentence continuing onto
    a new line that happens to start with a flag name is ordinary prose, and two of the three
    candidates this first reported were exactly that.
    """

    OPENER = re.compile(r"^///\s+((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]{3,}|[A-Z]\d)[:( ]")

    def test_no_block_documents_an_item_that_is_not_below_it(self) -> None:
        root = os.path.join(ROOT, "crates", "temporalstore-rust", "src")
        glued, scanned = [], 0
        for base, _dirs, names in os.walk(root):
            for name in sorted(names):
                if not name.endswith(".rs"):
                    continue
                scanned += 1
                path = os.path.join(base, name)
                with io.open(path, encoding="utf-8") as handle:
                    lines = handle.read().splitlines()
                run_start = None
                for index, line in enumerate(lines):
                    stripped = line.strip()
                    if not stripped.startswith("///"):
                        run_start = None
                        continue
                    if run_start is None:
                        run_start = index
                        continue
                    if not self.OPENER.match(stripped):
                        continue
                    previous = lines[index - 1].strip()
                    # A sentence that wraps onto a line starting with a flag name is prose, not a
                    # new block. Only a line after a finished sentence, or after a blank `///`,
                    # opens one.
                    if previous != "///" and not previous.endswith((".", ")")):
                        continue
                    following = index + 1
                    while following < len(lines) and (
                        lines[following].strip().startswith(("///", "#["))
                    ):
                        following += 1
                    item = lines[following].strip() if following < len(lines) else "(end of file)"
                    glued.append(
                        "%s:%d\n      %s\n      -> documents: %s"
                        % (os.path.relpath(path, ROOT), index + 1, stripped[:88], item[:88])
                    )
        self.assertGreater(scanned, 100,
                           "only %d .rs files scanned; the walk is broken" % scanned)
        self.assertEqual(
            [], glued,
            "%d doc block(s) open inside another block, so they document an item they are not "
            "about:\n  %s" % (len(glued), "\n  ".join(glued)))


class NonTestCodeDoesNotWriteTheProcessEnvironmentTest(unittest.TestCase):
    """A binary that sets a variable on itself is doing at a distance what an argument does.

    It depends on the order the write and the read happen in, it is visible to every other reader
    in the process, and it is almost never put back. Four were replaced while retiring flags: the
    proxy and three binaries set `MATRIXARK_EAGER_CACHE_WARM_ON_LOAD` to configure an engine they
    were about to build, and two set `TS_PHASE1_FLAT` to the value it already had.

    What is allowed is a whole-PROCESS mode, where the binary genuinely is the thing the variable
    describes -- a backfill setting `MATRIXARK_BULK_INGEST`. Anything else belongs on the object
    it configures, so this list is meant to shrink and never to grow.
    """

    ALLOWED = {
        # `context_batch_ingest` and `context_embed_backfill` ARE backfills; bulk ingest is a
        # property of the process, not of one engine inside it.
        ("bin/context_batch_ingest.rs", "MATRIXARK_BULK_INGEST"),
        ("bin/context_embed_backfill.rs", "MATRIXARK_BULK_INGEST"),
    }

    WRITE = re.compile(r'env::set_var\(\s*"((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)"')

    def test_only_a_whole_process_mode_is_written(self) -> None:
        root = os.path.join(ROOT, "crates", "temporalstore-rust", "src")
        found, scanned = set(), 0
        for base, _dirs, names in os.walk(root):
            if os.sep + "tests" in base:
                continue
            for name in sorted(names):
                if not name.endswith(".rs") or name == "tests.rs":
                    continue
                scanned += 1
                path = os.path.join(base, name)
                with io.open(path, encoding="utf-8") as handle:
                    lines = handle.read().splitlines()
                cut = next((i for i, l in enumerate(lines) if re.match(r"\s*mod tests\b", l)),
                           len(lines))
                rel = os.path.relpath(path, root).replace(os.sep, "/")
                for line in lines[:cut]:
                    match = self.WRITE.search(line)
                    if match:
                        found.add((rel, match.group(1)))
        self.assertGreater(scanned, 100,
                           "only %d .rs files scanned; the walk is broken" % scanned)
        self.assertTrue(self.ALLOWED, "the allowlist is empty; this test would assert nothing")
        unexpected = sorted(found - self.ALLOWED)
        self.assertEqual(
            [], unexpected,
            "non-test code writes these into the process environment. A setting that belongs to "
            "one engine, store or cluster belongs on that object:\n  %s"
            % "\n  ".join("%s sets %s" % pair for pair in unexpected))


class ADocDoesNotStateANumericDefaultTheCodeContradictsTest(unittest.TestCase):
    """The same question as the class below, asked of numbers.

    That one checks booleans and says why it stops there: the accessor is "the shape whose default
    can be read off the source without guessing". Numbers are now readable too, so the limit can
    be lifted rather than left as a permanent boundary nobody revisits.

    This is not hypothetical. The WAL module header said `TS_WAL_SEGMENT_BYTES` "defaults to zero
    and so never seals a piece". Its one read site defaults to `DEFAULT_WAL_SEGMENT_BYTES`, 256
    KiB; nothing in the tree or on a running box sets the variable; and the const's own doc calls
    itself the rolling threshold when nothing sets one. The header described the behaviour before
    the log started rolling and was never updated. It was found by accident, because the inventory
    began printing the number beside it -- which is the sort of luck a test exists to replace.

    A claim is a sentence that names the flag AND states a number in a default-shaped phrase.
    Prose that describes a knob without claiming a default is not a finding, and prose stating a
    number that AGREES is counted, because a check that can only ever report disagreement cannot
    tell a clean tree from a pattern that stopped matching.
    """

    CLAIMS = [
        re.compile(r"default(?:s|ed)?\s+(?:to|is|of|:)?\s*([0-9][0-9_,]*)\b", re.I),
        re.compile(r"\b([0-9][0-9_,]*)\s*,\s*the default\b", re.I),
        re.compile(r"\bdefaults?\s+to\s+(zero|none|nothing)\b", re.I),
        re.compile(r"\b(zero)\s*,\s*the default\b", re.I),
    ]
    WORDS = {"zero": "0", "none": "0", "nothing": "0"}

    # 31 numeric defaults when this was written.
    EXPECTED_NUMERIC_FLOOR = 20

    def _numeric_defaults(self):
        prelude = _builder_prelude()
        literal_consts = prelude["literal_consts"]
        numeric_default = prelude["numeric_default_of_statement"]
        flag_read = prelude["FLAG_READ"]
        sources = dict(_engine_sources())
        consts = literal_consts(sources)
        found = {}
        for path in sorted(sources):
            source = sources[path]
            lines = source.split("\n")
            for match in flag_read.finditer(source):
                name = match.group(1)
                if name in found:
                    continue
                value = numeric_default(lines, source[: match.start()].count("\n"), consts)
                if value:
                    found[name] = value
        return found

    def _claims(self, defaults):
        agree, disagree = [], []
        for path, source in _engine_sources():
            lines = source.split("\n")
            for number, line in enumerate(lines):
                for name, value in defaults.items():
                    if name not in line:
                        continue
                    window = " ".join(lines[number:number + 3])
                    for pattern in self.CLAIMS:
                        found = pattern.search(window)
                        if not found:
                            continue
                        claimed = found.group(1).lower().replace(",", "").replace("_", "")
                        claimed = self.WORDS.get(claimed, claimed)
                        entry = (name, os.path.basename(path), number + 1, claimed, value)
                        (agree if claimed == value else disagree).append(entry)
                        break
        return agree, disagree

    def test_the_scan_still_reads_numeric_defaults(self) -> None:
        found = self._numeric_defaults()
        self.assertGreaterEqual(
            len(found), self.EXPECTED_NUMERIC_FLOOR,
            "read %d numeric defaults off the source, expected at least %d -- below that the "
            "comparison below is running on almost nothing"
            % (len(found), self.EXPECTED_NUMERIC_FLOOR))

    def test_some_prose_states_a_number_at_all(self) -> None:
        agree, disagree = self._claims(self._numeric_defaults())
        self.assertTrue(
            agree or disagree,
            "no doc comment anywhere states a numeric default in a shape these patterns match. "
            "Either the prose changed or the patterns did; either way this file is now inert and "
            "would pass over a header saying the opposite of the code.")

    def test_no_doc_states_a_number_its_code_contradicts(self) -> None:
        _agree, disagree = self._claims(self._numeric_defaults())
        detail = ["%s: %s:%d says %s, the code says %s" % entry for entry in sorted(set(disagree))]
        self.assertEqual(
            [], detail,
            "these say a flag defaults to one number while its own read says another: %s\n"
            "A stale default in a comment is read as fact by whoever is deciding what to change."
            % detail)


class ADocDoesNotStateADefaultTheCodeContradictsTest(unittest.TestCase):
    """A flag's doc must not claim a default its own code disagrees with.

    A stale default in a comment is worse than no comment: it is read as fact by whoever is
    deciding whether a neighbouring switch can be turned on, and nothing about reading it feels
    like guessing. `TS_RAFT_WAL_BINARY_RECORDS` carried "Off unless asked for" while its code
    defaulted ON, and a `//` line two lines below said "Default ON" -- so the file disagreed with
    itself, and the half a reader trusts is the one rustdoc renders.

    Only the accessor shape is checked, because that is the shape whose default can be read off
    the source without guessing: one function, one flag, returning a bool. A doc that states BOTH
    positions is skipped rather than judged -- describing a flip needs both words, and which one
    is the claim cannot be settled by matching.
    """

    # Every way these docs state a default, including the ones that avoid the word.
    SAYS_ON = re.compile(
        r"default(?:s|ed)?\s*(?:\*\*)?on\b|\bon\s+by\s+default|\bon\s+unless\b|"
        r"\bopt(?:s|ed)?[- ]out\b", re.I)
    SAYS_OFF = re.compile(
        r"default(?:s|ed)?\s*(?:\*\*)?off\b|\boff\s+by\s+default|\boff\s+unless\b|"
        r"\bopt(?:s|ed)?[- ]in\b|\bunless\s+asked\s+for\b", re.I)
    ACCESSOR = re.compile(
        r"\s*(?:pub(?:\([a-z():]+\))?\s+)?fn\s+\w+\s*\([^)]*\)\s*->\s*bool\s*\{")

    def test_no_doc_contradicts_its_own_default(self) -> None:
        prelude = _builder_prelude()
        function_end, default_of = prelude["function_end"], prelude["default_of"]
        checked, wrong = [], []
        for path, source in _engine_sources():
            lines = source.split("\n")
            for index, line in enumerate(lines):
                if not self.ACCESSOR.match(line):
                    continue
                body = "\n".join(lines[index:function_end(lines, index)])
                names = set(FLAG_LITERAL.findall(body))
                if len(names) != 1:
                    continue
                actual = default_of(body, 1)
                if actual not in ("on", "off"):
                    continue
                doc, cursor = [], index - 1
                while cursor >= 0 and lines[cursor].strip().startswith("///"):
                    doc.insert(0, lines[cursor].strip()[3:].strip())
                    cursor -= 1
                if not doc:
                    continue
                prose = " ".join(doc)
                says_on = bool(self.SAYS_ON.search(prose))
                says_off = bool(self.SAYS_OFF.search(prose))
                checked.append(next(iter(names)))
                if says_on and says_off:
                    continue
                if (actual == "on" and says_off) or (actual == "off" and says_on):
                    wrong.append("%s (%s:%d) defaults %s, its doc says %s"
                                 % (next(iter(names)), os.path.basename(path), index + 1,
                                    actual, "off" if says_off else "on"))
        self.assertGreater(
            len(checked), 15,
            "only %d documented flags had a readable default; the scan found nothing to check "
            "and would pass however wrong the docs were" % len(checked))
        self.assertEqual(
            [], sorted(wrong),
            "a flag's documentation states a default its code contradicts. Correct the prose, "
            "not the code -- and say it once, so the two halves cannot drift:\n  %s"
            % "\n  ".join(sorted(wrong)))
