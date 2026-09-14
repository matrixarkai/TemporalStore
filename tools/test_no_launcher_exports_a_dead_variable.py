#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A launcher must not export a variable nothing reads.

Retiring a flag is half the job: the engine stops consulting it and every surface that OFFERS it
carries on offering. The shipped config has a guard for that and the portal has one. The launch
scripts did not, and that is the surface where it is least visible -- an export puts the variable
into the environment of every process the script starts, where it sits beside the ones that decide
something and cannot be told apart from them.

Three were found when this was written, and one had been made hours earlier by retiring
`TS_INDEX_BINARY` from the engine without touching `deploy_profile_common.sh`. That is the whole
case for a guard rather than a habit.

The criterion is the SAFE one: a name counts as read if it appears ANYWHERE outside an export of
itself. A tighter test -- looking for `environ.get` and its friends -- reported
`MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS` as dead when it is read inside a call spanning four lines,
with the name on one of its own. A check that recommends deleting things has to be wrong in the
safe direction.

That criterion has one blind spot, and a fourth dead export sat in it: a COMMENT counts as a
mention. `TS_WAL_SINGLE_BARRIER` appeared exactly twice in the tree -- its own export in
`deploy_profile_common.sh`, and a doc comment in `engine.rs`. The function that comment sits on is
`wal_single_barrier()`, whose body is `!wal_legacy_recovery()`: the variable that decides it is
`TS_WAL_LEGACY_RECOVERY`, which the SAME comment names correctly four lines later. Two halves of
one comment, two different variables, and the profile exported the half that does nothing. An
operator setting it to 0 to get the historical three barriers back got single-barrier mode and no
message.

So there is now a second rule that repeats the first with comment lines removed. It is still far
weaker than "is read" -- it asks only whether some CODE line anywhere mentions the name, which is
what keeps `MATRIXARK_RUST_PROXY_DEDICATED_CLIENTS` and the two names read by being passed as an
argument to a helper (`MATRIXARK_EMBED_DRAINER_INTERVAL_MS`,
`TS_PAGE_STORE_COMPRESSION_ENABLED`) out of trouble.

Measured before adding it, over 104 exported names and 1582 tracked files: the comment-stripping
rule flags exactly ONE name, `TS_WAL_SINGLE_BARRIER`, and no others. A narrowing that produced a
second answer would have needed a different shape; this one produced no false positives at all,
which is the only reason it is safe to assert rather than report.

Nothing in the tree reads the environment by prefix or iterates `os.environ` / `env::vars()`, so an
exported name with no reader is genuinely inert -- checked, because if something consumed the
environment wholesale, "no reader" would not mean "no effect".
"""
from __future__ import annotations

import collections
import os
import re
import subprocess
import unittest
from typing import Dict, List

TOOLS = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(TOOLS)

_EXPORT = re.compile(
    r"^\s*export\s+((?:TS|MATRIXARK|TEMPORALSTORE)_[A-Z0-9_]+)\s*=", re.M)

# 103 when this was written. Asserted so a scan that stops matching fails rather than reporting
# that every export is consulted.
EXPECTED_EXPORT_FLOOR = 80

_SKIP_SUFFIXES = (".png", ".jpg", ".jpeg", ".gz", ".zip", ".pdf", ".ico")


def _tracked(pattern: str = "") -> List[str]:
    args = ["git", "ls-files"] + ([pattern] if pattern else [])
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True).stdout.split()


def _exported() -> Dict[str, List[str]]:
    found: Dict[str, List[str]] = collections.defaultdict(list)
    for rel in _tracked("*.sh"):
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8") as handle:
                text = handle.read()
        except OSError:
            continue
        for match in _EXPORT.finditer(text):
            found[match.group(1)].append(rel)
    return found


def _mentioned_elsewhere(names) -> set:
    seen = set()
    for rel in _tracked():
        if rel.endswith(_SKIP_SUFFIXES):
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                lines = handle.read().split("\n")
        except OSError:
            continue
        is_shell = rel.endswith(".sh")
        for line in lines:
            for name in names:
                if name in seen or name not in line:
                    continue
                if is_shell and re.match(r"\s*export\s+%s\s*=" % re.escape(name), line):
                    continue  # an export of itself is not a reader
                seen.add(name)
    return seen


#: Lines that are only a comment, per language. A doc comment naming a variable is exactly what
#: made TS_WAL_SINGLE_BARRIER look alive, so the second rule below does not read them.
_COMMENT_PREFIX = {
    ".rs": ("//", "/*", "*"),
    ".py": ("#",),
    ".sh": ("#",),
    ".toml": ("#",),
    ".yaml": ("#",),
    ".yml": ("#",),
    ".md": (),  # prose is not code; .md never counts as a mention under the second rule
}


def _strip_comment_lines(text: str, ext: str) -> str:
    prefixes = _COMMENT_PREFIX.get(ext)
    if prefixes is None:
        return text
    if not prefixes:
        return ""
    kept = [line for line in text.split("\n")
            if not line.lstrip().startswith(prefixes)]
    return "\n".join(kept)


def _mentioned_in_code(names) -> set:
    """Like _mentioned_elsewhere, but a comment line does not count as a mention."""
    seen = set()
    for rel in _tracked():
        if rel.endswith(_SKIP_SUFFIXES):
            continue
        try:
            with open(os.path.join(REPO, rel), encoding="utf-8", errors="replace") as handle:
                text = handle.read()
        except OSError:
            continue
        ext = os.path.splitext(rel)[1]
        if ext not in _COMMENT_PREFIX:
            continue  # an unknown file type is not evidence either way
        is_shell = rel.endswith(".sh")
        for line in _strip_comment_lines(text, ext).split("\n"):
            for name in names:
                if name in seen or name not in line:
                    continue
                if is_shell and re.match(r"\s*export\s+%s\s*=" % re.escape(name), line):
                    continue  # an export of itself is not a reader
                seen.add(name)
    return seen


class NoLauncherExportsADeadVariableTest(unittest.TestCase):

    def test_the_scan_still_finds_the_exports(self) -> None:
        exported = _exported()
        self.assertGreaterEqual(
            len(exported), EXPECTED_EXPORT_FLOOR,
            "found %d exported variables, expected at least %d -- if the scripts moved or the "
            "export shape changed, the assertion below passes on an empty set"
            % (len(exported), EXPECTED_EXPORT_FLOOR))

    def test_every_exported_variable_is_read_somewhere(self) -> None:
        exported = _exported()
        dead = sorted(set(exported) - _mentioned_elsewhere(set(exported)))
        self.assertEqual(
            [], dead,
            "these are exported by a launcher and appear nowhere else, so every process the "
            "script starts carries a setting that cannot take:\n  %s\nRemove the export, or point "
            "it at the name something actually reads."
            % "\n  ".join("%s (%s)" % (name, ", ".join(exported[name])) for name in dead))


    def test_a_comment_does_not_count_as_a_reader(self) -> None:
        """The second rule. Same shape as the first, with comment lines removed.

        Measured when added: over 104 exported names this flags exactly the one it was written
        for and nothing else, so it is asserted rather than reported.
        """
        exported = _exported()
        dead = sorted(set(exported) - _mentioned_in_code(set(exported)))
        self.assertEqual(
            [], dead,
            "these are exported by a launcher and appear nowhere but a COMMENT, so every process "
            "the script starts carries a setting that cannot take:\n  %s\nA note describing a "
            "variable is not a reader. Record the retired name in a comment in the launcher, the "
            "way TS_WAL_SINGLE_BARRIER is, instead of exporting it."
            % "\n  ".join("%s (%s)" % (name, ", ".join(exported[name])) for name in dead))

    def test_comment_stripping_actually_strips(self) -> None:
        """The load-bearing half of the second rule, pinned on a fixture rather than on the tree.

        If this regresses, every retired name that still has a note about it becomes invisible
        again -- and asserting it against the tree would only confirm what the tree holds today.
        """
        rust = ("// TS_A_COMMENTED_NAME described here and read nowhere\n"
                "/// TS_A_DOC_COMMENT_NAME likewise\n"
                " * TS_A_BLOCK_COMMENT_NAME likewise\n"
                "let v = env_bool(\"TS_A_REAL_READ\", false);\n")
        kept = _strip_comment_lines(rust, ".rs")
        self.assertIn("TS_A_REAL_READ", kept)
        for hidden in ("TS_A_COMMENTED_NAME", "TS_A_DOC_COMMENT_NAME",
                       "TS_A_BLOCK_COMMENT_NAME"):
            with self.subTest(name=hidden):
                self.assertNotIn(hidden, kept)
        kept = _strip_comment_lines("# MATRIXARK_NOTED\nx = get(\"MATRIXARK_READ\")\n", ".py")
        self.assertIn("MATRIXARK_READ", kept)
        self.assertNotIn("MATRIXARK_NOTED", kept)

    def test_the_retired_name_is_recorded_rather_than_deleted(self) -> None:
        """Recorded, not removed. A fork that set it must still find the explanation.

        This is the half a "just delete the dead line" fix loses, so it is asserted rather than
        trusted to survive the next tidy-up.
        """
        path = os.path.join(TOOLS, "deploy_profile_common.sh")
        with open(path, encoding="utf-8", errors="replace") as handle:
            profile = handle.read()
        self.assertIn(
            "TS_WAL_SINGLE_BARRIER", profile,
            "the retired name is gone from the profile entirely, so someone who set it in a fork "
            "greps and finds nothing -- the same silence the export produced")
        self.assertNotIn("export TS_WAL_SINGLE_BARRIER", profile,
                         "the profile exports the retired name again")
        self.assertIn(
            "TS_WAL_LEGACY_RECOVERY", profile,
            "the note no longer points at the variable that actually decides this")


if __name__ == "__main__":
    unittest.main()
