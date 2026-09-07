# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A shell script may not source a file that is not here.

With `set -euo pipefail` a missing `source` is not a degraded mode: the script exits on that line,
before it has done anything, and the error names a path instead of a cause. Both scripts that
source a fixed path in this repository were dead that way --
`tools/temporalstore_runtime_env.sh` is documented in the crate README as the file launchers
consume, and it had never been committed.

Only statically resolvable targets are checked. A path still holding a shell variable other than
the repository root depends on runtime state and cannot be decided here, so it is skipped rather
than guessed at.
"""
import os
import re
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE_LINE = re.compile(r"^\s*(?:source|\.)\s+[\"']?([^\"'\s;]+)", re.M)

# A floor: if the walk stops finding scripts, every assertion below passes on an empty set.
MIN_SHELL_SCRIPTS = 15


def _shell_scripts():
    found = []
    for dirpath, dirs, files in os.walk(REPO):
        dirs[:] = [d for d in dirs if d not in (".git", "target", "node_modules")]
        for name in files:
            if name.endswith(".sh"):
                found.append(os.path.join(dirpath, name))
    return sorted(found)


def _resolvable_sources():
    """(script, resolved target) for every source directive with no unresolved variable left."""
    out = []
    for path in _shell_scripts():
        try:
            with open(path, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
        except OSError:
            continue
        for raw in SOURCE_LINE.findall(text):
            resolved = raw
            for token in ("${ROOT}", "$ROOT", "${REPO_ROOT}", "$REPO_ROOT"):
                resolved = resolved.replace(token, REPO)
            if "$" in resolved:
                continue
            if not os.path.isabs(resolved):
                resolved = os.path.join(os.path.dirname(path), resolved)
            out.append((path, resolved))
    return out


class ASourcedFileIsInTheRepository(unittest.TestCase):
    def test_there_are_scripts_to_check(self):
        """Non-vacuity: with no scripts found, the check below passes having examined nothing."""
        scripts = _shell_scripts()
        self.assertGreaterEqual(
            len(scripts), MIN_SHELL_SCRIPTS,
            "found %d shell scripts under %s, expected at least %d -- if the layout changed, the "
            "assertion below runs on an empty set" % (len(scripts), REPO, MIN_SHELL_SCRIPTS))

    def test_something_is_actually_resolvable(self):
        """The second half of the floor: scripts exist, but do any of them source a fixed path?"""
        self.assertTrue(
            _resolvable_sources(),
            "no source directive resolved to a fixed path, so the check below cannot fail")

    def test_no_script_sources_a_file_that_is_missing(self):
        missing = sorted(
            "%s -> %s" % (os.path.relpath(script, REPO), os.path.relpath(target, REPO))
            for script, target in _resolvable_sources()
            if not os.path.isfile(target))
        self.assertEqual(
            [], missing,
            "these scripts source a file that is not in the repository, so they exit on that line "
            "before doing anything: %s" % missing)


if __name__ == "__main__":
    unittest.main()
