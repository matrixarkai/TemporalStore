# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A validator this repository ships has to load, and say why if it cannot run.

`validate_storage_engine_9_phase_conformance` names 32 scripts across nine phases. Eight of those
names are not in this repository, and that is deliberate and already handled: the gate reports them
as ABSENT and explains that an absent validator is "work not yet wired", separate from a
conformance failure.

Two of the names that ARE here could not be imported at all. They import modules this repository
does not contain, so they died with a `ModuleNotFoundError` traceback before `main` was reached --
for every person who clones the repo, permanently. The gate reported them beside real conformance
failures, and a stranger reading the output had no way to tell which of the two kinds each was.

`validate_page_block_metrics_conformance` already had the answer and had written down why:

    the honest outcome is a stated failure rather than a traceback (which reads as a bug in the
    validator) or a zero exit (which reads as conformance verified)

So this holds two properties. Every listed validator that is present must import. And one that
cannot run for want of a module must say so in a line an operator can act on -- while still exiting
non-zero, because the alternative reads as the check having passed.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
ROOT = TOOLS.parent
GATE = TOOLS / "validate_storage_engine_9_phase_conformance.py"

#: Present, and cannot run here for want of a module this repository does not contain.
#:
#: Listed by name because the property being checked is about these two specifically: that they
#: state the reason instead of throwing. If a module arrives and one of them starts working, the
#: test below notices and says to drop it from here rather than keeping an entry that describes
#: nothing.
CANNOT_RUN_HERE = (
    "validate_temporalstore_next_performance_plan.py",
    "validate_temporalstore_performance_execution_redaction.py",
)


def _listed() -> list:
    """The script names the nine-phase gate lists, read from its source as data."""
    if not GATE.exists():
        return []
    try:
        tree = ast.parse(GATE.read_text(encoding="utf-8", errors="replace"))
    except SyntaxError:
        return []
    names = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "Phase" and len(node.args) >= 3):
            try:
                names.extend(ast.literal_eval(node.args[2]))
            except (ValueError, TypeError):
                continue
    return sorted(dict.fromkeys(names))


class AValidatorThisRepositoryShipsHasToLoadTest(unittest.TestCase):

    def test_the_phase_list_could_be_read(self) -> None:
        """Vacuity floor on the SCAN. An empty list makes every check below pass by iterating over
        nothing, which is indistinguishable from every validator being healthy."""
        if not GATE.exists():
            self.skipTest("the nine-phase gate is not in this checkout")
        listed = _listed()
        self.assertGreaterEqual(
            len(listed), 15,
            "only %d distinct script names parsed out of the nine-phase gate. It lists far more "
            "than that across its nine phases, so the parse is not reading the Phase entries "
            "and this file reports clean whatever state the validators are in." % len(listed))

    def test_some_listed_validators_are_actually_present(self) -> None:
        """The other half of the denominator: names are read, but if none of them resolve to a
        file the import check has nothing to import."""
        if not GATE.exists():
            self.skipTest("the nine-phase gate is not in this checkout")
        present = [n for n in _listed() if (TOOLS / n).exists()]
        self.assertGreater(
            len(present), 5,
            "only %d of the listed validators are in tools/. The import check below would be "
            "nearly empty, and an empty check passes." % len(present))

    def test_every_listed_validator_that_is_present_imports(self) -> None:
        """The property. A validator that cannot be imported cannot report anything -- not a pass,
        not a failure, not a reason."""
        if not GATE.exists():
            self.skipTest("the nine-phase gate is not in this checkout")
        for name in [n for n in _listed() if (TOOLS / n).exists()]:
            with self.subTest(validator=name):
                result = subprocess.run(
                    [sys.executable, "-B", "-c",
                     "import sys; sys.path.insert(0, %r); __import__(%r)"
                     % (str(TOOLS), name[:-3])],
                    capture_output=True, text=True, timeout=300)
                self.assertEqual(
                    0, result.returncode,
                    "%s is listed by the nine-phase gate and cannot be imported: %s. A validator "
                    "that dies at import reports nothing at all, and its failure is indexed beside "
                    "conformance results that mean something else entirely."
                    % (name, (result.stderr.strip().splitlines() or ["<no output>"])[-1]))

    def test_one_that_cannot_run_says_so_instead_of_throwing(self) -> None:
        for name in CANNOT_RUN_HERE:
            path = TOOLS / name
            if not path.exists():
                continue
            with self.subTest(validator=name):
                result = subprocess.run([sys.executable, "-B", str(path)], cwd=str(ROOT),
                                        capture_output=True, text=True, timeout=600)
                output = result.stdout + result.stderr
                self.assertNotIn(
                    "Traceback", output,
                    "%s still fails with a traceback, which reads as a bug in the validator "
                    "rather than a statement about what this repository contains:\n%s"
                    % (name, output.strip()[-400:]))
                self.assertNotEqual(
                    0, result.returncode,
                    "%s now exits 0. If its missing module arrived and the check really runs, "
                    "that is good news -- drop it from CANNOT_RUN_HERE. If it is still skipping "
                    "the check, a zero exit reads as conformance verified." % name)

    def test_the_stated_reason_names_what_is_missing(self) -> None:
        """A line an operator can act on has to say which module. "cannot run" on its own sends
        the reader back into the source to find out what for."""
        for name in CANNOT_RUN_HERE:
            path = TOOLS / name
            if not path.exists():
                continue
            with self.subTest(validator=name):
                result = subprocess.run([sys.executable, "-B", str(path)], cwd=str(ROOT),
                                        capture_output=True, text=True, timeout=600)
                output = (result.stdout + result.stderr).strip()
                self.assertIn("cannot run:", output,
                              "%s does not state a reason: %s" % (name, output[-300:]))
                self.assertIn("absent from this repository", output,
                              "%s states a reason that does not say what is missing: %s"
                              % (name, output[-300:]))


if __name__ == "__main__":
    unittest.main()
