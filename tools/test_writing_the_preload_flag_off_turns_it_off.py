# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Writing the preload flag off has to turn it off.

`openai_compatible_hf_reader` read it as::

    os.environ.get("TEMPORALSTORE_HF_READER_PRELOAD", "1") != "0"

which has two faults, and the two lines directly above it get both right -- they strip, this one
did not:

* **it did not trim.** A shell export, a systemd `Environment=` line and a heredoc all leave
  whitespace behind, and `"0 " != "0"` is True. Writing the flag OFF turned it ON.
* **it heard only the literal `0`.** `false`, `no` and `off` are all "not 0", so every one of them
  read as ON too.

`test_a_flag_value_is_trimmed_before_it_is_parsed` records the same defect on the Rust side, where
`" 64".parse::<usize>()` is an `Err`. Python is not the same language -- `int(" 64 ")` is 64, so a
numeric read is safe here and a *comparison* is not. This is the comparison half.

The flag is read through `matrixark_mcp_env.env_bool` now, which strips, lowercases, checks the
shared vocabulary and falls back to the default for anything it does not recognise. That last part
matters as much as the rest: this switch defaults ON, so a typo must not turn it off.

Checked by evaluating the `default=` expression the file actually contains, pulled out by AST. A
test that restates the expression is testing the restatement.
"""

from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
READER = TOOLS / "openai_compatible_hf_reader.py"
FLAG = "TEMPORALSTORE_HF_READER_PRELOAD"

#: value -> what it must mean. Unrecognised values fall back to the default, which is ON.
EXPECTED = {
    "1": True, "true": True, "TRUE": True, "yes": True, "on": True, "ON": True,
    "0": False, "false": False, "FALSE": False, "no": False, "off": False, "OFF": False,
    # the trimming half: each of these means off and was read as on
    "0 ": False, " 0": False, "  0  ": False, " off": False, "off\t": False,
    # unrecognised: the default stands, so a typo cannot switch off a default-on flag
    "": True, "   ": True, "garbage": True, "2": True, "y": True, "n": True,
}

_PROBE = """
import sys, os, json
sys.path.insert(0, {tools!r})
try:
    from matrixark_mcp_env import env_bool as _env_bool
except Exception:
    _env_bool = None
out = {{}}
for value in {values!r}:
    os.environ[{flag!r}] = value
    try:
        out[value] = bool(eval({expr!r}))
    except Exception as exc:
        out[value] = "raised " + type(exc).__name__
os.environ.pop({flag!r}, None)
try:
    out["__unset__"] = bool(eval({expr!r}))
except Exception as exc:
    out["__unset__"] = "raised " + type(exc).__name__
print(json.dumps(out))
"""


def _default_expression():
    """The `default=` the file gives `--preload`, as source."""
    tree = ast.parse(READER.read_text(encoding="utf-8", errors="replace"))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_argument"
                and node.args and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "--preload"):
            for keyword in node.keywords:
                if keyword.arg == "default":
                    return ast.unparse(keyword.value)
    return None


def _evaluate(values):
    expr = _default_expression()
    if expr is None:
        return None, None
    code = _PROBE.format(tools=str(TOOLS), values=list(values), flag=FLAG, expr=expr)
    result = subprocess.run([sys.executable, "-B", "-c", code],
                            capture_output=True, text=True, timeout=300)
    lines = result.stdout.strip().splitlines()
    if not lines:
        raise AssertionError("could not evaluate %r: %s"
                             % (expr, (result.stderr.strip().splitlines() or ["?"])[-1]))
    return expr, json.loads(lines[-1])


class WritingThePreloadFlagOffTurnsItOffTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if not READER.exists():
            raise unittest.SkipTest("openai_compatible_hf_reader is not in this checkout")
        cls.expression, cls.answers = _evaluate(sorted(EXPECTED))

    def test_the_preload_default_expression_was_found(self) -> None:
        """Vacuity floor. With no expression there is nothing to evaluate and every check below
        would have to be skipped -- which reads the same as passing."""
        self.assertIsNotNone(
            self.expression,
            "no `default=` on the --preload argument. If the flag moved, this file has to move "
            "with it rather than quietly stop checking anything.")

    def test_the_flag_is_not_read_by_a_bare_comparison(self) -> None:
        """The shape that caused it. A `!= \"0\"` neither trims nor speaks the vocabulary, and it
        is the thing to notice before the behaviour drifts back."""
        self.assertNotIn(
            '!= "0"', (self.expression or "").replace("'", '"'),
            "the preload default is a bare comparison against \"0\" again: %s. That reads "
            "`0 ` and `false` as ON." % self.expression)

    def test_every_spelling_of_off_turns_it_off(self) -> None:
        for value, expected in sorted(EXPECTED.items()):
            if expected is not False:
                continue
            with self.subTest(value=value):
                self.assertIs(
                    False, self.answers.get(value),
                    "%s=%r left preload ON. Whitespace survives a shell export, a systemd "
                    "Environment= line and a heredoc, and `false`/`no`/`off` are the words the "
                    "rest of the tree accepts." % (FLAG, value))

    def test_every_spelling_of_on_leaves_it_on(self) -> None:
        for value, expected in sorted(EXPECTED.items()):
            if expected is not True:
                continue
            with self.subTest(value=value):
                self.assertIs(
                    True, self.answers.get(value),
                    "%s=%r turned preload OFF. Only the four false spellings should, and an "
                    "unrecognised value must fall back to the default rather than switching off "
                    "a flag that defaults on." % (FLAG, value))

    def test_an_unset_flag_still_preloads(self) -> None:
        self.assertIs(True, self.answers.get("__unset__"),
                      "preload no longer defaults on with %s unset" % FLAG)


if __name__ == "__main__":
    unittest.main()
