"""The documented context-char limit reaches both hooks, not one of them.

`MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT` is offered on the operator page as "how many
characters of retrieved context ONE HOOK INVOCATION may hand back", and is listed in the generated
engine-flag inventory. `matrixark_codex_hook` resolved it. `matrixark_agent_hook` capped at a
literal 8000 and read nothing, so a value set on the page reached one of its two subjects and the
other ignored it silently -- the direction that matters for a cap, because the operator believes
they lowered it.

The tests run each case in a FRESH PROCESS. The limit is resolved per call here, but the Codex
reader binds its default at import, and a test that set the variable in-process would prove nothing
about how either behaves at startup.

The fixture pack is far larger than any limit under test, and that is asserted: a pack smaller than
every limit would produce the same output for all of them and agree for the wrong reason.
"""

import json
import os
import pathlib
import subprocess
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
FLAG = "MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT"

PROBE = r"""
import json, sys
sys.path.insert(0, %r)
import matrixark_agent_hook as h
items = [{"text": ("x" * 500) + str(i)} for i in range(400)]
print(json.dumps({"chars": len(h.additional_context_from_retrieve({"groups": [{"items": items}]}))}))
"""


def _chars(value):
    env = dict(os.environ)
    env.pop(FLAG, None)
    if value is not None:
        env[FLAG] = value
    res = subprocess.run([sys.executable, "-c", PROBE % str(TOOLS)],
                         capture_output=True, text=True, cwd=str(TOOLS), env=env)
    if res.returncode != 0:
        raise AssertionError("probe failed for %r:\n%s" % (value, res.stderr[-500:]))
    return json.loads(res.stdout.strip().splitlines()[-1])["chars"]


class TheContextLimitReachesBothHooksTest(unittest.TestCase):

    def test_the_fixture_is_bigger_than_the_limits(self):
        """Otherwise every case returns the whole pack and they agree for the wrong reason."""
        unset = _chars(None)
        self.assertLess(unset, 100000,
                        "the pack is not being capped at all; the fixture cannot prove a limit")
        self.assertGreater(unset, 1000)

    def test_a_set_value_is_honoured(self):
        small, large = _chars("2000"), _chars("20000")
        self.assertLess(small, 2001, "a limit of 2000 produced %d characters" % small)
        self.assertGreater(large, small,
                           "raising the limit did not produce more context (%d then %d)"
                           % (small, large))
        self.assertLess(large, 20001)

    def test_unset_is_unchanged(self):
        """The floor on this change: a deployment that has not set the control must not move."""
        self.assertEqual(8000 - _chars(None), 8000 - _chars(None))  # deterministic
        unset = _chars(None)
        self.assertLessEqual(unset, 8000)
        self.assertGreater(unset, 7000,
                           "the unset budget moved away from the 8000 this hook has always used")

    def test_a_value_below_the_floor_is_raised_to_it(self):
        """Same floor as the Codex reader, so one control does not mean two things."""
        self.assertGreater(_chars("500"), 0)
        self.assertLessEqual(_chars("500"), 1000)

    def test_an_unparseable_value_falls_back_rather_than_raising(self):
        """A hook that dies on a malformed number takes the turn with it."""
        self.assertEqual(_chars(None), _chars("garbage"))

    def test_both_hook_modules_name_the_control(self):
        """The point of the change. If either stops reading it, the page is offering a control that
        reaches one subject again -- which is how this started."""
        for name in ("matrixark_agent_hook.py", "matrixark_codex_hook.py"):
            text = (TOOLS / name).read_text(encoding="utf-8")
            self.assertIn(FLAG, text,
                          "%s no longer reads %s, so the operator page offers a limit that hook "
                          "ignores" % (name, FLAG))


if __name__ == "__main__":
    unittest.main()
