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

One case runs in ONE process on purpose, and it is asking a different question. The operator page
labels this control `live`, which is a claim about what happens AFTER startup -- so the fresh
process the tests above use cannot decide it. Measured in a process that had imported the codex
hook, writing the variable the way the portal's `update()` does:

    moment                         codex chars   agent chars
    at import (unset)                    12662          7620
    after a portal write of 2000         12662          1567
    after a portal write of 20000        12662         19740

The sibling hook tracked the write on every row. The codex hook never moved, because its limit
was a DEFAULT ARGUMENT -- `char_limit: int = DEFAULT_ADDITIONAL_CONTEXT_CHAR_LIMIT` -- and a
default argument is evaluated once, when the `def` runs.

The audit that exists to catch exactly this could not: it classifies a read by whether the
variable NAME sits inside a function, and this one did. `test_matrixark_gateway_config_audit` now
resolves a module-level call to a same-module helper, which is the shape that hid it. It still
cannot see a LITERAL default, because there is no variable name in it to find -- that is what
this test is for.
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



class TheLiveLabelIsTrueAfterStartupTest(unittest.TestCase):
    """`applies: live` is a claim about a change made to a RUNNING process.

    One process, one import, then a write -- which is what the portal does for a live setting: it
    assigns into `os.environ` of the process serving the page. Both hooks must follow it.
    """

    PROBE = r"""
import json, os, sys
sys.path.insert(0, %r)
os.environ.pop(%r, None)
import matrixark_codex_hook as codex
import matrixark_agent_hook as agent
pack = {"groups": [{"items": [{"text": ("x" * 500) + str(i)} for i in range(400)]}]}
def codex_chars():
    return len(codex.additional_context_from_retrieve(pack, query="q", local_context_count=0))
def agent_chars():
    return len(agent.additional_context_from_retrieve(pack))
out = {"raw": sum(len(i["text"]) for i in pack["groups"][0]["items"]), "rows": []}
for value in (None, "2000", "20000"):
    if value is None:
        os.environ.pop(%r, None)
    else:
        os.environ[%r] = value
    out["rows"].append({"set": value, "codex": codex_chars(), "agent": agent_chars()})
print(json.dumps(out))
"""

    @classmethod
    def setUpClass(cls) -> None:
        env = dict(os.environ)
        env.pop(FLAG, None)
        source = cls.PROBE % (str(TOOLS), FLAG, FLAG, FLAG)
        result = subprocess.run([sys.executable, "-B", "-c", source],
                                capture_output=True, text=True, cwd=str(TOOLS), env=env)
        if result.returncode != 0:
            raise AssertionError("in-process probe failed:\n%s" % result.stderr[-800:])
        cls.out = json.loads(result.stdout.strip().splitlines()[-1])

    def test_every_row_returned(self) -> None:
        """A probe that raised and was read for a key would report agreement it never measured."""
        self.assertEqual(3, len(self.out["rows"]))
        for row in self.out["rows"]:
            self.assertIsInstance(row["codex"], int)
            self.assertIsInstance(row["agent"], int)

    def test_the_fixture_is_bigger_than_every_limit_under_test(self) -> None:
        self.assertGreater(self.out["raw"], 20000,
                           "a pack smaller than every limit agrees for the wrong reason")

    def test_lowering_the_limit_after_startup_reaches_both_hooks(self) -> None:
        unset, lowered = self.out["rows"][0], self.out["rows"][1]
        for hook in ("codex", "agent"):
            with self.subTest(hook=hook):
                self.assertLessEqual(
                    lowered[hook], 2000,
                    "%s handed back %d characters after the page lowered the limit to 2000; the "
                    "setting is labelled live and this process was already running"
                    % (hook, lowered[hook]))
                self.assertLess(
                    lowered[hook], unset[hook],
                    "%s returned the same %d characters before and after the write, so the write "
                    "reached nothing" % (hook, lowered[hook]))

    def test_raising_it_again_reaches_both_hooks(self) -> None:
        """The control that separates 'followed the write' from 'is always small'."""
        lowered, raised = self.out["rows"][1], self.out["rows"][2]
        for hook in ("codex", "agent"):
            with self.subTest(hook=hook):
                self.assertGreater(raised[hook], lowered[hook],
                                   "%s did not grow when the limit was raised again" % hook)


if __name__ == "__main__":
    unittest.main()
