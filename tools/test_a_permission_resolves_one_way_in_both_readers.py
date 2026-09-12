#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A permission read in two places must resolve the same way in both.

MATRIXARK_ALLOW_LOCAL_BACKEND is the one control in this tree that says a production profile may
use a local backend, and two different modules decide it:

* `matrixark_mcp_backends.validate_mcp_backend_policy` refuses the backend unless the constant
  MATRIXARK_ALLOW_LOCAL_BACKEND is true. It imports that constant from `matrixark_mcp_core`, which
  re-exports `matrixark_mcp_runtime_config`'s `env_bool("MATRIXARK_ALLOW_LOCAL_BACKEND", False)`.
* `matrixark_codex_hook.local_backend_allowed` answers the same question for the hook.

They agreed once, disagreed about the spelling "on", and agree again now that both go through the
same helper. What this file adds is that the agreement is CHECKED. A docstring in the hook said
for some time that "on" was refused there, which had stopped being true when the read was moved
onto `env_bool` -- a recorded constraint outliving its mechanism, and an expensive kind to leave
lying around, because a reader believes a production guard is narrower than it is.

Both readers resolve the flag at IMPORT, so each spelling is checked in its own interpreter. That
is what makes this a test of the two modules rather than of one import order.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))

#: Every spelling worth disagreeing about: the four TRUE_VALUES, a capitalised one because the
#: helper lowercases and a hand-rolled read might not, and two falses including the empty string an
#: unset variable is indistinguishable from.
SPELLINGS = ("1", "true", "yes", "on", "ON", "", "off", "nonsense")

_PROBE = """
import os, sys
sys.path.insert(0, %r)
import matrixark_mcp_runtime_config as runtime_config
import matrixark_codex_hook as hook
enforced = bool(runtime_config.MATRIXARK_ALLOW_LOCAL_BACKEND)
asked = bool(hook.local_backend_allowed())
sys.stdout.write("%%d %%d" %% (enforced, asked))
"""


def _resolve(value):
    """(what the MCP guard enforces, what the hook answers) for one spelling of the flag."""
    env = dict(os.environ)
    if value is None:
        env.pop("MATRIXARK_ALLOW_LOCAL_BACKEND", None)
    else:
        env["MATRIXARK_ALLOW_LOCAL_BACKEND"] = value
    out = subprocess.run([sys.executable, "-c", _PROBE % TOOLS], env=env,
                         capture_output=True, text=True, timeout=180)
    if out.returncode != 0:
        raise AssertionError("the probe did not run for %r:\n%s" % (value, out.stderr[-2000:]))
    enforced, asked = out.stdout.strip().split()
    return enforced == "1", asked == "1"


class APermissionResolvesOneWayInBothReadersTest(unittest.TestCase):

    def test_the_two_readers_agree_on_every_spelling(self) -> None:
        for value in SPELLINGS:
            with self.subTest(value=value):
                enforced, asked = _resolve(value)
                self.assertEqual(
                    enforced, asked,
                    "MATRIXARK_ALLOW_LOCAL_BACKEND=%r: the MCP guard enforces %s and the hook "
                    "answers %s. A permission that half-applies is worse than one that does not "
                    "apply, because each half looks correct on its own." % (value, enforced, asked))

    def test_an_unset_flag_permits_nothing(self) -> None:
        """The floor. Both readers agreeing on True everywhere would pass the check above."""
        enforced, asked = _resolve(None)
        self.assertFalse(enforced, "the MCP guard permits a local backend with the flag unset")
        self.assertFalse(asked, "the hook permits a local backend with the flag unset")

    def test_the_probe_can_tell_the_two_readers_apart(self) -> None:
        """Without this, a probe that read ONE module twice would agree with itself forever.

        `on` is the spelling the two actually disagreed about once, and `1` is the one the refusal
        message tells an operator to use. Both must turn the permission on, or the check above is
        agreeing about a flag nothing reads.
        """
        for value in ("1", "on"):
            with self.subTest(value=value):
                enforced, asked = _resolve(value)
                self.assertTrue(enforced and asked,
                                "%r does not permit a local backend in either reader" % value)


if __name__ == "__main__":
    unittest.main()
