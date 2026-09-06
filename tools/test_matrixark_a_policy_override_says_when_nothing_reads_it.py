#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""An override for a knob nothing reads says so, as the settings field already does.

The policy screen offers a per-user or per-tenant override for every knob in the registry.
**Eleven of the thirty-two are read by nothing in this build** -- the set mx#1104 derived and
mx#1110 marked on the settings field. An override written here is stored, resolved, returned when
asked for, and inert, which is indistinguishable from one that works.

The flag travels with the knob for the same reason its description does, which the endpoint's own
docstring gives: *the explanation for a setting lives next to the setting rather than in a copy that
drifts.*

**Marked, not withdrawn.** The first version of this replaced the control. That repeats the mistake
mx#1123 fixed on the settings field, where the reset button was hidden for exactly the fields that
had something to clear: a tenant may already hold an override on one of these, and taking the
control away would leave that value set, invisible, and impossible to clear from this page.
"""
from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_tenant_policy as tp  # noqa: E402


class TheKnobsTheScreenOffersTest(unittest.TestCase):

    def test_the_policy_layer_offers_knobs_nothing_reads(self) -> None:
        """The premise. If it offered none of them there would be nothing to mark."""
        dead = sorted(set(tp.KNOBS) & set(cfg.KNOBS_READ_BY_NOTHING))
        self.assertGreater(len(dead), 5, dead)
        self.assertLess(len(dead), len(tp.KNOBS),
                        "every knob is dead, which would make the marking meaningless")

    def test_the_dead_set_is_the_registry_s_own(self) -> None:
        """Not a second list. mx#1104's guard derives it and fails in either direction."""
        for name in cfg.KNOBS_READ_BY_NOTHING:
            self.assertIn(name, tp.KNOBS, "%s is not a knob at all" % name)


class ThePayloadCarriesItTest(unittest.TestCase):

    @staticmethod
    def _knobs() -> dict:
        with io.open(os.path.join(TOOLS, "matrixark_v1_gateway.py"),
                     encoding="utf-8") as handle:
            return handle.read()

    def test_the_endpoint_marks_each_knob(self) -> None:
        self.assertIn('"read_by_nothing": name in _gwconfig.KNOBS_READ_BY_NOTHING',
                      self._knobs(),
                      "the policy payload does not say which knobs are inert")

    def test_it_is_keyed_on_the_knob_name(self) -> None:
        """The settings field needed the VARIABLE because its keys differ from knob names; here
        the loop variable IS the knob name, so a name match is right and a variable lookup would
        be the wrong indirection."""
        self.assertIn("for name, state in (described.get(\"knobs\") or {}).items():",
                      self._knobs())


class TheControlIsMarkedNotWithdrawnTest(unittest.TestCase):

    SCRIPT = """
const fs = require("fs");
const page = fs.readFileSync(process.argv[1], "utf8");
const start = page.indexOf("function policyControl(name, knob) {");
let depth = 0, end = -1;
for (let i = page.indexOf("{", start); i < page.length; i++) {
  if (page[i] === "{") depth++;
  else if (page[i] === "}") { depth--; if (depth === 0) { end = i + 1; break; } }
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const arg = JSON.parse(process.argv[2]);
const scope = { esc, settableHere: () => arg.settable };
const names = Object.keys(scope);
const policyControl = new Function(...names,
  page.slice(start, end) + "; return policyControl;")(...names.map((k) => scope[k]));
process.stdout.write(policyControl(arg.name, arg.knob));
"""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def render(self, knob: dict, settable: bool = True, name: str = "a_knob") -> str:
        payload = json.dumps({"name": name, "knob": knob, "settable": settable})
        out = subprocess.run(["node", "-e", self.SCRIPT,
                              os.path.join(PORTAL, "setup_portal.html"), payload],
                             capture_output=True, text=True, timeout=300)
        if out.returncode != 0:
            raise AssertionError(out.stderr)
        return out.stdout

    def test_a_live_knob_is_not_marked(self) -> None:
        """The floor. Marking everything would pass every test below."""
        html = self.render({"kind": "int", "value": "8", "read_by_nothing": False})
        self.assertIn("data-knob=", html)
        self.assertNotIn("not read by this build", html)

    def test_a_dead_knob_keeps_its_control(self) -> None:
        """The mx#1123 lesson: an override already stored must stay visible and clearable."""
        for kind, knob in (("int", {"kind": "int", "value": "0"}),
                           ("bool", {"kind": "bool", "value": True}),
                           ("choice", {"kind": "choice", "value": "auto",
                                       "choices": ["auto", "none"]})):
            with self.subTest(kind=kind):
                html = self.render(dict(knob, read_by_nothing=True))
                self.assertIn("data-knob=", html, "the control was withdrawn")
                self.assertIn("not read by this build", html)

    def test_a_tenant_only_knob_is_marked_too(self) -> None:
        """That branch returns early, so it needed the marker adding separately -- and a reader
        looking at a knob they cannot set here still deserves to know it does nothing anywhere."""
        html = self.render({"kind": "choice", "value": "auto", "choices": [],
                            "read_by_nothing": True}, settable=False)
        self.assertIn("not read by this build", html)
        self.assertIn("whole tenant", html)


if __name__ == "__main__":
    unittest.main()
