#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""What reports on retrieval runs the same resolution retrieval runs.

Three surfaces described what a retrieve does, and none of them asked the retrieve path, because
it cannot be asked: ``matrixark_local_adapter_retrieval`` is a mixin split out of
``matrixark_mcp_local_adapter`` that imports back from it, so entering the cycle from outside
raises ``ImportError``. Each surface re-derived the answer instead.

**The metric parsed the flag the opposite way round.** It read ON for ``1 true yes on``; the
serving path reads ON *unless* the value is one of ``0 false no off ""``. The two agree on the
eight words both list and disagree on everything else. Measured over the 16 values in
``VALUE_SPACE`` below, five disagreed -- and the worst of them is ``disabled``, where retrieval
serves dense-only scoring and the dashboard reports the profile off, so monitoring confirms the
operator in exactly the belief the machine is contradicting.

**The one-box page asked the settings registry**, which answers "is a knob by this name offered"
rather than "what does a retrieve apply". Those were never the same answer -- the registry's ref
cap disagreed with the served one by up to 156x -- and when a later change stopped offering the
six settings the page named, both of its panels went from a wrong number to "This build offers
none of them." while the profile went on deciding every result and the caps went on cutting every
retrieve. Nothing failed. A panel rendering an empty state is a panel working.

So the assertions here are not that each surface produces the right string. They are that each one
**calls the same function**, and that a surface which could not call it says so rather than
rendering a default that looks like a reading.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_gateway_metrics as gwm  # noqa: E402
import matrixark_retrieval_effective as eff  # noqa: E402

TOOLS = os.path.dirname(os.path.abspath(__file__))
PORTAL = os.path.join(TOOLS, "portal")

# Eight words both parses list, and eight that separate them. The point of the second group is
# that the flag has no validation: an unrecognised value is not an error on either side, it is
# silently one of the two answers, and the two sides picked different ones.
VALUE_SPACE = ["1", "0", "true", "false", "yes", "no", "on", "off",
               "", "enabled", "disabled", "y", "2", "TRUE", "maybe", " 1 "]


def gauge(lines, name: str):
    for line in lines:
        if line.startswith(name + " "):
            return line.split(" ", 1)[1].strip()
    return None


class TheGaugeAgreesWithTheServingPathTest(unittest.TestCase):
    """The claim, over the value space rather than over three friendly values.

    The guard this replaces asserted the gauge for ``"1"``, ``"0"`` and unset -- three values where
    both parses agree -- and pinned the two copies of the DEFAULT string against each other. The
    defaults never drifted. The parses did, and nothing was looking at them.
    """

    def setUp(self) -> None:
        self.original = os.environ.get("MATRIXARK_ONEBOX_EMBEDDING_FIRST")
        self.addCleanup(self._restore)

    def _restore(self) -> None:
        if self.original is None:
            os.environ.pop("MATRIXARK_ONEBOX_EMBEDDING_FIRST", None)
        else:
            os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = self.original

    def test_the_gauge_says_what_the_serving_path_does_for_every_value(self) -> None:
        for raw in VALUE_SPACE:
            with self.subTest(value=raw):
                os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = raw
                served = eff.onebox_embedding_first()
                published = gauge(gwm.onebox_lines(),
                                  "matrixark_gateway_onebox_embedding_first")
                self.assertEqual("1" if served else "0", published,
                                 "with the variable set to %r retrieval scores %s and the gauge "
                                 "publishes %s" % (raw, "dense-only" if served else "blended",
                                                   published))

    def test_disabled_is_the_case_that_reads_backwards(self) -> None:
        """Named on its own because it is the one an operator can walk into.

        Writing ``disabled`` is a reasonable way to try to turn something off. It does not turn
        this off -- an unrecognised value reads as ON -- and the dashboard used to agree with the
        intent rather than the behaviour, which is the one direction a dashboard must never fail.
        """
        os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = "disabled"
        self.assertTrue(eff.onebox_embedding_first(),
                        "an unrecognised value reads as ON; if that changed, this test is stale "
                        "and the serving path moved")
        self.assertEqual("1", gauge(gwm.onebox_lines(),
                                    "matrixark_gateway_onebox_embedding_first"))

    def test_with_nothing_set_it_reports_the_default_which_is_on(self) -> None:
        """The case almost every deployment is in, and the one the first version got backwards."""
        os.environ.pop("MATRIXARK_ONEBOX_EMBEDDING_FIRST", None)
        self.assertEqual("1", gauge(gwm.onebox_lines(),
                                    "matrixark_gateway_onebox_embedding_first"))

    def test_a_read_that_failed_is_published_as_one(self) -> None:
        """The failure mode the whole arrangement exists to prevent.

        The first version of this gauge read the profile by importing the retrieval module, which
        is circular; the import raised, an ``except`` caught it, and the gauge published ``0``. A
        gauge saying "blended scoring" about a deployment running the profile ON is worse than no
        gauge, because every dashboard reading it describes the opposite of what happened. The
        readable series makes that state visible instead of letting it wear a plausible value.
        """
        lines = gwm.onebox_lines()
        self.assertEqual("1", gauge(lines, "matrixark_gateway_onebox_profile_readable"),
                         "the accessor module is a leaf and should always be readable")

    def test_when_the_read_really_fails_the_gauge_says_so(self) -> None:
        """The other half, and the half that was untested.

        Asserting the series is ``1`` on a healthy process says nothing about the case it exists
        for -- a mutation hardcoding it to ``1`` survived the first mutation run. The failure
        cannot happen by itself here, since the accessor is a leaf, so it is staged: ``None`` in
        ``sys.modules`` is what CPython leaves behind for a module that failed to import, and
        importing it again raises ``ImportError`` exactly as the original circular import did.
        """
        import matrixark_retrieval_effective  # noqa: F401 - ensure a real entry to put back
        saved = sys.modules["matrixark_retrieval_effective"]
        sys.modules["matrixark_retrieval_effective"] = None
        try:
            lines = gwm.onebox_lines()
        finally:
            sys.modules["matrixark_retrieval_effective"] = saved
        self.assertEqual("0", gauge(lines, "matrixark_gateway_onebox_profile_readable"),
                         "a read that did not happen is being published as one that did")
        self.assertIsNotNone(gauge(lines, "matrixark_gateway_onebox_embedding_first"),
                             "the profile series must still be emitted, so a dashboard reading "
                             "it sees the readable series fall to 0 beside it rather than the "
                             "series vanishing")


class OneDefinitionTest(unittest.TestCase):
    """The serving path and the surfaces run the same code, not equivalent code."""

    def test_the_serving_path_uses_the_accessor_the_surfaces_use(self) -> None:
        """Compared by SOURCE LOCATION, not by object identity and not by code object.

        ``import tools.x`` and ``import x`` execute the file twice and build two of everything --
        two module objects, two function objects, and two CODE objects. An earlier version of this
        compared ``__code__`` on the belief that the code object at least was shared. It is not:
        this passed alone, with one module object in the process, and failed under the full suite,
        where something has already imported the package path. That is the very duplication the
        accessor module exists to escape, reappearing in the test for it.

        ``co_filename`` needs normalising for the same reason -- with both ``.`` and ``tools`` on
        ``sys.path`` one file is recorded as ``tools/x.py`` and ``./tools/x.py``.

        What the claim means is that the serving path's accessor is the one defined in
        matrixark_retrieval_effective: same file, same line, however many times Python ran it.
        """
        import matrixark_mcp_local_adapter  # noqa: F401 - the cycle must be entered from the top
        import matrixark_local_adapter_retrieval as serving

        def where(fn):
            code = fn.__code__
            return (os.path.realpath(code.co_filename), code.co_name, code.co_firstlineno)

        for name in ("onebox_embedding_first", "flag_enabled", "retrieval_scan_projection"):
            with self.subTest(name=name):
                mine, theirs = where(getattr(serving, name)), where(getattr(eff, name))
                self.assertEqual(theirs, mine,
                                 "%s on the serving path is defined somewhere other than the "
                                 "accessor module, so it is a second copy of the rule -- and two "
                                 "copies agree until one is edited" % name)
                self.assertEqual(
                    os.path.basename(mine[0]), "matrixark_retrieval_effective.py",
                    "%s is not defined in the accessor module at all" % name)

    def test_the_serving_path_and_the_surfaces_agree_on_every_value(self) -> None:
        """The property the location check stands in for, asserted directly.

        Same-file-same-line is strong evidence of one definition, but the thing that actually
        matters is that the serving path and the surfaces answer alike -- including for the
        unrecognised values where the two old parses came apart.
        """
        import matrixark_mcp_local_adapter  # noqa: F401
        import matrixark_local_adapter_retrieval as serving
        original = os.environ.get("MATRIXARK_ONEBOX_EMBEDDING_FIRST")
        self.addCleanup(lambda: (os.environ.__setitem__("MATRIXARK_ONEBOX_EMBEDDING_FIRST",
                                                        original)
                                 if original is not None
                                 else os.environ.pop("MATRIXARK_ONEBOX_EMBEDDING_FIRST", None)))
        for raw in VALUE_SPACE:
            with self.subTest(value=raw):
                os.environ["MATRIXARK_ONEBOX_EMBEDDING_FIRST"] = raw
                self.assertEqual(eff.onebox_embedding_first(),
                                 serving.onebox_embedding_first(),
                                 "with the variable set to %r the serving path and the accessor "
                                 "the surfaces call disagree" % raw)

    def test_the_budget_rule_is_one_rule(self) -> None:
        import matrixark_mcp_local_adapter  # noqa: F401
        import matrixark_local_adapter_retrieve as retrieve
        for name, fallback in (("max_selected_refs", 1000), ("top_k_per_layer", 8)):
            with self.subTest(cap=name):
                self.assertEqual(eff.tenant_retrieval_limit(name, None, fallback),
                                 retrieve._tenant_retrieval_limit(name, None, fallback))

    def test_the_metrics_default_is_not_a_second_copy(self) -> None:
        self.assertIs(gwm.ONEBOX_PROFILE_DEFAULT, eff.ONEBOX_EMBEDDING_FIRST_DEFAULT)

    def test_the_accessor_module_can_be_imported_from_outside_the_cycle(self) -> None:
        """The property that makes all of the above possible, asserted in a FRESH process.

        Asserting it in this one proves nothing: by the time this runs, the adapter has been
        imported and the cycle is resolved. The whole failure was that a surface importing the
        accessor first, with nothing else loaded, could not.
        """
        out = subprocess.run(
            [sys.executable, "-c",
             "import matrixark_retrieval_effective as e; print(e.onebox_embedding_first())"],
            cwd=TOOLS, capture_output=True, text=True, timeout=120)
        self.assertEqual(0, out.returncode,
                         "the accessor module cannot be imported on its own:\n%s"
                         % out.stderr.strip()[:600])

    def test_importing_it_pulls_in_nothing_that_could_cycle(self) -> None:
        """A leaf that quietly grows an import of the adapter is a leaf that stops being one, and
        the symptom is the original bug returning in whichever surface imports it first."""
        out = subprocess.run(
            [sys.executable, "-c",
             "import sys, matrixark_retrieval_effective;"
             "print(' '.join(sorted(m for m in sys.modules if m.startswith('matrixark'))))"],
            cwd=TOOLS, capture_output=True, text=True, timeout=120)
        self.assertEqual(0, out.returncode, out.stderr[:400])
        pulled = set(out.stdout.split())
        self.assertNotIn("matrixark_mcp_local_adapter", pulled)
        self.assertNotIn("matrixark_local_adapter_retrieval", pulled)
        self.assertNotIn("matrixark_local_adapter_retrieve", pulled)


class TheEndpointTest(unittest.TestCase):

    def test_the_route_is_documented_and_admin_scoped(self) -> None:
        import matrixark_v1_gateway as gw
        found = [d for d in gw.ROUTE_DOCS if d.get("path") == "/v1/admin/retrieval"]
        self.assertEqual(1, len(found))
        self.assertEqual("admin", found[0].get("scope"),
                         "what a deployment applies is operator information, not public")

    def test_it_reports_both_halves(self) -> None:
        body = eff.effective_retrieval(None)
        self.assertIn("profile", body)
        self.assertEqual(3, len(body["caps"]),
                         "the page's heading promises three caps")

    def test_every_cap_carries_the_default_it_fell_back_to(self) -> None:
        """Without it the page cannot distinguish a deployment running a cap somebody set from one
        running the build's, which is the question an operator opens the page with."""
        for cap in eff.effective_retrieval(None)["caps"]:
            with self.subTest(cap=cap["name"]):
                self.assertIn("build_default", cap)
                self.assertIn("env", cap)

    def test_exactly_one_cap_is_the_one_that_cuts(self) -> None:
        cuts = [c["name"] for c in eff.effective_retrieval(None)["caps"] if c.get("cuts")]
        self.assertEqual(["max_selected_refs"], cuts)


class WhichLevelSuppliedItTest(unittest.TestCase):
    """A cap names an environment variable, and a tenant override beats that variable.

    Without the level, the page can print `MATRIXARK_MAX_SELECTED_REFS` beside a number that
    variable did not supply, and an operator sets it, sees nothing change, and has nothing on the
    page to explain why.

    The page must not work the level out for itself -- that would be a second copy of the
    precedence, which is the defect this whole change removes. It also cannot be done correctly
    from outside: see the fall-through case below. So the resolver reports what it chose.
    """

    KNOB = "max_selected_refs"
    ENV = "MATRIXARK_MAX_SELECTED_REFS"

    def setUp(self) -> None:
        self.original = os.environ.get(self.ENV)
        self.addCleanup(self._restore)
        os.environ.pop(self.ENV, None)

    def _restore(self) -> None:
        if self.original is None:
            os.environ.pop(self.ENV, None)
        else:
            os.environ[self.ENV] = self.original

    @staticmethod
    def _set_policy(tenant, knobs) -> None:
        """Set the policy on EVERY loaded copy of the module.

        Under `unittest discover` it is imported under two names in one process -- plain and
        `tools.`-prefixed -- and each copy keeps its own record store. Setting it on the copy this
        test imported while the resolver's lazy import binds the other makes the override vanish,
        and the failure reads as "the knob is not wired", which is exactly what it is not.
        """
        for module in list(sys.modules.values()):
            if (getattr(module, "__name__", "").endswith("matrixark_tenant_policy")
                    and hasattr(module, "set_tenant_policy")):
                module.set_tenant_policy(tenant, knobs)

    def _cap(self, scope):
        for cap in eff.effective_retrieval(scope)["caps"]:
            if cap["name"] == self.KNOB:
                return cap
        raise AssertionError("%s is not among the reported caps" % self.KNOB)

    def test_nothing_set_is_reported_as_the_build_default(self) -> None:
        cap = self._cap("level_none")
        self.assertEqual("default", cap["source"])
        self.assertEqual(cap["build_default"], cap["value"])

    def test_the_environment_is_reported_as_the_environment(self) -> None:
        os.environ[self.ENV] = "77"
        cap = self._cap("level_env")
        self.assertEqual(77, cap["value"])
        self.assertEqual("environment", cap["source"])

    def test_a_tenant_override_is_reported_as_the_tenant(self) -> None:
        self._set_policy("level_tenant", {self.KNOB: 55})
        cap = self._cap("level_tenant")
        self.assertEqual(55, cap["value"])
        self.assertEqual("tenant", cap["source"])

    def test_the_tenant_beats_the_variable_and_the_report_says_so(self) -> None:
        """The case the page exists to explain. Both are set; only one is in force."""
        os.environ[self.ENV] = "77"
        self._set_policy("level_both", {self.KNOB: 55})
        cap = self._cap("level_both")
        self.assertEqual(55, cap["value"], "the tenant override did not win")
        self.assertEqual("tenant", cap["source"],
                         "the variable is named on the page beside a value it did not supply")

    def test_a_policy_that_names_the_knob_but_supplies_nothing_falls_through(self) -> None:
        """Why the page cannot work this out for itself.

        A tenant policy carrying a zero for the knob is IGNORED by the resolver -- a budget of
        nothing returns nothing at all, which is worse than ignoring a bad setting -- so the
        environment supplies the value. A surface asking "does the policy mention this knob?"
        would answer "tenant" here and be wrong. Only the resolver knows which level it used.
        """
        os.environ[self.ENV] = "77"
        self._set_policy("level_zero", {self.KNOB: 0})
        cap = self._cap("level_zero")
        self.assertEqual(77, cap["value"])
        self.assertEqual("environment", cap["source"],
                         "a policy that mentions the knob without supplying a usable value is "
                         "being reported as the level in force")

    def test_the_value_half_is_unchanged(self) -> None:
        """explicit_int is now the first element of explicit_int_with_source. It must still answer
        exactly as before for every level, or this refactor moved a serving-path number."""
        import matrixark_tenant_policy as tp
        self._set_policy("level_same", {self.KNOB: 55})
        for scope in ("level_same", "level_none", None):
            with self.subTest(scope=scope):
                value, _ = tp.explicit_int_with_source(self.KNOB, scope, 1000)
                self.assertEqual(tp.explicit_int(self.KNOB, scope, 1000), value)


class ThePageRendersItTest(unittest.TestCase):
    """The shipped page JS, run against what the endpoint really returns."""

    def setUp(self) -> None:
        if subprocess.run(["node", "--version"], capture_output=True).returncode != 0:
            self.skipTest("node is not available")

    def test_the_panels_render_the_served_values(self) -> None:
        payload = json.dumps(dict(eff.effective_retrieval(None), known=True))
        handle = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8")
        with handle:
            handle.write(payload)
        self.addCleanup(os.unlink, handle.name)
        out = subprocess.run(
            ["node", os.path.join(PORTAL, "onebox_effective_harness.js"),
             os.path.join(PORTAL, "onebox_portal.html"), handle.name],
            capture_output=True, text=True, timeout=300)
        self.assertEqual(0, out.returncode,
                         (out.stdout + out.stderr)[-3000:])


if __name__ == "__main__":
    unittest.main()
