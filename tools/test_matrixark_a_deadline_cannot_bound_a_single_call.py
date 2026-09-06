#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A deadline cannot bound a single call, and the portal now says which control can.

Two controls decide how long a retrieve takes and only one of them can stop a slow call:

* the **retrieval deadline** is cooperative -- `deadline_exceeded()` is consulted between stages
  and every 64 or 128 candidates, and never around a call to the store;
* the **transport request timeout** is what a single call actually runs against.

So a deployment with a five-second deadline saw retrieves return after sixty. The deadline was not
being ignored; it was being checked at moments the call had not returned from. Neither control was
offered on the portal, and the transport timeout answered 20,000 in one module and 60,000 in two
others for the same variable, so there was no way to find out which a deployment was running.

`deadline_can_be_overrun_by_ms` states that gap instead of leaving it to be inferred.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_gateway_config as cfg  # noqa: E402
import matrixark_mcp_retrieve_planning as planning  # noqa: E402
import matrixark_mcp_runtime_config as runtime  # noqa: E402
import matrixark_v1_gateway as gateway  # noqa: E402

VARIABLES = ("MATRIXARK_RETRIEVAL_TIMEOUT_MS",
             "MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS",
             "MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS")

SETTINGS = {
    "retrieval.timeout_ms": "MATRIXARK_RETRIEVAL_TIMEOUT_MS",
    "limits.transport_request_timeout_ms": "MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS",
    "limits.transport_io_timeout_ms": "MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS",
}


class Case(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = {n: os.environ.get(n) for n in VARIABLES}
        for name in VARIABLES:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


class TheDeadlineIsResolvedInOnePlaceTest(Case):
    """The adapter resolved it inline, directly below a comment claiming the policy beside it came
    'from one implementation'. The audit policy had been consolidated; the deadline had not."""

    def inline(self, args, ranking):
        """What the adapter used to compute for itself."""
        raw = args.get("deadline_ms",
                       ranking.get("deadline_ms",
                                   os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS", 0)))
        return int(raw or 0)

    def test_the_shared_resolver_answers_the_same(self) -> None:
        cases = [
            ({}, {}, None),
            ({"deadline_ms": 1234}, {}, None),
            ({}, {"deadline_ms": 4321}, None),
            ({}, {}, "5000"),
            ({"deadline_ms": 10}, {"deadline_ms": 20}, "30"),
            ({}, {}, "0"),
        ]
        for args, ranking, env in cases:
            with self.subTest(args=args, ranking=ranking, env=env):
                if env is None:
                    os.environ.pop("MATRIXARK_RETRIEVAL_TIMEOUT_MS", None)
                else:
                    os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = env
                self.assertEqual(self.inline(args, ranking),
                                 planning.retrieval_deadline_ms(args, ranking))

    def test_the_cases_are_not_all_zero(self) -> None:
        """The floor: six cases that all resolve to 0 would agree trivially."""
        os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = "5000"
        self.assertEqual(5000, planning.retrieval_deadline_ms({}, {}))
        self.assertEqual(77, planning.retrieval_deadline_ms({"deadline_ms": 77}, {}))

    def test_the_adapter_no_longer_carries_its_own_copy(self) -> None:
        with open(os.path.join(TOOLS, "matrixark_local_adapter_retrieve.py"),
                  encoding="utf-8") as handle:
            source = handle.read()
        self.assertNotIn('os.environ.get("MATRIXARK_RETRIEVAL_TIMEOUT_MS"', source)
        self.assertIn("_retrieve_planning.retrieval_deadline_ms(args, ranking)", source)


class TheDeadlineIsCooperativeTest(unittest.TestCase):
    """Derived from where the check is, not asserted: this is the reason a deadline cannot bound a
    single call, and if it ever stopped being true the panel's claim would become false."""

    def deadline_check_sites(self):
        path = os.path.join(TOOLS, "matrixark_local_adapter_retrieve.py")
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read(), filename=path)
        found = []
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "deadline_exceeded"):
                found.append(node.lineno)
        return found

    def test_the_deadline_is_checked_in_many_places(self) -> None:
        """A cooperative deadline is one that is polled. If it were applied to the call instead
        there would be one site, not a dozen."""
        self.assertGreater(len(self.deadline_check_sites()), 8)

    def test_the_panel_says_it_is_cooperative(self) -> None:
        self.assertTrue(gateway._latency_summary()["deadline_is_cooperative"])


class ThePanelNamesWhatBoundsASlowCallTest(Case):

    def test_it_is_never_the_deadline(self) -> None:
        """Whatever the deadline is set to. It is not applied to the call."""
        for deadline in ("0", "5000", "300000"):
            with self.subTest(deadline=deadline):
                os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = deadline
                self.assertEqual("transport_request_timeout_ms",
                                 gateway._latency_summary()["bounds_a_slow_call"])

    def test_the_overrun_is_the_gap_between_them(self) -> None:
        """The live observation: a five-second deadline and a call able to run for sixty."""
        os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = "5000"
        os.environ["MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS"] = "60000"
        self.assertEqual(55000, gateway._latency_summary()["deadline_can_be_overrun_by_ms"])

    def test_lowering_the_transport_timeout_closes_it(self) -> None:
        """And the operator has a lever, which is the point of offering it."""
        os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = "5000"
        os.environ["MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS"] = "8000"
        self.assertEqual(3000, gateway._latency_summary()["deadline_can_be_overrun_by_ms"])

    def test_no_deadline_means_nothing_to_overrun(self) -> None:
        """0 here has to mean "no deadline is set", not "a deadline that is never exceeded"."""
        os.environ.pop("MATRIXARK_RETRIEVAL_TIMEOUT_MS", None)
        summary = gateway._latency_summary()
        self.assertEqual(0, summary["deadline_ms"])
        self.assertEqual(0, summary["deadline_can_be_overrun_by_ms"])

    def test_a_deadline_larger_than_the_transport_cannot_be_overrun(self) -> None:
        """The 300-second knob: the transport bounds it, so there is no overrun -- and the panel
        must not report a negative one."""
        os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = "300000"
        self.assertEqual(0, gateway._latency_summary()["deadline_can_be_overrun_by_ms"])

    def test_the_panel_reads_the_same_resolver_a_retrieve_does(self) -> None:
        os.environ["MATRIXARK_RETRIEVAL_TIMEOUT_MS"] = "4321"
        self.assertEqual(planning.retrieval_deadline_ms({}, {}),
                         gateway._latency_summary()["deadline_ms"])


class TheTransportTimeoutHasOneNumberTest(Case):
    """Unifying the two transport defaults was done separately, in #1039, and more simply: the odd
    literal was corrected in place with the reasoning written beside it.

    What this change owes is narrower. The panel below has to resolve the same timeout in order to
    report it, which is a SECOND site for that number, and nothing in the tree ties a constant to a
    literal in an argument parser. This does."""

    def backend_default(self, variable: str) -> int:
        """The number `matrixark_mcp_backends` actually parses, read out of its source."""
        with open(os.path.join(TOOLS, "matrixark_mcp_backends.py"), encoding="utf-8") as handle:
            source = handle.read()
        marker = 'os.environ.get("%s", "' % variable
        start = source.index(marker) + len(marker)
        return int(source[start:source.index('"', start)])

    def test_the_panel_resolves_the_number_the_backend_parses(self) -> None:
        for variable, constant in (
                ("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS",
                 runtime.DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS),
                ("MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS",
                 runtime.DEFAULT_TRANSPORT_IO_TIMEOUT_MS)):
            with self.subTest(variable=variable):
                self.assertEqual(
                    self.backend_default(variable), constant,
                    "the panel would report a timeout the backend does not use")

    def test_the_reader_found_a_real_number(self) -> None:
        """The floor: a parse that returned nothing would make the rule above compare two
        constants against each other and pass on anything."""
        self.assertGreater(self.backend_default("MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS"), 0)

    def test_it_is_read_per_call(self) -> None:
        os.environ["MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS"] = "9000"
        self.assertEqual(9000, runtime.transport_timeout_ms(
            "MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS",
            runtime.DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS))

    def test_a_bad_value_falls_back(self) -> None:
        """A socket timeout of zero never times out; nonsense would fail every call."""
        for raw in ("", "  ", "soon", "0", "-1"):
            with self.subTest(value=raw):
                os.environ["MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS"] = raw
                self.assertEqual(runtime.DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS,
                                 runtime.transport_timeout_ms(
                                     "MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS",
                                     runtime.DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS))


class ThePortalOffersThemTest(unittest.TestCase):

    def test_all_three_are_offered(self) -> None:
        for key, variable in SETTINGS.items():
            with self.subTest(setting=key):
                self.assertIn(key, cfg.SETTINGS_BY_KEY)
                self.assertEqual(variable, cfg.SETTINGS_BY_KEY[key].env)

    def test_they_take_effect_without_a_restart(self) -> None:
        for key in SETTINGS:
            with self.subTest(setting=key):
                self.assertEqual("live", cfg.SETTINGS_BY_KEY[key].applies)

    def test_the_declared_defaults_are_what_the_build_runs(self) -> None:
        self.assertEqual(0, int(cfg.SETTINGS_BY_KEY["retrieval.timeout_ms"].default))
        self.assertEqual(
            runtime.DEFAULT_TRANSPORT_REQUEST_TIMEOUT_MS,
            int(cfg.SETTINGS_BY_KEY["limits.transport_request_timeout_ms"].default))
        self.assertEqual(
            runtime.DEFAULT_TRANSPORT_IO_TIMEOUT_MS,
            int(cfg.SETTINGS_BY_KEY["limits.transport_io_timeout_ms"].default))

    def test_the_deadline_help_says_it_cannot_bound_a_call(self) -> None:
        """The one thing an operator has to know before setting it, or they will set it and
        conclude it does not work."""
        help_text = cfg.SETTINGS_BY_KEY["retrieval.timeout_ms"].help.lower()
        self.assertIn("cooperative", help_text)
        self.assertIn("cannot bound one slow backend call", help_text)


if __name__ == "__main__":
    unittest.main()
