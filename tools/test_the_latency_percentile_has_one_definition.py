#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Two backends report `p99_latency_ms` under one name and compute it two ways.

`_percentile` is defined three times, nested as a method each time, and two of the three are in
the SAME module:

    matrixark_mcp_metrics             MatrixArkServiceMetrics      ceil(q * n) - 1
    matrixark_mcp_temporal_adapters   MatrixArkRustProxyClient     ceil(q * n) - 1
    matrixark_mcp_temporal_adapters   MatrixArkRustCdylibClient    round((n - 1) * q)

The first two are the nearest-rank definition. The third indexes a position on the closed
interval, which is a different definition of the same word. Both Rust clients then publish the
result under the same two keys, `p95_latency_ms` and `p99_latency_ms`, so one named metric is
computed two ways depending on which client served.

MEASURED AT THE QUANTILES ACTUALLY REQUESTED. The call sites ask only for 0.95 and 0.99 -- no
caller asks for a median -- so a difference that only showed at p50 would be unreachable. It does
not only show there. Sweeping sample counts 1..1000:

    q = 0.95   the two disagree for 425 of 1000 sample counts, first at n = 12
    q = 0.99   the two disagree for 485 of 1000 sample counts, first at n = 52

The disagreement is one element of the sorted sample, and it is one element AT THE TAIL, which is
where a latency percentile is most sensitive and least forgiving: at n = 52, q = 0.99 the cdylib
client names the 51st sample and the proxy client names the 52nd.

WHAT IS NOT A DIVERGENCE, checked before it was written down. The metrics copy alone ends with
`round(value, 3)` and the other two return the raw element, which looks like a third difference.
It cannot show: every call site already wraps the call in `round(..., 3)` itself. A control below
asserts that, because it was nearly recorded as a finding and is not one.

WHY NO GUARD SEES IT. `test_a_nested_helper_has_one_copy_too` reports nested helpers whose bodies
are IDENTICAL, and its `test_no_module_holds_the_same_body_twice` asserts that no module holds one
body under two names -- asserted EMPTY, which is true. Both copies here are in one module and
neither assertion can see them, because the bodies differ. That is the third place in this tree
where a scan keyed on sameness goes blind exactly when the thing it watches starts to differ; the
other two are recorded in `test_the_production_seed_set_is_wider_than_serving`.

THIS FILE DOES NOT ASSERT THAT THEY AGREE. Picking one definition changes a published number that
somebody may already be alerting on, and picking the other changes the other client's. That is a
decision about what `p99_latency_ms` means, not a cleanup. Recorded in both directions: a copy
that stops diverging fails here and asks which definition won.
"""
from __future__ import annotations

import ast
import importlib
import math
import os
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

HELPER = "_percentile"

#: (module stem, enclosing class, statement count today).
COPIES = (
    ("matrixark_mcp_metrics", "MatrixArkServiceMetrics", 4),
    ("matrixark_mcp_temporal_adapters", "MatrixArkRustProxyClient", 4),
    ("matrixark_mcp_temporal_adapters", "MatrixArkRustCdylibClient", 4),
)
METRICS, PROXY, CDYLIB = (stem_class[1] for stem_class in COPIES)

#: The quantiles any caller actually asks for. A difference outside these is unreachable.
SERVED_QUANTILES = (0.95, 0.99)

#: The keys both Rust clients publish the result under.
PUBLISHED_KEYS = ("p95_latency_ms", "p99_latency_ms")

#: Measured over sample counts 1..1000. Recorded as floors, not exact counts: the arithmetic is
#: fixed, but a floor says "this is common" without breaking on an off-by-one in the sweep range.
DISAGREEMENT_FLOOR = {0.95: 300, 0.99: 300}
SWEEP = 1000


def _source(stem):
    with open(os.path.join(TOOLS, stem + ".py"), encoding="utf-8", errors="replace") as handle:
        return handle.read()


def _method(stem, class_name):
    """The helper's AST node, found inside the class it is a method of."""
    for node in ast.walk(ast.parse(_source(stem))):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for inner in ast.walk(node):
                if isinstance(inner, ast.FunctionDef) and inner.name == HELPER:
                    return inner
    return None


def _body(node):
    body = node.body
    if (body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)):
        body = body[1:]
    return body


def _compiled(stem, class_name):
    """Run one copy, lifted out of the class it is a method of."""
    node = _method(stem, class_name)
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    environment = {"math": math}
    exec(compile(module, "<%s.%s>" % (stem, class_name), "exec"), environment)  # noqa: S102
    return environment[HELPER]


def _copies():
    return {class_name: _compiled(stem, class_name) for stem, class_name, _n in COPIES}


class TheLatencyPercentileHasOneDefinition(unittest.TestCase):

    def test_all_three_copies_are_there_to_compare(self) -> None:
        """A floor. Every assertion below passes over a helper that was not found."""
        for stem, class_name, statements in COPIES:
            with self.subTest(module=stem, cls=class_name):
                node = _method(stem, class_name)
                self.assertIsNotNone(
                    node,
                    "%s.%s no longer defines %s. If the three were consolidated, strike this file "
                    "and say which definition won" % (stem, class_name, HELPER))
                self.assertEqual(
                    statements, len(_body(node)),
                    "%s.%s is now %d statements, recorded as %d"
                    % (stem, class_name, len(_body(node)), statements))

    def test_the_two_rust_clients_disagree_at_the_quantiles_served(self) -> None:
        """The finding, measured where it is reachable rather than where it is easiest to show."""
        copies = _copies()
        for quantile in SERVED_QUANTILES:
            with self.subTest(quantile=quantile):
                disagreeing = []
                for count in range(1, SWEEP + 1):
                    values = [float(i) for i in range(count)]
                    if copies[CDYLIB](list(values), quantile) != copies[PROXY](list(values), quantile):
                        disagreeing.append(count)
                self.assertGreaterEqual(
                    len(disagreeing), DISAGREEMENT_FLOOR[quantile],
                    "the two Rust clients now agree about q=%.2f for all but %d of %d sample "
                    "counts. If one adopted the other's definition, that is the fix -- strike "
                    "this record and say which won" % (quantile, len(disagreeing), SWEEP))
                count = disagreeing[0]
                values = [float(i) for i in range(count)]
                self.assertEqual(
                    1.0,
                    abs(copies[CDYLIB](list(values), quantile) - copies[PROXY](list(values), quantile)),
                    "the two now differ by more than one element of the sorted sample at n=%d, "
                    "which is a bigger change than this record describes" % count)

    def test_the_metrics_copy_matches_the_proxy_definition(self) -> None:
        """Which copy is the odd one out, asserted rather than asserted-by-omission.

        Two of the three are the same definition. Recording that is what makes "the cdylib client
        is the outlier" a statement rather than an impression.
        """
        copies = _copies()
        for quantile in SERVED_QUANTILES:
            for count in (1, 4, 12, 52, 100, 200, 999):
                values = [float(i) for i in range(count)]
                with self.subTest(quantile=quantile, n=count):
                    self.assertEqual(
                        copies[METRICS](list(values), quantile),
                        copies[PROXY](list(values), quantile),
                        "the service-metrics copy and the proxy copy have stopped agreeing at "
                        "q=%.2f n=%d, so the outlier is no longer the one this record names"
                        % (quantile, count))

    def test_the_internal_rounding_cannot_show(self) -> None:
        """The control, and the thing this file nearly recorded as a third divergence.

        Only the metrics copy rounds inside the helper. Every call site rounds to the same three
        places itself, so the internal round changes nothing a caller can see. If a call site ever
        stops rounding, that difference becomes real and this test is where it surfaces.
        """
        adapters = _source("matrixark_mcp_temporal_adapters")
        for key in PUBLISHED_KEYS:
            with self.subTest(key=key):
                self.assertIn(
                    key, adapters,
                    "%s is no longer published by the adapters, so the two clients may no longer "
                    "report one named metric two ways" % key)
        calls = [line for line in adapters.splitlines() if "self._percentile(" in line
                 and "def " not in line]
        self.assertTrue(calls, "no call sites found, so this control is vacuous")
        unrounded = [line.strip() for line in calls if "round(" not in line]
        self.assertEqual(
            [], unrounded,
            "these call sites no longer round the result themselves, so the metrics copy's "
            "internal round() is now a real difference and this record must say so: %s"
            % "; ".join(unrounded))

    def test_both_clients_publish_the_same_metric_names(self) -> None:
        """Why it matters: one name, two definitions, depending on which client served."""
        adapters = _source("matrixark_mcp_temporal_adapters")
        tree = ast.parse(adapters)
        published = {PROXY: set(), CDYLIB: set()}
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef) or node.name not in published:
                continue
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Constant) and isinstance(inner.value, str)
                        and inner.value in PUBLISHED_KEYS):
                    published[node.name].add(inner.value)
        # EVERY recorded key from EVERY client, not merely one key from each. Asking whether a
        # class publishes "some recorded key" lets one of them rename a single key away while the
        # assertion still passes -- the collision this file is about is per NAME.
        for class_name, keys in sorted(published.items()):
            with self.subTest(cls=class_name):
                self.assertEqual(
                    set(PUBLISHED_KEYS), keys,
                    "%s now publishes %s, not the recorded %s. If it stopped reporting one of "
                    "them, that key's two definitions no longer collide under one name and this "
                    "record has to narrow" % (class_name, sorted(keys) or "none",
                                              sorted(PUBLISHED_KEYS)))


if __name__ == "__main__":
    unittest.main()
