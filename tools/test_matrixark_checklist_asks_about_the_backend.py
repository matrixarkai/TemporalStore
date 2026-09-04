#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The setup checklist asks whether the gateway can reach its backend.

Thirteen checks covered extraction, embedding, auth, content, memory, metrics and imports. None
asked the most basic question: is the datanode there. A deployment whose backend was unreachable
satisfied every one of those and still could not serve a request -- which is the exact failure mode
the checklist exists for, since it says of its other rows that each one "fails quietly".

The row goes first. If the backend is unreachable the rest of the list is moot, and a reader who
sees it at the top stops working through checks about models and keys.
"""
from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_v1_gateway as gw  # noqa: E402

SNAP = {"extraction": {}, "embedding": {}, "warnings": []}


def _rows(datanode):
    return gw._readiness_checks(SNAP, {}, gw.GatewayConfig(), datanode=datanode)


def _datanode_row(rows):
    found = [r for r in rows if r["id"] == "datanode"]
    return found[0] if found else None


class TheChecklistAsksIfTheBackendIsThereTest(unittest.TestCase):

    def test_a_reachable_backend_is_ok(self) -> None:
        row = _datanode_row(_rows("ok"))
        self.assertIsNotNone(row)
        self.assertEqual("ok", row["status"])

    def test_an_unreachable_backend_is_flagged(self) -> None:
        row = _datanode_row(_rows("unreachable"))
        self.assertEqual("warn", row["status"])
        self.assertIn("could not connect", row["detail"])

    def test_an_erroring_backend_is_flagged_differently(self) -> None:
        """A backend answering 5xx and one that is not listening want different things looked at."""
        row = _datanode_row(_rows("erroring"))
        self.assertEqual("warn", row["status"])
        self.assertIn("answered with an error", row["detail"])
        self.assertNotEqual(row["detail"], _datanode_row(_rows("unreachable"))["detail"])

    def test_it_says_the_worker_should_be_out_of_rotation(self) -> None:
        """Ties the row to what the orchestrator is already doing, so the two are not read apart."""
        self.assertIn("readyz", _datanode_row(_rows("unreachable"))["detail"])

    def test_nothing_probed_claims_nothing(self) -> None:
        """Absent is not unreachable. A row either way would be inventing an answer."""
        self.assertIsNone(_datanode_row(_rows(None)))

    def test_an_unrecognised_state_is_not_waved_through(self) -> None:
        """A state this gateway does not know is a warning, not a silent pass."""
        row = _datanode_row(_rows("something_new"))
        self.assertIsNotNone(row, "an unknown state produced no row at all")
        self.assertEqual("warn", row["status"])

    def test_it_comes_first(self) -> None:
        for state in ("ok", "unreachable", "erroring"):
            self.assertEqual("datanode", _rows(state)[0]["id"],
                             "with the backend %s, the reader works through other checks first"
                             % state)

    def test_the_other_checks_are_still_there(self) -> None:
        """Adding a row must not displace the list it was added to."""
        ids = {r["id"] for r in _rows("ok")}
        for expected in ("extraction", "embedding", "auth", "content", "memory", "metrics"):
            self.assertIn(expected, ids)

    def test_it_declares_what_kind_of_claim_it_makes(self) -> None:
        """The guard in `add` requires this, and 'measured' is the honest word: it counted a real
        connection, unlike a row reporting that a setting is set."""
        self.assertEqual("measured", gw._CHECK_SOURCES.get("datanode"))


class TheEndpointSuppliesIt(unittest.TestCase):

    def test_the_overview_route_passes_what_the_probe_found(self) -> None:
        import inspect
        source = inspect.getsource(gw)
        self.assertIn("datanode=datanode_state", source,
                      "the checklist takes a datanode state and the route never passes one, so "
                      "the row can never appear in the served answer")

    def test_it_reuses_the_shared_probe_rather_than_making_its_own(self) -> None:
        """The frame already probes on a slow shared cadence; a second prober would double it."""
        import inspect
        source = inspect.getsource(gw)
        self.assertIn("datanode_state = await _datanode_for_frame(cfg)", source)


if __name__ == "__main__":
    unittest.main()
