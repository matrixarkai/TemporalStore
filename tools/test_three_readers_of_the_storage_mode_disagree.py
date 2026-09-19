#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Three readers of "the storage mode" answer from different variables.

`MATRIXARK_STORAGE_MODE` has aliases. Three places in production resolve them, and each knows a
different set in a different order with a different default:

    _TemporalDirectBackendMixin._disk_fallback_replay_gate
        MATRIXARK_STORAGE_MODE, MATRIXARK_TEMPORALSTORE_STORAGE_MODE            -> "default"

    _TemporalDirectWriteMixin._async_context_warmup_storage_mode
        MATRIXARK_TEMPORALSTORE_STORAGE_MODE, MATRIXARK_NATIVE_STORAGE_MODE,
        MATRIXARK_STORAGE_MODE, MATRIXARK_BENCHMARK_STORAGE_MODE               -> "local"

    RustProxyDaemon._startup_warmup_allowed
        MATRIXARK_TEMPORALSTORE_MODE, MATRIXARK_TEMPORALSTORE_STORAGE_MODE,
        MATRIXARK_STORAGE_MODE, MATRIXARK_HOOK_STORAGE_ROUTE,
        TEMPORALSTORE_STORAGE_MODE                                    (substring match)

## What that costs, measured rather than reasoned about

The replay gate exists to refuse replaying a local disk fallback when the store is distributed --
its own `skip_reason` says
`distributed_storage_uses_replication_or_shared_store_recovery`. **Three ways of declaring
`shared_store` never reach it**, so it answers `allowed=True` on a store the rest of the system
is treating as distributed:

| environment | warmup | gate mode | gate allowed |
| --- | --- | --- | --- |
| nothing set | local | default | True |
| MATRIXARK_NATIVE_STORAGE_MODE=shared_store | shared_store | **default** | **True** |
| MATRIXARK_BENCHMARK_STORAGE_MODE=shared_store | shared_store | **default** | **True** |
| TEMPORALSTORE_STORAGE_MODE=shared_store | local | **default** | **True** |

And where both of the gate's OWN two names are set, it and the warmup answer OPPOSITELY, because
one puts `MATRIXARK_STORAGE_MODE` first and the other puts `MATRIXARK_TEMPORALSTORE_STORAGE_MODE`
first.

## Recorded rather than fixed

Teaching the gate the other three spellings makes it refuse replay where it currently allows it.
That is the safer answer and probably the right one, but it changes recovery behaviour for any
deployment declaring its mode through one of those names, and "which spelling is canonical" is a
question about the product's configuration surface rather than a cleanup. This repository already
treats that class as a decision -- matrixarkai#1872 and #1875 guarded a diverged pair and filed
the choice separately.

So this asserts the divergence EXACTLY. Narrowing it fails here and somebody confirms it was
meant; widening it fails here too, and the disagreement cannot grow quietly.

Answers come from CALLING the three functions, not from reading their source. An earlier draft
re-implemented the gate's `or` chain inside the test, which would have kept passing after the
real chain changed -- a control has to read the same thing its subject reads.
"""
from __future__ import annotations

import os
import sys
import types
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

# The parent adapter first: these three sit in an import cycle with it, and production loads the
# adapter first. Importing a child first gets a half-initialised module.
import matrixark_mcp_temporal_adapters  # noqa: E402,F401
import matrixark_rust_proxy_daemon as daemon_module  # noqa: E402
import matrixark_temporal_direct_backend as backend_module  # noqa: E402
import matrixark_temporal_direct_write as write_module  # noqa: E402

#: Every variable any of the three consults, cleared between scenarios so one test cannot leak
#: into the next and so the developer's own shell cannot decide the answer.
TOUCHED = (
    "MATRIXARK_STORAGE_MODE", "MATRIXARK_TEMPORALSTORE_STORAGE_MODE",
    "MATRIXARK_NATIVE_STORAGE_MODE", "MATRIXARK_BENCHMARK_STORAGE_MODE",
    "TEMPORALSTORE_STORAGE_MODE", "MATRIXARK_TEMPORALSTORE_MODE",
    "MATRIXARK_HOOK_STORAGE_ROUTE", "MATRIXARK_STORAGE_FAMILY",
    "MATRIXARK_TEMPORALSTORE_STORAGE_FAMILY", "MATRIXARK_REPLICATION_MODE",
    "MATRIXARK_TEMPORALSTORE_REPLICATION_MODE",
    "MATRIXARK_TEMPORALSTORE_RECOVER_LOCAL_STORE_ANY_MODE",
    "MATRIXARK_TEMPORALSTORE_METASERVER", "TEMPORALSTORE_METASERVER",
)

#: scenario -> (warmup mode, gate's storage_mode, gate allows local replay)
#: Measured on main. Each row that says `shared_store` beside `default` is a spelling the gate
#: cannot see.
RECORDED = {
    (): ("local", "default", True),
    (("MATRIXARK_NATIVE_STORAGE_MODE", "shared_store"),): ("shared_store", "default", True),
    (("MATRIXARK_BENCHMARK_STORAGE_MODE", "shared_store"),): ("shared_store", "default", True),
    (("TEMPORALSTORE_STORAGE_MODE", "shared_store"),): ("local", "default", True),
    (("MATRIXARK_STORAGE_MODE", "local"),
     ("MATRIXARK_TEMPORALSTORE_STORAGE_MODE", "shared_store")): ("shared_store", "local", True),
}


def _warmup_mode() -> str:
    return write_module._TemporalDirectWriteMixin._async_context_warmup_storage_mode(
        types.SimpleNamespace())


def _gate() -> dict:
    return backend_module._TemporalDirectBackendMixin._disk_fallback_replay_gate(
        types.SimpleNamespace())


class ThreeReadersOfTheStorageModeDisagreeTest(unittest.TestCase):

    def setUp(self) -> None:
        self._saved = {name: os.environ.get(name) for name in TOUCHED}
        for name in TOUCHED:
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        for name, value in self._saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    def _apply(self, scenario) -> None:
        for name, value in scenario:
            os.environ[name] = value

    def test_each_recorded_scenario_still_answers_the_way_it_did(self) -> None:
        for scenario, expected in RECORDED.items():
            with self.subTest(scenario=scenario or "nothing set"):
                for name in TOUCHED:
                    os.environ.pop(name, None)
                self._apply(scenario)
                gate = _gate()
                actual = (_warmup_mode(), gate["storage_mode"], gate["allowed"])
                self.assertEqual(
                    expected, actual,
                    "the three storage-mode readers no longer answer as recorded. If they now "
                    "AGREE, the divergence was resolved and this record should go with it; if "
                    "they disagree differently, a fourth spelling or a new precedence has "
                    "appeared.")

    def test_the_gate_cannot_see_three_spellings_of_shared_store(self) -> None:
        """The consequence, stated as its own check so it cannot be lost in a table.

        The gate refuses replay on distributed storage. These three say `shared_store` and it
        answers `default` and allows replay anyway.
        """
        for name in ("MATRIXARK_NATIVE_STORAGE_MODE", "MATRIXARK_BENCHMARK_STORAGE_MODE",
                     "TEMPORALSTORE_STORAGE_MODE"):
            with self.subTest(spelling=name):
                for other in TOUCHED:
                    os.environ.pop(other, None)
                os.environ[name] = "shared_store"
                gate = _gate()
                self.assertEqual(
                    "default", gate["storage_mode"],
                    "%s now reaches the replay gate. If that was intended, this record and the "
                    "table above should shrink with it." % name)
                self.assertTrue(
                    gate["allowed"],
                    "%s now makes the gate refuse replay -- the behaviour change this file was "
                    "filed to get a decision on may have landed." % name)

    def test_the_two_the_gate_does_know_are_ordered_oppositely_to_the_warmup(self) -> None:
        """With both set, the two readers answer differently. That is the whole finding in one row."""
        for other in TOUCHED:
            os.environ.pop(other, None)
        os.environ["MATRIXARK_STORAGE_MODE"] = "local"
        os.environ["MATRIXARK_TEMPORALSTORE_STORAGE_MODE"] = "shared_store"
        self.assertEqual("local", _gate()["storage_mode"])
        self.assertEqual("shared_store", _warmup_mode())

    def test_the_probe_actually_moves_something(self) -> None:
        """Vacuity floor. If every scenario produced one answer, the table would assert nothing."""
        answers = set()
        for scenario in RECORDED:
            for name in TOUCHED:
                os.environ.pop(name, None)
            self._apply(scenario)
            answers.add((_warmup_mode(), _gate()["storage_mode"]))
        self.assertGreater(
            len(answers), 2,
            "the scenarios produced %d distinct answers; they are no longer exercising the "
            "difference between these readers" % len(answers))


if __name__ == "__main__":
    unittest.main()
