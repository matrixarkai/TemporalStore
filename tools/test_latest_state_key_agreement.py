# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Every module must agree on what carries a latest-state identity.

`latest_context_state_key` decides whether a record collapses to one row in the latest-state
hash or keeps appending rows to the log forever. It existed in two modules, and they drifted:
one gave `matrixark_async_pipeline_task` an identity and the other did not. The WRITE path
resolves its name through `import *`, so it took the copy WITHOUT the identity while the reader
took the copy with it -- the feature's two halves disagreed at runtime, tasks accumulated in the
append log, and the scan that reads them grew with the corpus.

Nothing failed. Both copies were self-consistent, every suite passed, and the only symptom was a
read that got slower as the store grew. So the test is agreement itself.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

MODULES = (
    # The reader's copy and the copy the write path resolves -- these two ARE the drift.
    # The heavier write modules cannot be imported standalone here: they rely on the
    # application's own import order to resolve a cycle, and fail the same way on pristine main.
    "matrixark_mcp_serving_records",
    "matrixark_mcp_core_compact",
    "matrixark_mcp_latest_context_state",
)

# One per record type that carries an identity, plus one that must not.
SAMPLES = (
    {"record_type": "matrixark_async_pipeline_task", "task_hash": 123},  # must stay None
    {"record_type": "context_summary", "summary_type": "l0", "summary_hash": 7},
    {"record_type": "context_model_registry", "model_kind": "embedding", "model_ref": "m"},
    {"record_type": "context_embedding", "embedding_type": "node_l0", "ref_type": "node", "ref_hash": 9},
    {"record_type": "context_event", "event_id_hash": 5},
)


def _resolved(module_name):
    module = __import__(module_name)
    return getattr(module, "latest_context_state_key", None)


class LatestStateKeyAgreementCase(unittest.TestCase):
    def test_only_one_module_implements_it(self):
        """core_compact must delegate, not carry a second body that can drift.

        A delegating wrapper is fine. A second real implementation is not -- that is what
        shipped, and because both copies were self-consistent nothing failed; only the write
        path silently took the one that gave pipeline tasks no identity.
        """
        import inspect

        bodies = {}
        for name in MODULES:
            fn = _resolved(name)
            if fn is None:
                continue
            try:
                src = inspect.getsource(fn)
            except (OSError, TypeError):
                continue
            # A real implementation branches on record_type; a delegate just calls through.
            if "record_type ==" in src:
                bodies[fn.__module__] = len(src)  # a re-export is the same object, not a copy
        self.assertEqual(
            ["matrixark_mcp_serving_records"], sorted(bodies),
            "more than one module carries a real body for latest_context_state_key: %r" % (bodies,),
        )

    def test_every_module_agrees_on_every_sample(self):
        for sample in SAMPLES:
            keys = {}
            for name in MODULES:
                fn = _resolved(name)
                if fn is None:
                    continue
                keys[name] = fn(dict(sample))
            distinct = {repr(v) for v in keys.values()}
            self.assertEqual(
                1, len(distinct),
                "modules disagree for %s: %r" % (sample["record_type"], keys),
            )

    def test_an_async_pipeline_task_stays_in_the_append_log(self):
        """A pipeline task must NOT get a latest-state identity, and this is measured, not taste.

        Collapsing each task to one row looks free -- the drain already folds them by task_hash.
        But the latest-state hash is read WHOLESALE by `_load_latest_context_state_records()` on
        every idle-commit check and folded by other readers besides, so it is only cheap while
        its identity count stays small. Tasks are per event, so that count grows with the corpus.
        Measured both ways on a 600-add store: giving them an identity cut the per-call task
        count 545.8 -> 310.8 and made an add 143.2 -> 265.6 ms, against two control arms 7%
        apart. This test exists so the expensive direction is not re-applied as an optimisation.
        """
        for name in MODULES:
            fn = _resolved(name)
            if fn is None:
                continue
            self.assertIsNone(
                fn({"record_type": "matrixark_async_pipeline_task", "task_hash": 42}),
                "%s gives a pipeline task a latest-state identity; that was measured at ~2x "
                "the add latency because the latest-state hash is scanned whole" % name,
            )

    def test_a_plain_event_has_no_identity(self):
        """The guard in the other direction: an event must keep its append-log row."""
        for name in MODULES:
            fn = _resolved(name)
            if fn is None:
                continue
            self.assertIsNone(fn({"record_type": "context_event", "event_id_hash": 5}), name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
