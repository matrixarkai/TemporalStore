"""`materialize_serving_record_batch` gives two different answers, and this records which.

The function is defined twice. The two bodies differ by exactly one statement::

    # matrixark_mcp_serving_records
    materialized.extend(context_model_registry_records(records))   # present
    # matrixark_mcp_core_compact
                                                                   # absent

`context_model_registry_records` scans a batch for `context_embedding` rows, reads each one's
`model`, and emits one `context_model_registry` row per distinct model. That row is the only
carrier of `model_hash`, and it also holds `model_ref`, `model_name`, `provider` and
`execution_mode` -- so whether it is emitted decides whether a store records which model produced
its vectors.

Seven modules answer to the name and they do not agree. `matrixark_mcp_core_compact.__all__` lists
it and `matrixark_mcp_core` does `from matrixark_mcp_core_compact import *`, so core republishes
the thin copy to everything that reaches it that way -- which is exactly the hazard the file itself
documents for its NEIGHBOUR, `latest_context_state_key`, made to delegate because "the write path
resolves THIS module ... so a copy that answered differently here was the answer that shipped".
The same argument was never applied to this function.

This test does not pick a winner. Consolidating means a path that writes no registry row today
starts writing one, which changes what lands in an existing store, and that is a decision rather
than a tidy-up. What this test does is stop the split moving without anyone noticing, in either
direction:

* a module that changes sides fails, whichever way it moves;
* a module that stops answering to the name at all fails;
* and the probe itself is checked, so a batch that shapes nothing cannot read as agreement.

Measured by CALLING each resolution on one embedding record, not by reading source. Two names can
share a body and still resolve to different objects, and a source match would not survive either
copy being refactored.
"""

import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

NAME = "materialize_serving_record_batch"

# module -> does its resolution of the name emit a context_model_registry row
#
# Recorded, not derived. Deriving it from the tree would make this test agree with whatever the
# tree happens to do, which is the one thing it must not do.
RECORDED = {
    "matrixark_mcp_serving_records": True,
    "matrixark_mcp_local_adapter": True,
    "matrixark_mcp_server": True,
    "matrixark_mcp_core_compact": False,
    "matrixark_mcp_core": False,
    "matrixark_context_backfill": False,
    "matrixark_temporal_direct_backend": False,
}

# The default backend, from matrixark_mcp_server.default_mcp_backend(). Named so the list above
# cannot quietly become "all False" without this being read again.
DEFAULT_BACKEND_MODULE = "matrixark_temporal_direct_backend"

EMBEDDING_RECORD = {
    "record_type": "context_embedding",
    "model": "intfloat/multilingual-e5-large",
    "ref": "ctx:probe:1",
    "created_at_ms": 1_700_000_000_000,
    "updated_at_ms": 1_700_000_000_000,
    "vector": [0.0, 0.0, 0.0],
}


# Modules that cannot be imported first, and the one to import ahead of them.
#
# `matrixark_temporal_direct_backend` and `matrixark_mcp_temporal_adapters` import each other, so
# importing the backend first raises "cannot import name '_TemporalDirectBackendMixin' from
# partially initialized module". Importing the adapters module first resolves it; importing
# `matrixark_mcp_local_adapter` first does NOT, so this is a specific pairing rather than a general
# warm-up.
#
# Written as an explicit import of the cycle parent rather than a sys.path change or a broad
# try/except. Both of those would work here and both reach further than this file: sys.path is
# process-wide and would change how every other test in the run resolves its imports, and swallowing
# the ImportError would turn "this module cannot be loaded" into "this module does not emit a
# registry row", which is the answer the test is trying to measure.
_IMPORT_FIRST = {
    "matrixark_temporal_direct_backend": "matrixark_mcp_temporal_adapters",
}


def _emits_registry_row(module_name):
    """(resolved?, emitted?) for one module, by calling it on a single embedding record."""
    parent = _IMPORT_FIRST.get(module_name)
    if parent is not None:
        __import__(parent)
    module = __import__(module_name)
    shaper = getattr(module, NAME, None)
    if shaper is None:
        return False, None
    rows = shaper([dict(EMBEDDING_RECORD)])
    kinds = [str(r.get("record_type") or "") for r in rows if isinstance(r, dict)]
    return True, "context_model_registry" in kinds


class TheBatchShaperHasTwoAnswersTest(unittest.TestCase):

    def test_the_probe_actually_shapes_something(self):
        """If the probe record stopped being shaped at all, every module would agree on False."""
        module = __import__("matrixark_mcp_serving_records")
        rows = getattr(module, NAME)([dict(EMBEDDING_RECORD)])
        self.assertGreaterEqual(
            len(rows), 2,
            "the fuller copy shaped %d rows from one embedding record; it should produce the "
            "record and a registry row, so the probe is no longer exercising the difference"
            % len(rows))
        kinds = {str(r.get("record_type") or "") for r in rows if isinstance(r, dict)}
        self.assertIn("context_embedding", kinds)

    def test_every_module_answers_the_way_it_is_recorded(self):
        drifted = []
        for module_name, expected in sorted(RECORDED.items()):
            resolved, emitted = _emits_registry_row(module_name)
            if not resolved:
                drifted.append("    %s no longer answers to %s" % (module_name, NAME))
            elif emitted != expected:
                drifted.append("    %s now emits=%s, recorded emits=%s"
                               % (module_name, emitted, expected))
        self.assertFalse(
            drifted,
            "the split in %s has moved:\n%s\n\nIf this is a deliberate consolidation, say so and "
            "update the table -- and check what it means for stores written by the old shape, "
            "because a path that wrote no registry row will start writing one. If it is not "
            "deliberate, a copy has drifted again." % (NAME, "\n".join(drifted)))

    def test_the_split_is_still_a_split(self):
        """The whole point is disagreement. If it ever becomes unanimous, this file is obsolete
        and should be replaced by an assertion that there is ONE implementation -- not left here
        passing over a question that no longer exists."""
        answers = set()
        for module_name in RECORDED:
            resolved, emitted = _emits_registry_row(module_name)
            if resolved:
                answers.add(emitted)
        self.assertEqual(
            {True, False}, answers,
            "every module now gives the same answer (%s). That is the good outcome, and it makes "
            "this test the wrong shape: replace it with a guard that one implementation serves "
            "every caller." % answers)

    def test_the_default_backend_is_named_and_still_on_the_thin_side(self):
        """Recorded separately because it is the fact that gives the split its weight: the backend
        default_mcp_backend() returns is one of the paths that writes no registry row."""
        self.assertIn(DEFAULT_BACKEND_MODULE, RECORDED)
        self.assertFalse(
            RECORDED[DEFAULT_BACKEND_MODULE],
            "the table now says the default backend emits a registry row; if that is true the "
            "finding this file records has been fixed and the file should say so")
        resolved, emitted = _emits_registry_row(DEFAULT_BACKEND_MODULE)
        self.assertTrue(resolved)
        self.assertFalse(
            emitted,
            "the default backend now emits a registry row -- the divergence is resolved, which is "
            "good news this file is not written to report; rewrite it")


if __name__ == "__main__":
    unittest.main()
