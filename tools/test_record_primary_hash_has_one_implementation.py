"""`record_primary_hash` has one implementation, and the method that used to copy it delegates.

`matrixark_mcp_direct_cache.record_primary_hash` existed with no callers, not in any `__all__` and
not re-exported, while `_TemporalDirectReadMixin._record_primary_hash` carried the same body and did
the work. A helper extracted and never adopted -- the pattern this tree has recorded before.

The field order in that body is a record's identity PRECEDENCE: which of `event_id_hash`,
`entity_hash`, `segment_hash` and the rest is consulted first decides which hash identifies a
record. Two copies of that order are two chances to disagree about identity, and a disagreement
would not raise -- it would file a record under a different hash.

Asserted by ANSWER rather than by object identity, because the method is a thin wrapper and will
never be the same object as the function. Two names resolving is only half the promise; if they
resolved to two bodies the duplication would be back, so the test compares what they RETURN across
the precedence cases.
"""

import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

# matrixark_temporal_direct_read and matrixark_mcp_temporal_adapters import each other, so importing
# the reader first raises from a partially initialized module. Import the cycle parent first -- not
# a sys.path change, which is process-wide and would alter how other tests resolve their imports.
import matrixark_mcp_temporal_adapters  # noqa: F401
import matrixark_mcp_direct_cache
import matrixark_temporal_direct_read

FIELDS = (
    "event_id_hash", "entity_hash", "segment_hash", "compression_id_hash", "summary_hash",
    "chunk_hash", "section_hash", "skill_hash", "resource_hash", "batch_id_hash", "ref_hash",
)


def _probes():
    out = [{}, {"ref": "x"}, {"record_type": "context_event"}]
    for field in FIELDS:
        out.extend([{field: 1234}, {field: "abc"}, {field: None}, {field: 0}])
    # adjacent pairs, so PRECEDENCE is exercised and not merely presence
    for i in range(len(FIELDS)):
        for j in range(i + 1, min(i + 4, len(FIELDS))):
            out.append({FIELDS[i]: 11, FIELDS[j]: 22})
    return out


class _Fake:
    """The method reads nothing off self, which is the reason it could be a free function."""


class RecordPrimaryHashHasOneImplementationTest(unittest.TestCase):

    def test_the_probe_set_is_wide_enough_to_mean_something(self):
        probes = _probes()
        self.assertGreater(len(probes), 50,
                           "only %d probes; the comparison below is too thin" % len(probes))
        # a probe set of only empty dicts would agree for the wrong reason
        self.assertGreater(len({tuple(sorted(p)) for p in probes}), 10)

    def test_both_names_answer_identically(self):
        free = matrixark_mcp_direct_cache.record_primary_hash
        method = matrixark_temporal_direct_read._TemporalDirectReadMixin._record_primary_hash
        fake = _Fake()
        disagreements = []
        for probe in _probes():
            try:
                a = free(dict(probe))
            except Exception as exc:
                a = "raised %s" % type(exc).__name__
            try:
                b = method(fake, dict(probe))
            except Exception as exc:
                b = "raised %s" % type(exc).__name__
            if a != b:
                disagreements.append("%r -> free=%r method=%r" % (probe, a, b))
        self.assertFalse(
            disagreements,
            "the two answer differently, so the method is carrying its own copy again:\n   "
            + "\n   ".join(disagreements[:8])
            + "\n\nThe field order is a record's identity precedence -- a disagreement here files "
              "a record under a different hash rather than raising.")

    def test_the_method_does_not_carry_the_field_list_again(self):
        """A body that lists the fields is a body that can drift. Delegation is the point."""
        import inspect
        source = inspect.getsource(
            matrixark_temporal_direct_read._TemporalDirectReadMixin._record_primary_hash)
        code = "\n".join(line for line in source.splitlines()
                         if not line.strip().startswith("#"))
        # strip the docstring, which legitimately names fields while explaining the delegation
        parts = code.split('"""')
        code = parts[0] + ("".join(parts[2:]) if len(parts) > 2 else "")
        named = [f for f in FIELDS if f in code]
        self.assertEqual(
            [], named,
            "the method names %s in its own body again -- it should call "
            "matrixark_mcp_direct_cache.record_primary_hash, not restate the precedence" % named)


if __name__ == "__main__":
    unittest.main()
