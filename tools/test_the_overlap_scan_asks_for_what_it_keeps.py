"""The overlap scan asks for the two record types it keeps, and answers the same either way.

`session_commit` builds its overlap set from two loops that keep exactly `context_batch_commit` and
`context_event` and skip everything else. This path used to read the WHOLE store to find them. The
sibling implementation in `matrixark_local_adapter_session_commit` stopped doing that, and recorded
why on `_commit_records_of_types`: 13 of 20 active samples on a 250-memory store fell inside that
read and its compaction, 893 ms per call with the proxy idle, growing with the store.

The copy tested HERE is not the one a request reaches: `matrixark_mcp_session_runtime` is on the
recorded unreachable-from-production list in `test_a_module_only_tests_reach_is_not_live`. The
commit path that runs is the sibling mixin, and `test_the_live_commit_path_asks_for_what_it_keeps`
is what pins its two reads. These tests stay because the module does, and because the reasoning
below is the reasoning that path needs.

These tests pin the three things that make narrowing the read safe:

* an adapter WITH the type scan and one WITHOUT produce the same overlap records;
* an adapter whose scan returns ``None`` -- "could not ask", not "nothing of these types" -- falls
  back to the full read rather than silently returning an empty overlap set;
* the types asked for are exactly the types the loops keep, so the read cannot narrow past the
  filters.

The last one is the failure that would be silent: ask for one type fewer and the overlap set quietly
loses rows, with no error anywhere.
"""

import pathlib
import sys
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

import matrixark_mcp_session_runtime as runtime


def _store():
    """A store holding both wanted types and several the loops must ignore."""
    return [
        {"record_type": "context_batch_commit", "scope": {"session_id": "s1"},
         "source_event_ids": [1, 2]},
        {"record_type": "context_event", "event_id_hash": 1},
        {"record_type": "context_event", "event_id_hash": 2},
        {"record_type": "context_event", "event_id_hash": 3},
        {"record_type": "context_entity", "entity_hash": 9},
        {"record_type": "context_summary", "summary_hash": 8},
        {"record_type": "matrixark_async_pipeline_task", "task_hash": 7},
        {"record_type": "context_embedding", "model": "m"},
    ]


class _FullReadOnly:
    """Something with no type scan at all.

    NOT the plain local adapter, though this said so and so did the docstring on the function under
    test: `MatrixArkLocalAdapter` inherits `_commit_records_of_types` from the sibling commit mixin,
    as do all three backend adapters -- six of six classes in tools/ that answer `session_commit`
    have it. What the local adapter lacks is `_scan_records_of_types`, one level further down.
    """

    def __init__(self, records):
        self._records = records
        self.read_all_calls = 0

    def read_all(self):
        self.read_all_calls += 1
        return list(self._records)


class _WithScan(_FullReadOnly):
    """The TemporalStore-backed adapter: answers by type."""

    def __init__(self, records, answer=True):
        super().__init__(records)
        self._answer = answer
        self.scanned_for = None

    def _commit_records_of_types(self, wanted):
        self.scanned_for = list(wanted)
        if not self._answer:
            return None  # could not ask
        return [r for r in self._records if r.get("record_type") in set(wanted)]


class TheOverlapScanAsksForWhatItKeepsTest(unittest.TestCase):

    def test_both_adapters_produce_the_same_records(self):
        plain, scanning = _FullReadOnly(_store()), _WithScan(_store())
        from_full = runtime._overlap_records(plain)
        from_scan = runtime._overlap_records(scanning)

        def kept(records):
            """What the loops in session_commit actually keep."""
            return [r for r in records
                    if r.get("record_type") in {"context_batch_commit", "context_event"}]

        self.assertEqual(kept(from_full), kept(from_scan),
                         "the narrower read changed which records the overlap loops see")
        self.assertGreater(len(kept(from_full)), 2,
                           "the fixture holds too few wanted records to prove anything")

    def test_the_scan_is_asked_for_exactly_what_the_loops_keep(self):
        """Asking for fewer types than the loops consume loses overlap rows with no error."""
        scanning = _WithScan(_store())
        runtime._overlap_records(scanning)
        self.assertEqual(["context_batch_commit", "context_event"], scanning.scanned_for)

    def test_a_scan_that_cannot_answer_falls_back_to_the_full_read(self):
        """`None` means "could not ask". Treating it as "nothing of these types" would empty the
        overlap set silently, which is the failure that looks like a clean result."""
        scanning = _WithScan(_store(), answer=False)
        records = runtime._overlap_records(scanning)
        self.assertEqual(1, scanning.read_all_calls,
                         "a scan returning None must fall back to the full read")
        self.assertEqual(len(_store()), len(records))

    def test_an_adapter_without_the_scan_still_works(self):
        plain = _FullReadOnly(_store())
        records = runtime._overlap_records(plain)
        self.assertEqual(1, plain.read_all_calls)
        self.assertEqual(len(_store()), len(records))

    def test_the_scanning_adapter_does_not_read_the_whole_store(self):
        """The point of the change. If this stops holding, the narrowing has been undone."""
        scanning = _WithScan(_store())
        runtime._overlap_records(scanning)
        self.assertEqual(0, scanning.read_all_calls,
                         "the scanning adapter read the whole store anyway")


if __name__ == "__main__":
    unittest.main()
