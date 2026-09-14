# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The commit path that RUNS narrows its two reads, and the narrowing loses nothing.

`session_commit` reads the store twice and keeps a couple of record types out of each read: once
for the finalize branch (`context_batch_commit` + `context_session_boundary`) and once for the
overlap branch (`context_batch_commit` + `context_event`). Both asked the store for EVERYTHING and
then threw almost all of it away, at a measured 893 ms per call on a 250-memory store with the
proxy idle -- 13 of 20 active samples inside that read and its compaction, growing with the store.
Both were narrowed to `_commit_records_of_types`.

There are two copies of `session_commit`. This one, on `_LocalAdapterSessionCommitMixin`, is the
one production reaches: `MatrixArkLocalAdapter` mixes it in and every backend adapter subclasses
that. The other, `matrixark_mcp_session_runtime.session_commit`, is on the recorded
unreachable-from-production list in `test_a_module_only_tests_reach_is_not_live.py` -- and it is
the copy that had a guard. `test_the_overlap_scan_asks_for_what_it_keeps` pins ONE of its two
reads; nothing pinned either read on this one. Reverting `_commit_records_of_types` here to
`self.read_all()`, and separately dropping a type from each of the two lists, was run against the
whole python suite: neither mutation failed a single test.

The three properties below are what make narrowing a read safe, and each is a failure that looks
like success if it stops holding:

* the narrowed read and the full read produce the SAME rows, tombstones included -- the scan is
  handed the tombstone type as well and applies it to the subset, so a commit belonging to a
  forgotten session cannot come back from the dead;
* a scan that CANNOT answer (`None`, not `[]`) falls back to the full read -- reading `None` as
  "nothing of these types" would empty the finalize and overlap sets silently;
* the types asked for are exactly the types the loops keep -- ask for one fewer and rows are lost
  with no error anywhere, which is why this runs `session_commit` itself and compares its output
  rather than comparing the two type lists to a copy of themselves.

Also pinned: `MatrixArkLocalAdapter` HAS `_commit_records_of_types`. It is the plain local adapter
and it inherits the method from this very mixin; what it does not have is `_scan_records_of_types`,
which is why `_commit_records_of_types` falls back to the full read from the inside. The sibling
copy's docstring says the plain local adapter does not have `_commit_records_of_types` -- counted
by importing tools/ and keeping the classes that answer `session_commit` rather than by assuming
the hierarchy, six of six have it and three of those six have `_scan_records_of_types`.
"""

import json
import pathlib
import sys
import unittest
from unittest import mock

TOOLS = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))

import matrixark_mcp_temporal_adapters  # noqa: F401  (imported first: it and the backend cycle)
import matrixark_mcp_local_adapter as local_adapter
import matrixark_local_adapter_session_commit as session_commit_module
from matrixark_local_adapter_session_commit import _LocalAdapterSessionCommitMixin

#: `session_commit` stamps records with `now_ms()` and hashes it into the commit id, so two runs a
#: millisecond apart differ in fields that have nothing to do with which records were read. Frozen
#: so that "the two adapters wrote the same thing" is a claim about the READ.
FROZEN_MS = 1_700_000_000_000

MEMORY_TOMBSTONE_RECORD_TYPE = local_adapter.MEMORY_TOMBSTONE_RECORD_TYPE

SCOPE = {"user_id": "u1", "session_id": "s1"}
FINALIZE_TYPES = ["context_batch_commit", "context_session_boundary"]
OVERLAP_TYPES = ["context_batch_commit", "context_event"]
#: The third narrowed read: what skill discovery dedupes against. Read from the module rather than
#: copied, so a type added there without a fixture row here fails the "more than one row" floor
#: below instead of passing on a list that agrees with itself.
SKILL_TYPES = list(session_commit_module.SKILL_DISCOVERY_RECORD_TYPES)

#: Record types the commit loops must ignore. Present so that "the narrowed read equals the full
#: read" is a claim about a store that holds more than the wanted types.
_NOISE_TYPES = (
    "context_entity",
    "context_summary",
    "context_segment",
    "context_embedding",
    "context_node",
    "context_index",
    "session_buffer_event",
    "matrixark_async_pipeline_task",
)


def _event(event_id, text):
    return {
        "record_type": "context_event",
        "event_id_hash": event_id,
        "node_hash": 1,
        "scope": dict(SCOPE),
        "envelope": {"scope": dict(SCOPE), "messages": [{"role": "user", "content": text}]},
        "updated_at_ms": 1000 + event_id,
    }


def _raw_store(with_tombstone=False):
    """The append log, in append order, BEFORE the serving pipeline runs over it."""
    records = [
        _event(1, "one"),
        _event(2, "two"),
        _event(3, "three"),
        {"record_type": "context_batch_commit", "scope": dict(SCOPE),
         "source_event_ids": [1, 2], "commit_id_hash": 11, "updated_at_ms": 2000},
        {"record_type": "context_batch_commit", "scope": dict(SCOPE),
         "source_event_ids": [3], "commit_id_hash": 12, "updated_at_ms": 2001},
        {"record_type": "context_session_boundary", "scope": dict(SCOPE),
         "boundary_hash": 21, "final_session_boundary": False, "updated_at_ms": 2002},
    ]
    for position, record_type in enumerate(_NOISE_TYPES):
        records.append({
            "record_type": record_type, "scope": dict(SCOPE), "node_hash": 1,
            "ref_type": "event", "ref_hash": 1, "updated_at_ms": 3000 + position,
        })
    if with_tombstone:
        records.append({
            "record_type": MEMORY_TOMBSTONE_RECORD_TYPE,
            "tombstone_kind": "delete", "target_memory_id": "2",
            "scope": dict(SCOPE), "updated_at_ms": 4000,
        })
    return records


def _raw_store_with_skills():
    """`_raw_store` plus the rows skill discovery dedupes against, two of each so that "the
    narrowed read equals the full read" is a claim about more than a single row."""
    records = _raw_store()
    for position, record_type in enumerate(SKILL_TYPES):
        for copy in range(2):
            records.append({
                "record_type": record_type,
                "scope": dict(SCOPE),
                "node_hash": 1,
                "skill_hash": 900 + position * 10 + copy,
                "skill_name": f"{record_type}-{copy}",
                "updated_at_ms": 5000 + position * 10 + copy,
            })
    return records


#: A repeated tool procedure: three episodes with the same four-step signature, which is what
#: `discover_skills` needs before it will call anything a skill (min_support 2, min_steps 2). Four
#: DISTINCT tools on purpose -- repeats of one tool collapse to a one-step signature and discover
#: nothing, which would make every comparison below agree for free.
_SKILL_EPISODE_STEPS = """[tool:git] pull
[tool:make] build
[tool:deploy] push
[tool:curl] /healthz"""
SKILL_DISCOVERY_MESSAGES = [
    message
    for round_number in (1, 2, 3)
    for message in (
        {"role": "user", "content": f"deploy the service and verify it, round {round_number}"},
        {"role": "assistant", "content": _SKILL_EPISODE_STEPS},
    )
]


def _serving_view(raw):
    """What `read_all()` returns: the serving pipeline over the whole raw log."""
    return local_adapter.filter_live_memory_records(
        local_adapter.compact_and_apply_tombstones(list(raw))
    )


class _Plain(_LocalAdapterSessionCommitMixin):
    """`MatrixArkLocalAdapter`'s shape: the commit scan from this mixin, no backend type scan."""

    def __init__(self, raw):
        self.raw = list(raw)
        self.read_all_calls = 0
        self.appended = []
        self.batch_extract_args = []

    # -- store -------------------------------------------------------------------------------
    def read_all(self):
        self.read_all_calls += 1
        return _serving_view(self.raw)

    def append(self, record):
        self.appended.append(record)

    def append_many(self, records):
        self.appended.extend(records)

    # -- the rest of what session_commit calls on self ----------------------------------------
    def pending_session_events(self, scope):
        return list(self.pending)

    pending = ()

    def surviving_ids_for_pending_events(self, records):
        return None

    def default_session_node_path(self, scope):
        return ["user", "u1", "session", "s1"]

    def node_summary_dirty_records(self, **kwargs):
        return [], []

    def batch_extract(self, args, *, hook=None):
        self.batch_extract_args.append(args)
        return {"events_written": len(args.get("messages") or [])}


class _Scanning(_Plain):
    """A backend adapter's shape: answers by record type without reading the store."""

    def __init__(self, raw, answer=True):
        super().__init__(raw)
        self.answer = answer
        self.scanned_for = []

    def _scan_records_of_types(self, wanted):
        self.scanned_for.append(list(wanted))
        if not self.answer:
            return None  # could not ask -- NOT "nothing of these types"
        wanted_set = set(wanted)
        return [r for r in self.raw if r.get("record_type") in wanted_set]


class _WithPending(_Plain):
    """Pending session events, so the overlap branch runs instead of the finalize branch."""

    pending = ()


def _commit(adapter, args):
    """One commit with the clock frozen (see FROZEN_MS)."""
    with mock.patch.object(session_commit_module, "now_ms", lambda: FROZEN_MS):
        return adapter.session_commit(dict(args))


def _finalize_result(adapter):
    return _commit(adapter, {"scope": dict(SCOPE), "force": True})


class TheLiveCommitPathAsksForWhatItKeepsTest(unittest.TestCase):
    maxDiff = None

    # -- the read itself ------------------------------------------------------------------------

    def _discover(self, adapter):
        """One skill-discovery pass with the flag on and the clock frozen (see FROZEN_MS)."""
        flag_on = mock.patch.object(session_commit_module, "SKILL_DISCOVERY_ENABLED", True)
        frozen_clock = mock.patch.object(session_commit_module, "now_ms", lambda: FROZEN_MS)
        with flag_on, frozen_clock:
            result = adapter._maybe_discover_skills(
                dict(SCOPE), list(SKILL_DISCOVERY_MESSAGES), final_session_boundary=True
            )
        written = [json.dumps(r, sort_keys=True, default=str) for r in adapter.appended]
        return result, written

    def _assert_same_rows(self, wanted, with_tombstone):
        raw = _raw_store(with_tombstone=with_tombstone)
        narrowed = _Scanning(raw)._commit_records_of_types(list(wanted))
        full = [r for r in _serving_view(raw) if r.get("record_type") in set(wanted)]
        kept = [r for r in narrowed if r.get("record_type") in set(wanted)]
        self.assertGreater(len(full), 1, "the fixture holds too few wanted records to prove anything")
        self.assertEqual(full, kept, f"the narrowed read changed the rows for {wanted}")

    def test_the_finalize_read_returns_the_same_rows_either_way(self):
        self._assert_same_rows(FINALIZE_TYPES, with_tombstone=False)

    def test_the_overlap_read_returns_the_same_rows_either_way(self):
        self._assert_same_rows(OVERLAP_TYPES, with_tombstone=False)

    def test_a_tombstone_is_applied_to_the_narrowed_read_too(self):
        """The reason this is not a one-line swap: the scan returns RAW rows, so a commit or event
        belonging to a forgotten memory would come back from the dead without this."""
        raw = _raw_store(with_tombstone=True)
        live_events = [r for r in _serving_view(raw) if r.get("record_type") == "context_event"]
        self.assertEqual([1, 3], [r["event_id_hash"] for r in live_events],
                         "the fixture's tombstone did not remove anything, so nothing is proven")
        self._assert_same_rows(OVERLAP_TYPES, with_tombstone=True)
        self._assert_same_rows(FINALIZE_TYPES, with_tombstone=True)

    def test_the_scan_is_asked_for_the_tombstones_as_well(self):
        scanning = _Scanning(_raw_store())
        scanning._commit_records_of_types(list(OVERLAP_TYPES))
        self.assertEqual([OVERLAP_TYPES + [MEMORY_TOMBSTONE_RECORD_TYPE]], scanning.scanned_for)

    def test_a_scan_that_cannot_answer_falls_back_to_the_full_read(self):
        """`None` means "could not ask". Reading it as "nothing of these types" would empty the
        finalize and overlap sets silently -- the failure that looks like a clean result."""
        scanning = _Scanning(_raw_store(), answer=False)
        records = scanning._commit_records_of_types(list(OVERLAP_TYPES))
        self.assertEqual(1, scanning.read_all_calls)
        self.assertEqual(len(_serving_view(scanning.raw)), len(records))

    def test_an_adapter_without_the_backend_scan_still_works(self):
        plain = _Plain(_raw_store())
        records = plain._commit_records_of_types(list(OVERLAP_TYPES))
        self.assertEqual(1, plain.read_all_calls)
        self.assertEqual(len(_serving_view(plain.raw)), len(records))

    def test_the_skill_discovery_read_returns_the_same_rows_either_way(self):
        raw = _raw_store_with_skills()
        narrowed = _Scanning(raw)._commit_records_of_types(list(SKILL_TYPES))
        full = [r for r in _serving_view(raw) if r.get("record_type") in set(SKILL_TYPES)]
        kept = [r for r in narrowed if r.get("record_type") in set(SKILL_TYPES)]
        self.assertGreater(len(full), 1, "the fixture holds too few skill rows to prove anything")
        self.assertEqual(full, kept, "the narrowed read changed the rows skill discovery dedupes against")

    def test_a_refused_scan_does_not_hand_skill_discovery_an_empty_store(self):
        """The failure this read cannot be allowed to have. These rows are what discovery dedupes
        AGAINST, so an empty answer that was really a refusal reads as "this user has no skills
        yet" and every skill is captured again -- a write, not a slow read. `None` must reach the
        full read, and the rows must come back."""
        scanning = _Scanning(_raw_store_with_skills(), answer=False)
        records = scanning._commit_records_of_types(list(SKILL_TYPES))
        kept = [r for r in records if r.get("record_type") in set(SKILL_TYPES)]
        self.assertEqual(1, scanning.read_all_calls)
        self.assertEqual(
            [r for r in _serving_view(scanning.raw) if r.get("record_type") in set(SKILL_TYPES)],
            kept,
        )

    def test_the_skill_scan_is_asked_for_exactly_the_types_the_loop_keeps(self):
        """Ask for one fewer and rows are lost with no error anywhere. Compared against the
        module's own tuple, which is the single list the call site also spells."""
        scanning = _Scanning(_raw_store_with_skills())
        scanning._commit_records_of_types(list(SKILL_TYPES))
        self.assertEqual([SKILL_TYPES + [MEMORY_TOMBSTONE_RECORD_TYPE]], scanning.scanned_for)
        self.assertEqual(("skill_manifest", "skill_registry"),
                         session_commit_module.SKILL_DISCOVERY_RECORD_TYPES)

    def test_skill_discovery_reads_only_what_it_keeps(self):
        """The guard for the CALL SITE, not the helper. The three above pin
        `_commit_records_of_types`; reverting `_maybe_discover_skills` to `self.read_all()` leaves
        every one of them green, because the helper is still correct -- it is simply no longer the
        thing being called. This is the one that fails on that revert."""
        scanning = _Scanning(_raw_store_with_skills())
        with mock.patch.object(session_commit_module, "SKILL_DISCOVERY_ENABLED", True):
            scanning._maybe_discover_skills(
                dict(SCOPE),
                [{"role": "user", "content": "run the migration then check the log"}],
                final_session_boundary=True,
            )
        self.assertEqual(0, scanning.read_all_calls, "skill discovery still read the whole store")
        self.assertIn(SKILL_TYPES + [MEMORY_TOMBSTONE_RECORD_TYPE], scanning.scanned_for)

    def test_skill_discovery_does_not_read_at_all_when_it_is_switched_off(self):
        """Denominator for the test above: with the flag off there is no read to narrow, so a zero
        there would mean nothing on its own."""
        scanning = _Scanning(_raw_store_with_skills())
        with mock.patch.object(session_commit_module, "SKILL_DISCOVERY_ENABLED", False):
            result = scanning._maybe_discover_skills(
                dict(SCOPE),
                [{"role": "user", "content": "run the migration then check the log"}],
                final_session_boundary=True,
            )
        self.assertIsNone(result)
        self.assertEqual(0, scanning.read_all_calls)
        self.assertEqual([], scanning.scanned_for)

    def test_the_narrowed_read_still_dedupes_against_skills_already_in_the_store(self):
        """The failure mode that kept this read wide. Discovery DEDUPES against these rows, so a
        narrowed read that came back short would look like "no skills yet" and capture every one of
        them a second time -- a write, not a slow read. Run twice, with the first run's rows put
        back into the store: the second run must capture nothing, on both adapters."""
        by_adapter = {}
        for adapter in (_Plain(_raw_store_with_skills()), _Scanning(_raw_store_with_skills())):
            name = type(adapter).__name__
            with self.subTest(adapter=name):
                first, first_writes = self._discover(adapter)
                by_adapter[name] = (first, first_writes)
                # Denominator: two runs that discovered nothing would agree for free.
                self.assertEqual(1, first["captured"], "the fixture discovered nothing")
                self.assertGreater(len(first_writes), 1, "nothing was written")
                adapter.raw.extend(adapter.appended)
                adapter.appended = []
                second, second_writes = self._discover(adapter)
                self.assertEqual(0, second["captured"], "a skill already in the store was captured again")
                self.assertEqual("all_dup_of_local_skills", second["reason"])
                self.assertEqual([], second_writes)
        # The clock is frozen, so the two adapters' captured rows are comparable byte for byte:
        # the narrowed read wrote exactly what the full read wrote.
        self.assertEqual(by_adapter["_Plain"], by_adapter["_Scanning"])

    def test_the_plain_local_adapter_has_the_commit_scan_and_not_the_backend_one(self):
        """Measured, not assumed. `MatrixArkLocalAdapter` inherits `_commit_records_of_types` from
        this mixin -- what it lacks is `_scan_records_of_types`, which is the absence the fallback
        inside `_commit_records_of_types` is there for."""
        self.assertTrue(hasattr(local_adapter.MatrixArkLocalAdapter, "_commit_records_of_types"))
        self.assertFalse(hasattr(local_adapter.MatrixArkLocalAdapter, "_scan_records_of_types"))

    # -- session_commit itself, end to end ------------------------------------------------------

    def test_the_finalize_branch_reads_only_what_it_keeps(self):
        """The point of the change: a backend that can answer by type does not read the store."""
        scanning = _Scanning(_raw_store())
        result = _finalize_result(scanning)
        self.assertEqual("finalized", result.get("status"),
                         "the fixture did not reach the finalize branch, so nothing is measured")
        self.assertEqual(0, scanning.read_all_calls, "the finalize branch read the whole store")
        self.assertEqual([FINALIZE_TYPES + [MEMORY_TOMBSTONE_RECORD_TYPE]], scanning.scanned_for)

    def test_the_finalize_branch_writes_the_same_records_either_way(self):
        """Ask for one type fewer and the finalize branch loses rows with no error. Comparing the
        two type lists could not catch that; comparing what the commit WRITES can."""
        plain, scanning = _Plain(_raw_store()), _Scanning(_raw_store())
        from_full, from_scan = _finalize_result(plain), _finalize_result(scanning)
        self.assertEqual("finalized", from_full.get("status"))
        self.assertEqual(from_full, from_scan, "the narrowed read changed what session_commit returned")
        self.assertGreater(len(plain.appended), 2,
                           "the finalize branch wrote too little to prove anything")
        self.assertEqual(plain.appended, scanning.appended,
                         "the narrowed read changed what session_commit wrote")

    def test_a_finalized_session_is_not_finalized_twice_by_the_narrowed_read(self):
        """`context_session_boundary` is the second type the finalize read asks for, and this is
        the only thing it is asked for: the already-finalized check. Drop it from the list and the
        commit finalizes a session that was already finalized -- a second boundary, a second final
        summary and its index postings written over the first, with no error. Nothing else in this
        file notices, because every other assertion holds on a store with no final boundary in it.
        """
        raw = _raw_store()
        raw.append({"record_type": "context_session_boundary", "scope": dict(SCOPE),
                    "boundary_hash": 22, "final_session_boundary": True, "status": "finalized",
                    "updated_at_ms": 2500})
        plain, scanning = _Plain(raw), _Scanning(raw)
        from_full, from_scan = _finalize_result(plain), _finalize_result(scanning)
        self.assertNotEqual("finalized", from_full.get("status"),
                            "the fixture's final boundary was not seen, so nothing is proven")
        self.assertEqual([], plain.appended)
        self.assertEqual(from_full, from_scan,
                         "the two reads disagreed about whether the session was already finalized")
        self.assertEqual(plain.appended, scanning.appended)

    def test_the_overlap_branch_sees_the_same_overlap_either_way(self):
        """The overlap read only happens on a non-forced commit: `force` sets the overlap limit to
        zero and the read is skipped entirely."""
        pending = [
            {"record_type": "session_buffer_event", "event_id_hash": 4,
             "scope": dict(SCOPE), "updated_at_ms": 5000,
             "envelope": {"scope": dict(SCOPE),
                          "messages": [{"role": "user", "content": "four"}]}},
        ]
        args = {"scope": dict(SCOPE), "force": False, "threshold_messages": 1,
                "extraction_context_overlap_messages": 2}
        results = {}
        for label, cls in (("full", _Plain), ("scan", _Scanning)):
            adapter = cls(_raw_store())
            adapter.pending = pending
            outcome = _commit(adapter, args)
            results[label] = (outcome, adapter)
        (full_out, full_adapter), (scan_out, scan_adapter) = results["full"], results["scan"]
        self.assertTrue(full_adapter.batch_extract_args,
                        "the fixture never reached batch_extract, so no overlap was computed")
        full_extract = full_adapter.batch_extract_args[-1]
        scan_extract = scan_adapter.batch_extract_args[-1]
        self.assertTrue(full_extract["extraction_context_event_ids"],
                        "the fixture produced an EMPTY overlap set, so equality proves nothing")
        self.assertEqual(full_extract["extraction_context_event_ids"],
                         scan_extract["extraction_context_event_ids"],
                         "the narrowed read changed the overlap events")
        self.assertEqual(full_extract["extraction_context_messages"],
                         scan_extract["extraction_context_messages"],
                         "the narrowed read changed the overlap messages")
        self.assertEqual(full_out, scan_out)
        self.assertEqual(0, scan_adapter.read_all_calls, "the overlap branch read the whole store")
        self.assertIn(OVERLAP_TYPES + [MEMORY_TOMBSTONE_RECORD_TYPE], scan_adapter.scanned_for)


if __name__ == "__main__":
    unittest.main()
