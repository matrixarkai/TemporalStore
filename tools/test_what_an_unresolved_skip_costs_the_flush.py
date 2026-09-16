#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""What leaving `idle_commit_skipped` out of the resolved set actually costs -- measured, not read.

The note above `IDLE_COMMIT_RESOLVED_STATUSES` leaves a decision open, and states the cost of
changing it as ongoing: "Adding it here would stop the re-attempts". Read plainly that describes a
task the flush retries on every retrieve, which is exactly the failure mode recorded two paragraphs
above it for `idle_commit_attempted` -- "came back on every retrieve". Under that reading the
exclusion is expensive and the question is urgent.

It is not what happens. Driving the flush over repeated retrieves:

  * a skipped task is attempted ONCE and then resolved by its own attempt, because the flush's three
    outcomes -- `idle_commit_failed`, `idle_commit_committed`, `idle_commit_attempted` -- are all in
    the resolved set. There is no second attempt, let alone one per retrieve;
  * and the skip record contributes NOTHING while it happens. `idle_commit_skipped` is in neither
    `IDLE_COMMIT_RESOLVED_STATUSES` nor `IDLE_COMMIT_ACTED_ON_STATUSES`, so the flush filters it out
    before either loop sees it. The outcome is identical to the record not existing.

So the exclusion does not buy a re-attempt. The one attempt happens anyway.

THE PART THAT BEARS ON THE DECISION. The note's stated worry is that "marking dead work resolved
looks identical from outside to dropping live work". That already happens: a declining
`session_commit` is written as `idle_commit_attempted`, which IS resolved, so a task nothing
committed is marked done one retrieve later regardless. Adding `idle_commit_skipped` to the set
would move that by one retrieve; it would not introduce it.

THIS FILE CHANGES NO BEHAVIOUR and does not settle the question -- what a skip MEANS is a product
decision and the note is right to hold it open. It prices it, so that whoever settles it is choosing
between one attempt and none rather than between one attempt and an unbounded stream of them.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matrixark_mcp_retrieve_request as flush_module  # noqa: E402

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "s-1"}
SCHEDULED_HASH = 4242
RETRIEVES = 4

#: What the drain writes when `session_commit` declines (matrixark_local_adapter_retrieval). It
#: reuses the SCHEDULED task's hash and carries no `scheduled_task_hash`, and it is written as work
#: still outstanding -- empty `completed_stages`, a full `remaining_stages`.
DRAIN_SKIP = {
    "record_type": "matrixark_async_pipeline_task",
    "status": "idle_commit_skipped",
    "task_hash": SCHEDULED_HASH,
    "event_id_hash": 7,
    "scope": SCOPE,
    "completed_stages": [],
    "remaining_stages": ["summary", "compression", "embedding"],
    "updated_at_ms": 2,
}


class _Store:
    """A store that keeps what the flush appends and serves it back on the next retrieve.

    Feeding the appended records back is the whole point: a flush that resolves a task writes the
    resolution into the store, and only a store that remembers can show the next retrieve skipping
    it. A fake that forgets would report one attempt per retrieve no matter what the code did.
    """

    def __init__(self, commit_status: str, seed=()) -> None:
        self.commit_status = commit_status
        self.commit_calls = 0
        self.records: list[dict] = [
            {
                "record_type": "matrixark_async_pipeline_task",
                "status": "idle_commit_scheduled",
                "task_hash": SCHEDULED_HASH,
                "event_id_hash": 7,
                "scope": SCOPE,
                "idle_commit_deadline_ms": 1,      # long past, so it is due on every retrieve
                "idle_commit_timeout_ms": 1000,
                "threshold_messages": 20,
                "updated_at_ms": 1,
                "stages": ["extraction", "summary"],
            }
        ] + list(seed)

    def session_commit(self, commit_args: dict) -> dict:
        self.commit_calls += 1
        return {"status": self.commit_status, "committed_event_count": 0}

    def idle_commit_task_records(self, scope: dict) -> list[dict]:
        return list(self.records)

    def append(self, record: dict) -> None:
        self.records.append(record)


def _retrieves(seed=(), commit_status: str = "declined", rounds: int = RETRIEVES):
    """Run the flush `rounds` times against one store; return (store, per-round statuses)."""
    store = _Store(commit_status, seed=seed)
    statuses = []
    for _ in range(rounds):
        out = flush_module.pre_retrieval_idle_commit_flush(store, {}, {}, scope=SCOPE)
        statuses.append(str(out.get("status") or ""))
    return store, statuses


class WhatAnUnresolvedSkipCostsTheFlush(unittest.TestCase):

    def test_the_flush_really_does_attempt_a_due_task(self):
        """The floor. If nothing is ever due, every measurement below is about nothing."""
        store, statuses = _retrieves()
        self.assertEqual(
            "attempted", statuses[0],
            "the first retrieve did not attempt the due task (it answered %r), so this file is "
            "measuring a flush that never runs" % statuses[0])
        self.assertGreaterEqual(
            store.commit_calls, 1,
            "session_commit was never called, so nothing below distinguishes anything")

    def test_a_skip_costs_exactly_one_attempt_not_one_per_retrieve(self):
        """The measurement the open note turns on."""
        store, statuses = _retrieves(seed=[DRAIN_SKIP])
        self.assertEqual(
            1, store.commit_calls,
            "a skipped task cost %d session_commit calls over %d retrieves. The note above "
            "IDLE_COMMIT_RESOLVED_STATUSES weighs the exclusion against ongoing re-attempts; if "
            "this is no longer 1, that reading has become the true one and the note should be "
            "re-read with the new number." % (store.commit_calls, RETRIEVES))
        self.assertEqual(
            ["no_due_idle_commits"] * (RETRIEVES - 1), statuses[1:],
            "the task was still due after its own attempt resolved it: %s" % statuses[1:])

    def test_the_skip_record_is_invisible_to_the_flush(self):
        """The control, and the other direction.

        Without this, the test above cannot tell "a skip costs one attempt" apart from "a skip
        causes one attempt". It does not cause it: the same attempt happens with no record at all,
        because `idle_commit_skipped` is in neither status set the flush reads.
        """
        with_skip, _ = _retrieves(seed=[DRAIN_SKIP])
        without_any, _ = _retrieves(seed=[])
        self.assertEqual(
            without_any.commit_calls, with_skip.commit_calls,
            "the skip record changed the number of attempts (%d against %d with no record at all), "
            "so it is no longer invisible to the flush and the note's framing needs revisiting"
            % (with_skip.commit_calls, without_any.commit_calls))
        self.assertNotIn(
            "idle_commit_skipped", flush_module.IDLE_COMMIT_ACTED_ON_STATUSES,
            "idle_commit_skipped is now acted on, which is the change the open note describes -- "
            "if it was made deliberately, this file records the old cost and should be updated")

    def test_a_resolved_status_costs_nothing_at_all(self):
        """The other control: a status that IS in the set suppresses the attempt entirely.

        This is what separates "the flush resolves everything it touches" from "the flush never
        attempts anything in this fixture".
        """
        committed = dict(DRAIN_SKIP, status="idle_commit_committed")
        store, statuses = _retrieves(seed=[committed])
        self.assertEqual(
            0, store.commit_calls,
            "a task the drain already committed was still attempted %d time(s); the resolved set "
            "is no longer suppressing it" % store.commit_calls)
        self.assertEqual(["no_due_idle_commits"] * RETRIEVES, statuses)

    def test_the_attempt_resolves_the_task_even_though_it_committed_nothing(self):
        """Why the open question is narrower than the note makes it look.

        `session_commit` declines throughout this file, so the attempt commits nothing -- and the
        flush still writes `idle_commit_attempted`, a resolved status. Dead work is marked done
        either way; the exclusion moves that by one retrieve rather than preventing it.
        """
        store, _ = _retrieves(seed=[DRAIN_SKIP])
        written = [str(record.get("status") or "") for record in store.records
                   if record.get("reason") == "pre_retrieval_idle_commit_flush"]
        self.assertEqual(
            ["idle_commit_attempted"], written,
            "the flush wrote %s for a commit that declined; this file's reasoning rests on that "
            "outcome being `idle_commit_attempted`" % written)
        self.assertLessEqual(
            set(written), set(flush_module.IDLE_COMMIT_RESOLVED_STATUSES),
            "the status the flush wrote is not in the resolved set, which would mean the task is "
            "re-attempted forever -- a different and much worse finding than this file records")


if __name__ == "__main__":
    unittest.main()
