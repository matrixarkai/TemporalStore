# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The resolved-status set must cover the statuses the flush actually writes.

`pre_retrieval_idle_commit_flush` decides a task is done by looking its status up in
`IDLE_COMMIT_RESOLVED_STATUSES`, and it writes those statuses itself, in the same function. A
status it writes but does not list is a task that is never marked resolved and is re-attempted on
every retrieve, forever, with no error anywhere.

That happened. `idle_commit_attempted` was removed from the set because a search of the repository
for the name found nothing -- it is built, not spelled:

    status = "committed" if commit_status == "committed" else "attempted"
    ...
    "status": f"idle_commit_{status}",

So this test does not search for names. It runs the flush and reads what it wrote.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import matrixark_mcp_retrieve_request as flush_module

SCOPE = {"tenant_id": "acme", "user_id": "dana", "session_id": "s-1"}


class _Target:
    """Only what the flush reaches for: session_commit, task records, append."""

    def __init__(self, commit_status, resolution_records=()):
        self.commit_status = commit_status
        self.appended = []
        self._resolutions = list(resolution_records)

    def session_commit(self, commit_args):
        return {"status": self.commit_status, "committed_event_count": 1}

    def idle_commit_task_records(self, scope):
        scheduled = {
            "record_type": "matrixark_async_pipeline_task",
            "status": "idle_commit_scheduled",
            "task_hash": 4242,
            "event_id_hash": 7,
            "scope": SCOPE,
            "idle_commit_deadline_ms": 1,          # long past
            "idle_commit_timeout_ms": 1000,
            "threshold_messages": 20,
            "updated_at_ms": 1,
            "stages": ["extraction", "summary"],
        }
        return [scheduled] + self._resolutions

    def append(self, record):
        self.appended.append(record)


def _flush(target):
    return flush_module.pre_retrieval_idle_commit_flush(
        target, {"pre_retrieval_idle_commit_flush": True}, {}, scope=SCOPE)


def _written_statuses(target):
    return [r.get("status") for r in target.appended
            if r.get("record_type") == "matrixark_async_pipeline_task"]


class TheResolvedSetCoversWhatTheFlushWrites(unittest.TestCase):
    def test_the_attempted_outcome_is_written_and_is_resolved(self):
        """A commit that returns anything but "committed" writes idle_commit_attempted."""
        target = _Target("accepted")
        _flush(target)

        written = _written_statuses(target)
        self.assertIn("idle_commit_attempted", written,
                      "the flush no longer writes this status; the guard below is then vacuous")
        self.assertIn(
            "idle_commit_attempted", flush_module.IDLE_COMMIT_RESOLVED_STATUSES,
            "the flush writes idle_commit_attempted but does not count it as resolved, so the "
            "task is re-attempted on every retrieve and nothing reports it",
        )

    def test_the_committed_outcome_is_written_and_is_resolved(self):
        target = _Target("committed")
        _flush(target)

        written = _written_statuses(target)
        self.assertIn("idle_commit_committed", written)
        self.assertIn("idle_commit_committed", flush_module.IDLE_COMMIT_RESOLVED_STATUSES)

    def test_every_status_the_flush_writes_is_accounted_for(self):
        """The general form, so a NEW outcome cannot be added without a decision about it.

        A status is accounted for if it resolves the task, or if it is one the file documents as
        deliberately unresolved. `idle_commit_skipped` is that second kind and is written by the
        drain rather than here; it is listed so this test states the whole contract in one place.
        """
        deliberately_unresolved = {"idle_commit_skipped"}
        for commit_status in ("committed", "accepted", "deferred", ""):
            target = _Target(commit_status)
            _flush(target)
            for status in _written_statuses(target):
                with self.subTest(commit_status=commit_status, written=status):
                    self.assertIn(
                        status,
                        set(flush_module.IDLE_COMMIT_RESOLVED_STATUSES) | deliberately_unresolved,
                        "the flush writes %r, which neither resolves the task nor is a documented "
                        "exception; such a task returns on every retrieve" % (status,),
                    )

    def test_a_resolved_task_is_not_attempted_again(self):
        """What the set is FOR: the resolution record must stop the re-attempt."""
        target = _Target("accepted")
        _flush(target)
        resolution = [r for r in target.appended
                      if r.get("status") == "idle_commit_attempted"]
        self.assertTrue(resolution, "nothing was written to resolve the task")

        again = _Target("accepted", resolution_records=resolution)
        result = _flush(again)
        self.assertEqual(
            [], _written_statuses(again),
            "the task was re-attempted even though a resolution record for it exists: %r" % (result,),
        )


if __name__ == "__main__":
    unittest.main()
