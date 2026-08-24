#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""mem0 `delete_users`: forget one named subject, or every subject that holds memories.

`delete_all` forgets ONE subject and `reset` wipes the tenant; there was nothing in between, so a
caller wanting mem0's no-argument `delete_users()` had to list the subjects and loop, getting each
confirm token right themselves.

What is worth pinning is the honesty of the result, not the happy path. A wipe that half worked
must not return looking like a wipe that worked -- the caller is deleting data and needs to know
which subjects still hold it.
"""
import unittest

try:
    from tools import matrixark_mem0_compat as compat
except ImportError:  # run from tools/ dir
    import matrixark_mem0_compat as compat


class _Memory(compat.Memory):
    """A shim whose HTTP calls are recorded instead of sent."""

    def __init__(self, listed, *, failing=()):
        self.listed = listed
        self.failing = set(failing)
        self.forgotten = []
        self.users_calls = 0

    def users(self, **kw):
        self.users_calls += 1
        return {"results": list(self.listed)}

    def delete_all(self, *, user_id=None, agent_id=None, run_id=None, **kw):
        if not user_id:
            raise ValueError("delete_all requires user_id (the subject to forget)")
        if user_id in self.failing:
            raise RuntimeError("forget refused for %s" % user_id)
        self.forgotten.append(user_id)
        return {"forgotten": user_id}


class DeleteUsersTests(unittest.TestCase):
    def test_a_named_subject_is_forgotten(self):
        memory = _Memory([])
        result = memory.delete_users(user_id="alice")
        self.assertEqual(["alice"], memory.forgotten)
        self.assertEqual(1, result["deleted"])
        self.assertEqual([], result["failed"])
        self.assertEqual(0, memory.users_calls, "a named subject needs no listing")

    def test_no_arguments_forgets_every_listed_subject(self):
        memory = _Memory([{"type": "user", "name": "alice"}, {"type": "user", "name": "bob"}])
        result = memory.delete_users()
        self.assertEqual(["alice", "bob"], memory.forgotten)
        self.assertEqual(2, result["deleted"])

    def test_a_subject_that_fails_is_reported_not_skipped(self):
        """A partial wipe must not read as a complete one."""
        memory = _Memory(
            [{"type": "user", "name": "alice"}, {"type": "user", "name": "bob"}],
            failing=["bob"],
        )
        result = memory.delete_users()
        self.assertEqual(["alice"], memory.forgotten)
        self.assertEqual(1, result["deleted"])
        self.assertEqual(["bob"], [row["user_id"] for row in result["failed"]])
        self.assertIn("refused", result["failed"][0]["error"])
        self.assertEqual(2, result["subjects_listed"],
                         "the caller can see one of the two listed subjects still holds memories")

    def test_non_user_subjects_are_left_alone(self):
        """An agent or run is not addressable by forget, so it is not silently counted as deleted."""
        memory = _Memory([
            {"type": "user", "name": "alice"},
            {"type": "agent", "name": "planner"},
            {"type": "run", "name": "run-7"},
        ])
        result = memory.delete_users()
        self.assertEqual(["alice"], memory.forgotten)
        self.assertEqual(1, result["subjects_listed"])

    def test_an_agent_only_call_raises_rather_than_deleting_nothing(self):
        memory = _Memory([])
        with self.assertRaises(ValueError) as caught:
            memory.delete_users(agent_id="planner")
        self.assertIn("user_id", str(caught.exception))
        self.assertEqual([], memory.forgotten)
        self.assertEqual(0, memory.users_calls)

    def test_a_run_only_call_raises_too(self):
        memory = _Memory([])
        with self.assertRaises(ValueError):
            memory.delete_users(run_id="run-7")

    def test_nothing_to_delete_is_not_an_error(self):
        memory = _Memory([])
        result = memory.delete_users()
        self.assertEqual({"deleted": 0, "results": [], "failed": [], "subjects_listed": 0}, result)

    def test_a_listing_in_the_items_shape_is_understood(self):
        memory = _Memory([])
        memory.users = lambda **kw: {"items": [{"type": "user", "name": "carol"}]}
        memory.delete_users()
        self.assertEqual(["carol"], memory.forgotten)

    def test_a_row_without_a_type_is_treated_as_a_user(self):
        """`users()` labels rows, but a bare listing should still be actionable."""
        memory = _Memory([{"name": "dave"}])
        memory.delete_users()
        self.assertEqual(["dave"], memory.forgotten)


if __name__ == "__main__":
    unittest.main()
