#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A queued request must not outlive the caller waiting for it.

The shared proxy daemon serializes every caller behind one lock and drives the engine over a
single stdin/stdout pipe, so requests cannot interleave. The deadline used to be measured from the
moment a request WON that lock, while the caller's own budget started before it queued -- so the
daemon would spend a full budget on an answer whose caller had already timed out, holding the lock
while the next caller queued behind a result nobody would read.

These tests drive `_call_proxy` directly against a fake engine process, so they assert the
behaviour rather than a duration: the contended call must not reach the engine at all.
"""
import json
import threading
import time
import unittest

import matrixark_rust_proxy_daemon as daemon


class _FakeStdin:
    """Records what the daemon tried to send the engine."""

    def __init__(self):
        self.writes: list[str] = []

    def write(self, payload):
        self.writes.append(payload)

    def flush(self):
        pass


class _FakeStdout:
    """Answers every request immediately, so a call that REACHES the engine succeeds."""

    def __init__(self):
        self._answers = 0

    def readline(self):
        self._answers += 1
        return json.dumps({"ok": True, "answered": self._answers}) + "\n"


class _FakeProc:
    def __init__(self):
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout()

    def poll(self):
        return None


def _daemon_with_fake_engine():
    """A daemon whose engine is a fake, and which never tries to start a real one."""
    instance = daemon.RustProxyDaemon.__new__(daemon.RustProxyDaemon)
    instance._lock = threading.Lock()
    instance._proc = _FakeProc()
    instance._log_file = None
    instance._ensure_proxy = lambda: None
    return instance


class QueuedRequestDoesNotOutliveItsCaller(unittest.TestCase):
    def test_a_request_that_spent_its_budget_queueing_is_not_started(self):
        """The whole point: work with no reader must not be started, or handed the lock."""
        instance = _daemon_with_fake_engine()
        # Smallest budget the daemon honours: request_timeout_ms is floored at 2.0s + 2.0s.
        request = {"op": "get_string", "request_timeout_ms": 1}
        budget_s = 2.0

        holder_has_lock = threading.Event()
        release_holder = threading.Event()

        def hold_the_lock():
            with instance._lock:
                holder_has_lock.set()
                release_holder.wait(timeout=30)

        holder = threading.Thread(target=hold_the_lock, daemon=True)
        holder.start()
        self.assertTrue(holder_has_lock.wait(timeout=5), "holder never took the lock")

        result: dict = {}

        def caller():
            result.update(instance._call_proxy(request))

        waiter = threading.Thread(target=caller, daemon=True)
        waiter.start()
        # Outlast the budget while the caller is stuck in the queue, then let it through.
        time.sleep(budget_s + 0.5)
        release_holder.set()
        waiter.join(timeout=30)
        holder.join(timeout=5)

        self.assertTrue(result, "the queued call never returned")
        self.assertTrue(result.get("daemon_abandoned"), f"expected an abandoned call, got {result}")
        self.assertFalse(result.get("ok"), "an abandoned request has no answer to report")
        # The claim that matters: the engine was never asked. A request whose caller has gone
        # must not consume the one lock every other caller is waiting for.
        self.assertEqual(
            instance._proc.stdin.writes,
            [],
            "a request that spent its budget queueing was still sent to the engine",
        )
        self.assertGreaterEqual(result.get("daemon_queue_wait_ms", 0), int(budget_s * 1000))

    def test_an_uncontended_request_still_reaches_the_engine(self):
        """The control. Without this, dropping every request would pass the test above."""
        instance = _daemon_with_fake_engine()
        result = instance._call_proxy({"op": "get_string", "request_timeout_ms": 60000})

        self.assertTrue(result.get("ok"), f"an uncontended call should be served, got {result}")
        self.assertNotIn("daemon_abandoned", result)
        self.assertEqual(len(instance._proc.stdin.writes), 1, "the engine should have been asked once")

    def test_a_request_queued_within_its_budget_is_still_served(self):
        """Queueing is normal and must stay survivable -- only a SPENT budget is abandoned."""
        instance = _daemon_with_fake_engine()
        holder_has_lock = threading.Event()
        release_holder = threading.Event()

        def hold_the_lock():
            with instance._lock:
                holder_has_lock.set()
                release_holder.wait(timeout=30)

        holder = threading.Thread(target=hold_the_lock, daemon=True)
        holder.start()
        self.assertTrue(holder_has_lock.wait(timeout=5), "holder never took the lock")

        result: dict = {}

        def caller():
            # A 60s budget, queued for well under it.
            result.update(instance._call_proxy({"op": "get_string", "request_timeout_ms": 60000}))

        waiter = threading.Thread(target=caller, daemon=True)
        waiter.start()
        time.sleep(0.5)
        release_holder.set()
        waiter.join(timeout=30)
        holder.join(timeout=5)

        self.assertTrue(result.get("ok"), f"a briefly queued call should be served, got {result}")
        self.assertEqual(len(instance._proc.stdin.writes), 1, "the engine should have been asked once")


if __name__ == "__main__":
    unittest.main()
