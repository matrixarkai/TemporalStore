#!/usr/bin/env python3
"""The prior-context event window must not change what the consumer sees.

`collect_prior_context` walks records newest-first and stops at MAX_PRIOR_MESSAGES, so fetching a
subject's whole event history to select eight is work whose result is discarded. Capping the fetch
is only safe if the payload is identical -- so that is what this asserts, against the real
consumer, over a history far deeper than the window.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from tools.matrixark_mcp_core import MAX_PRIOR_MESSAGES
    from tools.matrixark_mcp_core_session import collect_prior_context
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_core import MAX_PRIOR_MESSAGES
    from matrixark_mcp_core_session import collect_prior_context


def _event(index: int, session: str = "s1") -> dict:
    scope = {"user_id": "deep", "session_id": session, "tenant_id": "t"}
    return {
        "record_type": "context_event",
        "event_id_hash": 1000 + index,
        "scope": scope,
        "scope_key": "t=t|u=deep|s=%s" % session,
        "envelope": {"scope": scope, "messages": [{"role": "user", "content": "fact %d" % index}]},
        "text": "fact %d" % index,
        "created_at_ms": 1_700_000_000_000 + index,
    }


class PriorContextWindowTest(unittest.TestCase):
    def envelope(self) -> dict:
        scope = {"user_id": "deep", "session_id": "s1", "tenant_id": "t"}
        return {"scope": scope, "messages": [{"role": "user", "content": "the new one"}]}

    def test_window_far_deeper_than_the_consumer_needs_changes_nothing(self):
        history = [_event(i) for i in range(1000)]
        window = 256
        self.assertGreater(window, MAX_PRIOR_MESSAGES,
                           "a window at or below the consumer's own limit would truncate it")

        full = collect_prior_context(self.envelope(), history)
        capped = collect_prior_context(self.envelope(), history[-window:])
        self.assertEqual(full, capped,
                         "capping the fetch changed what prior context reports")

    def test_the_consumer_really_does_stop_early(self):
        """Guards the premise: if this ever stopped being true the cap would start truncating."""
        history = [_event(i) for i in range(1000)]
        payload = collect_prior_context(self.envelope(), history)
        self.assertLessEqual(len(payload.get("messages") or []), MAX_PRIOR_MESSAGES)

    def test_a_window_smaller_than_the_limit_does_truncate(self):
        """The test above must not be passing vacuously: too small a window IS visible."""
        history = [_event(i) for i in range(1000)]
        full = collect_prior_context(self.envelope(), history)
        starved = collect_prior_context(self.envelope(), history[-2:])
        self.assertNotEqual(full, starved,
                            "the comparison cannot see truncation, so it proves nothing")

    def test_interleaved_sessions_still_fill_the_window(self):
        """The realistic worry: other sessions crowding the newest slice."""
        history = []
        for i in range(1000):
            history.append(_event(i, session="s1" if i % 4 == 0 else "s%d" % (i % 7 + 2)))
        full = collect_prior_context(self.envelope(), history)
        capped = collect_prior_context(self.envelope(), history[-256:])
        self.assertEqual(full, capped,
                         "with one session in four, a 256 window still holds the newest eight")


if __name__ == "__main__":
    unittest.main(verbosity=2)
