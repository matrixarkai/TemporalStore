#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The session buffer must commit ON the message that reaches the threshold, not after it.

`append_session_buffer_event` writes through the append coalescer, so the record is not readable
until the coalescer is flushed. The ingest then counts the pending buffer to decide whether
`session_buffer_threshold` has been reached, and on one of the two branches that do this there was
no flush between the write and the count:

    self.append_session_buffer_event(...)      the message just ingested
    pending_events = self.pending_session_events(...)

So the count was one short and the commit fired on the message AFTER the one that reached the
threshold. The configured number was not the effective number -- at the default of 20, extraction
at 21.

Nothing announced it. No message is lost, the commit still happens, and the only difference is that
it happens one message late. The assertions here are therefore about EXACTLY WHICH ingest returns
the commit, because "a commit eventually happened" is true either way and is what made this
invisible.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

SCOPE = {"account_id": "acct_threshold", "tenant_id": "t", "user_id": "u",
         "session_id": "codex:threshold"}


def _server():
    adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "t.jsonl")
    return adapter, mcp.MatrixArkMcpServer(adapter, access_mode="dev")


def _ingest(server, index, threshold):
    return server.call_tool("matrixark_ingest", {
        "scope": SCOPE,
        "session_buffer_threshold": threshold,
        "auto_batch_extract": True,
        "messages": [{"role": "user" if index % 2 else "assistant",
                      "content": "message %d about the widget code" % index}],
    })


def _commits(results):
    """Which ingest indexes came back with a threshold commit, 1-based."""
    out = []
    for index, result in enumerate(results, 1):
        batch = result.get("auto_batch_extract_result")
        if isinstance(batch, dict) and batch.get("trigger_policy") == "threshold":
            out.append(index)
    return out


class TheBufferCommitsOnTheMessageThatReachesTheThresholdTest(unittest.TestCase):

    def test_a_threshold_of_two_commits_on_the_second_message(self) -> None:
        """The defect, at its smallest. With the count one short this fired on the third."""
        _, server = _server()
        results = [_ingest(server, i, 2) for i in range(1, 4)]
        self.assertEqual(
            [2], _commits(results),
            "a threshold of 2 must commit on the 2nd message; committing on the 3rd means the "
            "count did not include the message being decided about")

    def test_a_threshold_of_three_commits_on_the_third_message(self) -> None:
        """Not a special case of two. An off-by-one shows at every threshold, and a fix that
        happened to work only for 2 would pass the test above."""
        _, server = _server()
        results = [_ingest(server, i, 3) for i in range(1, 5)]
        self.assertEqual(
            [3], _commits(results),
            "a threshold of 3 must commit on the 3rd message")

    def test_the_commit_reports_the_threshold_as_its_trigger(self) -> None:
        """A commit that fires for some other reason at the right moment would satisfy the counts
        above while meaning something different."""
        _, server = _server()
        results = [_ingest(server, i, 2) for i in range(1, 3)]
        batch = results[-1].get("auto_batch_extract_result")
        self.assertIsInstance(batch, dict, "the second ingest returned no batch result at all")
        self.assertEqual("threshold", batch.get("trigger_policy"))
        self.assertEqual("committed", batch.get("status"))

    def test_the_earlier_messages_are_not_committed_one_at_a_time(self) -> None:
        """The other direction. A flush placed so that every message looks like it reaches the
        threshold would commit on the first, and the counts above only check WHICH index fired."""
        _, server = _server()
        results = [_ingest(server, i, 3) for i in range(1, 3)]
        self.assertEqual(
            [], _commits(results),
            "the buffer committed before reaching the threshold")

    def test_the_buffer_actually_fills_before_the_commit(self) -> None:
        """A floor. If nothing were being buffered at all, the assertions above would still hold
        for the wrong reason -- no commits, and none expected until the last message."""
        adapter, server = _server()
        _ingest(server, 1, 5)
        _ingest(server, 2, 5)
        pending = adapter.pending_session_events(SCOPE)
        self.assertEqual(
            2, len(pending),
            "two ingests below the threshold should leave two pending buffer events")


if __name__ == "__main__":
    unittest.main()
