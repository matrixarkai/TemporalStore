#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The hot path supplies the summary it computes, so the policy that keeps it has one to keep.

Ingest is two-phase. The async path's `pending_event_record` carries
`"summary_text": summarize_text(text)`. The hot path computed the identical value 140 lines before
building its record and did not put it in -- so the same message produced a `context_event` with a
summary or without one depending on which half wrote it.

WHAT THAT DOES AND DOES NOT COST, checked rather than assumed. `store_event_summary_text` is a
per-tenant policy and it is **OFF by default**: `apply_storage_policy` strips `summary_text` from a
`context_event` unless a tenant turns it on, deliberately, to bound index growth -- every event
reader is written as `summary_text or text`, and for an event under 220 characters the summary was
a byte-identical copy of the text anyway. So with the default policy BOTH halves store no summary,
and the divergence is invisible.

It is visible for a tenant that turns the policy on. There, the async half stores a summary and the
hot half stores nothing, for the same message -- the policy can only keep a field the writer
supplied. That is what this file pins, and it is why the fixture sets the policy rather than
trusting the default.

THE FIXTURE HAS TO DISCRIMINATE, twice over. `summarize_text` collapses whitespace and truncates at
220 characters, so a short tidy string summarises to itself and an assertion could not tell a stored
summary from a stored `text`. And a test that never turned the policy on would pass on a record
whose field was stripped. `test_the_fixture_can_tell_them_apart` asserts the first; every case below
sets the policy for the second.

WHAT THIS DOES NOT CLAIM. `matrixark_mcp_ingest_message_records.context_event_record`, which the
other ingest module uses, writes `"summary_text": text[:512]` -- a raw 512-character slice, neither
absent nor `summarize_text`'s 220-character collapsed form. That is a third definition of the same
field and choosing between them changes stored data, so it is recorded in matrixarkai#1864 rather
than decided here.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

TOOLS = os.path.dirname(os.path.abspath(__file__))
if TOOLS not in sys.path:
    sys.path.insert(0, TOOLS)

import matrixark_mcp_server as mcp  # noqa: E402
import matrixark_tenant_policy as policy  # noqa: E402

try:
    from tools.matrixark_mcp_core import summarize_text
except ImportError:  # direct execution from tools/
    from matrixark_mcp_core import summarize_text

TENANT = "tenant_hot_path_summary"

#: Long enough to be truncated and spaced badly enough to be collapsed, so the summary cannot
#: coincide with the text.
MESSAGE = (
    "Alice   prefers    espresso\n\n\nover filter coffee, and said so again during the review "
    "of the ingestion path, at some length, because the point kept coming up in a way that "
    "eventually needed writing down somewhere a later reader would actually find it rather than "
    "in a comment nobody opens."
)


def _scope() -> dict:
    return {
        "account_id": "acct_local",
        "tenant_id": TENANT,
        "user_id": "alice",
        "session_id": "s1",
        "agent_name": "d",
    }


class TheHotPathSuppliesTheSummaryItComputes(unittest.TestCase):

    def setUp(self) -> None:
        # The policy is OFF by default and strips the field; with it off this file would pass on
        # a writer that supplies nothing.
        policy.set_tenant_policy(TENANT, {"store_event_summary_text": True})

    def _hot_path_events(self) -> list[dict]:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "events.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            try:
                server.call_tool(
                    "matrixark_ingest",
                    {"messages": [{"role": "user", "content": MESSAGE}], "scope": _scope()},
                )
                records = adapter.recent_records(512)
            finally:
                server.close(timeout_s=1.0)
        return [
            record
            for record in records
            if isinstance(record, dict)
            and record.get("record_type") == "context_event"
            and record.get("extraction_phase") == "hot_path"
        ]

    def test_the_fixture_can_tell_them_apart(self) -> None:
        """A floor. A tidy short message summarises to itself and proves nothing below."""
        summary = summarize_text(MESSAGE)
        self.assertNotEqual(
            MESSAGE, summary,
            "the fixture summarises to itself, so every assertion below would pass on a record "
            "that stored `text` under the summary's name",
        )
        self.assertNotIn("\n", summary, "the summary is the whitespace-collapsed form")
        self.assertLess(len(summary), len(MESSAGE), "and the truncated one")

    def test_a_hot_path_event_carries_a_summary(self) -> None:
        events = self._hot_path_events()
        self.assertTrue(events, "no hot-path context_event was written, so nothing was checked")
        for event in events:
            with self.subTest(event=event.get("event_id_hash")):
                self.assertTrue(
                    str(event.get("summary_text") or "").strip(),
                    "the hot path wrote a context_event with no summary_text for a tenant whose "
                    "policy keeps the field, while computing the value it did not store",
                )

    def test_the_summary_is_the_one_the_async_path_would_have_written(self) -> None:
        """Not merely non-empty: the same function on the same text, so the halves agree."""
        events = self._hot_path_events()
        self.assertTrue(events, "no hot-path context_event was written")
        for event in events:
            with self.subTest(event=event.get("event_id_hash")):
                stored_text = str(event.get("text") or "")
                # A floor per record: ingest composes the text it stores (it prepends the role),
                # and if THAT summarises to itself the comparison below proves nothing.
                self.assertNotEqual(
                    stored_text, summarize_text(stored_text),
                    "this record's own text summarises to itself, so the assertion below cannot "
                    "tell a stored summary from a stored text",
                )
                self.assertEqual(
                    summarize_text(stored_text), event.get("summary_text"),
                    "the hot path's summary is not the summary of the text it stored, which is "
                    "what the async half writes for the same event",
                )


if __name__ == "__main__":
    unittest.main()
