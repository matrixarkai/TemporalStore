#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""raw_storage_policy="attachment" skips chunk materialization; anything else does not.

A resource is normally chunked, embedded and indexed so it can be recalled selectively. For a
file that is fetched rarely or never that trade is wrong, and measured it is expensive: one
66.2 KB document cost 7.05x its own size (60 chunk records at 3.75x plus 76 embeddings at
2.93x), with 32-dim deterministic vectors -- a 384-dim encoder takes the same file toward 40x.

Both arms are asserted here. The default arm matters as much as the new one: the gate must not
quietly disable chunking for ordinary resources, which is what retrieval depends on.
"""
from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp

DOC = "\n\n".join("## Section %d\n%s" % (n, ("operational detail line %d. " % n) * 40)
                  for n in range(60))


def ingest(policy):
    scope = {"account_id": "acct_local", "tenant_id": "attachpolicy", "user_id": "u",
             "session_id": "s0", "agent_name": "t"}
    adapter = mcp.MatrixArkLocalAdapter(Path(tempfile.mkdtemp()) / "a.jsonl")
    server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
    args = {"scope": scope, "kind": "resource", "finalize": True,
            "text": DOC, "raw_uri": "file://ops-handbook.md"}
    if policy:
        args["raw_storage_policy"] = policy
    server.call_tool("matrixark_ingest", args)
    # The import runs on a background worker. Waiting for the FIRST chunk and sleeping a fixed
    # interval is not enough: under suite load the worker is still emitting records when the
    # sleep expires, and the assertions then see a half-written import. Wait for the record
    # count to stop changing instead, so the test tracks the worker rather than the clock.
    deadline = time.time() + 180
    stable_for = 0
    previous = -1
    while time.time() < deadline:
        rows = adapter.read_all()
        started = any(r.get("record_type") in ("resource_chunk", "resource_manifest")
                      or r.get("record_type") == "resource_import_task" for r in rows)
        if started and len(rows) == previous:
            stable_for += 1
            if stable_for >= 3:
                break
        else:
            stable_for = 0
        previous = len(rows)
        time.sleep(1)
    return adapter.read_all()


def of_type(rows, record_type):
    return [r for r in rows if r.get("record_type") == record_type]


def stored_bytes(rows):
    return sum(len(json.dumps(r)) for r in rows)


class AttachmentResourcePolicyTest(unittest.TestCase):
    def test_attachment_writes_no_chunks(self):
        rows = ingest("attachment")
        self.assertEqual([], of_type(rows, "resource_chunk"),
                         "attachment policy must not materialize chunks")

    def test_attachment_keeps_the_manifest_so_the_file_stays_discoverable(self):
        """Skipping chunks must not make the resource invisible -- it is stored, not dropped."""
        rows = ingest("attachment")
        self.assertTrue(of_type(rows, "resource_manifest"),
                        "attachment lost its manifest and is no longer discoverable")

    def test_default_still_chunks(self):
        """The gate must not disable chunking for ordinary resources."""
        rows = ingest(None)
        self.assertTrue(of_type(rows, "resource_chunk"),
                        "default policy stopped materializing chunks")

    def test_attachment_is_dramatically_cheaper(self):
        """The whole point: an attachment should cost metadata, not multiples of itself."""
        attachment = stored_bytes(ingest("attachment"))
        default = stored_bytes(ingest(None))
        self.assertLess(attachment * 3, default,
                        "attachment (%d bytes) is not materially cheaper than default (%d)"
                        % (attachment, default))
        # 4x, not the ~1x the chunk saving alone might suggest. Skipping chunks removes the
        # chunk records and their embeddings, but an attachment STILL stores the document text
        # twice as raw text -- once in session_buffer_event and once in context_event, ~1.05x
        # each -- which no chunk gate touches. Measured breakdown of the attachment arm:
        #
        #     session_buffer_event  1 record   71,560 bytes (1.06x)
        #     context_event         1 record   71,023 bytes (1.05x)
        #     context_embedding    11 records  27,298 bytes (0.40x)
        #     context_summary       7 records  25,027 bytes (0.37x)
        #     TOTAL                           238,000 bytes (3.51x)
        #
        # That duplicated full text is the next target and a separate change; this bound is set
        # where the gate actually lands so it fails if the saving regresses, not at an
        # aspirational number the gate alone cannot reach.
        self.assertLess(attachment, len(DOC) * 4,
                        "attachment cost %d exceeds 4x the %d-char source"
                        % (attachment, len(DOC)))


class ResourceEventTextBoundTest(unittest.TestCase):
    """A resource must not store its whole document as event text as well as in its chunks."""

    def test_resource_event_text_is_bounded(self):
        rows = ingest(None)
        events = [r for r in rows if r.get("record_type") == "context_event"
                  and r.get("source_kind") == "resource"]
        self.assertTrue(events, "no resource context_event was written")
        for event in events:
            self.assertLess(
                len(str(event.get("text") or "")), len(DOC) // 4,
                "resource event still carries the whole document (%d of %d chars)"
                % (len(str(event.get("text") or "")), len(DOC)))

    def test_the_document_itself_is_untouched(self):
        """The bound is on the RECORD, never on the input -- chunks derive from the same
        messages list, so clipping the input would truncate the document instead of
        deduplicating its storage."""
        rows = ingest(None)
        joined = "".join(str(r.get("text") or "") for r in rows
                         if r.get("record_type") == "resource_chunk")
        for n in range(0, 60, 7):
            self.assertIn("Section %d" % n, joined,
                          "section %d vanished from the chunks -- the document was truncated" % n)

    def test_message_ingest_text_is_not_bounded(self):
        """Only resource/skill kinds are bounded; an ordinary message keeps its full text."""
        from matrixark_mcp_core_resource_io import bound_resource_event_text
        long_text = "x" * 50000
        self.assertEqual(long_text, bound_resource_event_text("message", long_text, ""))
        self.assertLess(len(bound_resource_event_text("resource", long_text, "file://a")),
                        len(long_text))


if __name__ == "__main__":
    unittest.main()
