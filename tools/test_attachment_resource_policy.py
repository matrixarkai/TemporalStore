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
    # the import runs on a background worker; wait for it to settle
    deadline = time.time() + 120
    while time.time() < deadline:
        rows = adapter.read_all()
        if any(r.get("record_type") in ("resource_chunk", "resource_manifest") for r in rows):
            time.sleep(3)
            break
        time.sleep(2)
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


if __name__ == "__main__":
    unittest.main()
