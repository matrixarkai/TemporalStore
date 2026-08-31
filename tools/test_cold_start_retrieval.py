#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A node must be scoreable the moment it exists, not once something warms it.

A context_node draws its embedding from two places: its PATH, written when the node is created, and
its SUMMARY, written later by refresh_summaries. Only the first exists at creation time. Remove it
and a node cannot be scored until a summary lands, so the traversal selects nothing and the pack
degrades to profile entities alone -- a 40-event store returning 1 item instead of 38.

Every test in this file retrieves ONCE against a freshly built store. That matters more than the
assertions: 25 retrieves inside one process all pass even with the bug, because the first retrieve
warms the state the rest depend on. A warm check cannot see this failure.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import matrixark_mcp_server as mcp


def scope():
    return {"account_id": "acct_local", "tenant_id": "cold", "user_id": "alice",
            "session_id": "s0", "agent_name": "cold"}


def build_store(turns=12):
    tmp = tempfile.mkdtemp()
    adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "cold.jsonl")
    server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
    for index in range(turns):
        server.call_tool("matrixark_ingest", {
            "scope": scope(), "finalize": True,
            "messages": [{"role": "user",
                          "content": "Note %d: the widget code is W%03d." % (index, index)}]})
    server.call_tool("matrixark_session_commit", {"scope": scope()})
    server.call_tool("matrixark_refresh_summaries", {"scope": scope(), "limit": 200})
    return adapter, server


def pack_items(server):
    pack = server.call_tool("matrixark_retrieve", {"scope": scope(), "query": "widget code"})
    return [str(item.get("text") or "")
            for group in pack.get("groups") or []
            for item in group.get("items") or []]


class ColdStartRetrievalCase(unittest.TestCase):
    def test_first_retrieve_after_build_returns_the_events(self):
        """The regression this guards returned exactly one item here."""
        _adapter, server = build_store(turns=12)
        items = pack_items(server)
        self.assertGreater(len(items), 5,
                           "a cold store returned %d items -- nodes were unscoreable at first "
                           "retrieve, so the traversal selected nothing" % len(items))

    def test_every_node_has_an_embedding_the_moment_it_exists(self):
        adapter, _server = build_store(turns=6)
        records = adapter.read_all()
        nodes = {r.get("node_hash") for r in records
                 if r.get("record_type") == "context_node"}
        # Folded: a node's path vector rides on the context_node record itself; the
        # separate rows are retired from new logs.
        embedded = {r.get("node_hash") for r in records
                    if r.get("record_type") == "context_node" and r.get("vector")}
        summary_nodes = {r.get("node_hash") for r in records
                         if r.get("record_type") == "context_summary"}
        unreachable = {n for n in nodes if n not in embedded and n not in summary_nodes}
        self.assertFalse(unreachable,
                         "%d node(s) have neither a path embedding nor a summary, so nothing can "
                         "score them" % len(unreachable))

    def test_events_reach_the_pack_and_are_not_capped_at_top_k(self):
        """top_k_per_layer caps child NODES per parent, never events under one node."""
        _adapter, server = build_store(turns=40)
        items = pack_items(server)
        self.assertGreater(len(items), 24,
                           "events under a single node must not be bounded by top_k_per_layer "
                           "(got %d)" % len(items))


if __name__ == "__main__":
    unittest.main()
