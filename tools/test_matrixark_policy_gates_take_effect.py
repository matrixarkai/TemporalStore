#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Setting a tenant knob has to change what the deployment stores.

Before this, it did not. The knob was in the registry, the portal offered it, `resolve()` returned
exactly what the tenant set, and the write path never asked -- so two tenants with *opposite*
policies produced near-identical records. Nothing errored and nothing logged.

These tests are deliberately behavioural: they ingest the same text under two policies and compare
what landed. A test that asserts the gate function is called would have passed throughout the period
the knob did nothing, because the gate was always callable; it was simply never called.

Two details are load-bearing, both found by this test failing:

* A vector reaches storage by TWO routes -- its own `context_embedding` record, and an inline
  `vector` field written straight onto the owner. Handling only the first left 9 of 10 records
  still carrying vectors for a tenant that had opted out.

`extract_segments` is wired the same way and lives in its own change: honouring its default (OFF)
stops segment rows for every deployment that has set no policy, which six existing tests depend on.
* An embedding record usually carries no scope of its own; it is addressed by its owner's hash. The
  tenant is knowable only from the OWNER, and reading the embedding's own scope let 7 of 19 through.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_tenant_policy as policy  # noqa: E402


def _ingest_under(policies: dict) -> dict:
    """Ingest one identical message per tenant and report what was stored for each."""
    import matrixark_mcp_server as mcp

    for tenant, knobs in policies.items():
        policy.set_tenant_policy(tenant, knobs)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
        for tenant in policies:
            scope = {"tenant_id": tenant, "user_id": "u1", "session_id": "s1"}
            server.call_tool("matrixark_ingest", {
                "scope": scope, "finalize": True,
                "messages": [{"role": "user",
                              "content": "I am allergic to peanuts and I live in Kyoto."}]})
            server.call_tool("matrixark_session_commit", {"scope": scope})
        records = adapter.read_all()

    out = {}
    for tenant in policies:
        identities = {tenant, policy.tenant_hash_of(tenant)}
        segments = vectors = total = 0
        for record in records:
            scope = (record.get("scope") or record.get("access_scope")
                     or record.get("scope_key"))
            if policy.tenant_of(scope) not in identities:
                continue
            total += 1
            if str(record.get("record_type")) == "context_segment":
                segments += 1
            if record.get("vector"):
                vectors += 1
        out[tenant] = {"total": total, "segments": segments, "vectors": vectors}
    return out


class TheKnobChangesWhatIsStoredTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.result = _ingest_under({
            "gates_on": {"generate_embeddings": True},
            "gates_off": {"generate_embeddings": False},
        })

    def test_both_tenants_stored_something(self) -> None:
        # Without this the assertions below pass on a run that stored nothing at all.
        for tenant in ("gates_on", "gates_off"):
            with self.subTest(tenant=tenant):
                self.assertGreater(self.result[tenant]["total"], 0,
                                   "nothing was stored for %s, so this file proves nothing"
                                   % tenant)

    def test_generate_embeddings_off_stores_no_vectors(self) -> None:
        self.assertEqual(0, self.result["gates_off"]["vectors"],
                         "a tenant that declined embeddings still has stored vectors; a vector "
                         "reaches storage both as its own record and inline on the owner, so this "
                         "fails when only one route is gated")
        self.assertGreater(self.result["gates_on"]["vectors"], 0,
                           "no vectors stored for anyone, so the check above is vacuous")

    def test_declining_embeddings_leaves_the_records_themselves(self) -> None:
        # Declining vectors must not decline the memory: a tenant that turned embeddings off still
        # has its records, just without stored vectors on them.
        result = _ingest_under({
            "no_vectors": {"generate_embeddings": False},
        })["no_vectors"]
        self.assertGreater(result["total"], 0, "the records went too, not just their vectors")
        self.assertEqual(0, result["vectors"], "vectors survived with embeddings off")


class TheStripHandlesBothVectorShapesTest(unittest.TestCase):
    """`drop_vectors_for_opted_out_tenants` on its own.

    The ingest test above cannot reach this: on that path every vector arrives as a
    `context_embedding` record and the fold's owner check catches it first. But records are also
    written with an inline `vector` and no embedding record at all -- `context_node` is one -- and
    that shape has to be handled or a tenant who opted out keeps vectors on those records.
    """

    def setUp(self) -> None:
        import matrixark_mcp_local_adapter as adapter
        self.adapter = adapter
        policy.set_tenant_policy("strip_off", {"generate_embeddings": False})
        policy.set_tenant_policy("strip_on", {"generate_embeddings": True})

    def test_an_inline_vector_is_removed_for_an_opted_out_tenant(self) -> None:
        record = {"record_type": "context_node", "scope": {"tenant_id": "strip_off"},
                  "vector": [0.1, 0.2], "embedding_meta": {"model": "x"}}
        out = self.adapter.drop_vectors_for_opted_out_tenants([record])
        self.assertEqual(1, len(out), "the owner record itself must survive")
        self.assertNotIn("vector", out[0])
        # The metadata describes a vector that is no longer there; leaving it would tell a reader
        # this record was embedded when it was not.
        self.assertNotIn("embedding_meta", out[0])

    def test_a_separate_embedding_record_is_dropped_entirely(self) -> None:
        record = {"record_type": "context_embedding", "scope": {"tenant_id": "strip_off"},
                  "vector": [0.1, 0.2]}
        self.assertEqual([], self.adapter.drop_vectors_for_opted_out_tenants([record]))

    def test_an_opted_in_tenant_is_untouched(self) -> None:
        records = [{"record_type": "context_node", "scope": {"tenant_id": "strip_on"},
                    "vector": [0.1]}]
        out = self.adapter.drop_vectors_for_opted_out_tenants(records)
        self.assertIs(records, out, "an unaffected batch should not even be copied")

    def test_the_interned_scope_forms_are_all_understood(self) -> None:
        # A written record carries `scope_key` holding the tenant hash rather than a scope dict.
        # Reading only `scope` made this gate fail OPEN on nearly every record it saw.
        digest = policy.tenant_hash_of("strip_off")
        for field, value in (("scope", {"tenant_id": "strip_off"}),
                             ("access_scope", {"tenant_id": "strip_off"}),
                             ("scope_key", digest)):
            with self.subTest(field=field):
                record = {"record_type": "context_node", field: value, "vector": [0.1]}
                out = self.adapter.drop_vectors_for_opted_out_tenants([record])
                self.assertNotIn("vector", out[0],
                                 "%s was not understood, so the gate failed open" % field)

    def test_a_record_with_no_tenant_keeps_its_vector(self) -> None:
        # Nothing to attribute means nothing to decide, and failing closed here would delete
        # vectors from records that belong to tenants who never opted out.
        record = {"record_type": "context_node", "vector": [0.1]}
        out = self.adapter.drop_vectors_for_opted_out_tenants([record])
        self.assertIn("vector", out[0])


class TheBoundaryPolicyTest(unittest.TestCase):
    """Three knobs enforced in one place, because gating the writers does not scale.

    `context_event` is built in eight places and node-path vectors in at least two. A per-writer
    gate has to find them all and stay found, and a knob that half-works looks like it works -- an
    earlier attempt gated one writer of each and moved the counts 4 -> 3 and not at all. Every
    record from every writer crosses the adapter's append path, so the policy is applied there,
    where a new writer cannot bypass it by existing.
    """

    def setUp(self) -> None:
        import matrixark_mcp_local_adapter as adapter
        self.adapter = adapter

    def _batch(self, tenant):
        scope = {"tenant_id": tenant}
        return [
            {"record_type": "context_embedding", "embedding_type": "context_node",
             "scope": scope, "vector": [0.1]},
            {"record_type": "context_embedding", "embedding_type": "event_text",
             "scope": scope, "vector": [0.2]},
            {"record_type": "context_node", "scope": scope, "vector": [0.3]},
            {"record_type": "context_event", "scope": scope,
             "text": "t", "summary_text": "t"},
        ]

    def test_node_path_embeddings_off_drops_only_the_path_vectors(self) -> None:
        # The knob is specifically about vectorising a synthetic path string. It must not touch the
        # event-text embedding, nor the node's own vector, which is the L1 summary vector and
        # belongs to generate_embeddings.
        policy.set_tenant_policy("np_off", {"generate_embeddings": True,
                                            "node_path_embeddings": False})
        out = self.adapter.apply_storage_policy(self._batch("np_off"))
        kinds = [(r.get("record_type"), r.get("embedding_type")) for r in out]
        self.assertNotIn(("context_embedding", "context_node"), kinds)
        self.assertIn(("context_embedding", "event_text"), kinds)
        self.assertTrue(any(r.get("record_type") == "context_node" and r.get("vector")
                            for r in out), "the node's own summary vector was taken too")

    def test_an_embedding_with_no_type_is_not_swept_up_as_a_path_vector(self) -> None:
        # The knob is about ONE embedding_type. A record that does not declare one is not a
        # node-path vector, and matching it would quietly delete embeddings the tenant still wants
        # -- a widened condition here is indistinguishable from the knob working.
        policy.set_tenant_policy("np_off2", {"generate_embeddings": True,
                                             "node_path_embeddings": False})
        records = [{"record_type": "context_embedding", "scope": {"tenant_id": "np_off2"},
                    "vector": [0.4]}]
        self.assertEqual(1, len(self.adapter.apply_storage_policy(records)),
                         "an untyped embedding was dropped by the node-path knob")

    def test_node_path_embeddings_on_keeps_them(self) -> None:
        policy.set_tenant_policy("np_on", {"generate_embeddings": True,
                                           "node_path_embeddings": True})
        out = self.adapter.apply_storage_policy(self._batch("np_on"))
        self.assertIn(("context_embedding", "context_node"),
                      [(r.get("record_type"), r.get("embedding_type")) for r in out])

    def test_store_event_summary_text_off_omits_the_field(self) -> None:
        # Omitted rather than emptied: readers are written as `summary_text or text`, so absent
        # falls back to the text it copied while "" would read as "summarised to nothing".
        policy.set_tenant_policy("st_off", {"store_event_summary_text": False})
        out = self.adapter.apply_storage_policy(self._batch("st_off"))
        event = [r for r in out if r.get("record_type") == "context_event"][0]
        self.assertNotIn("summary_text", event)
        self.assertEqual("t", event["text"], "the text itself must survive")

    def test_store_event_summary_text_on_keeps_it(self) -> None:
        policy.set_tenant_policy("st_on", {"store_event_summary_text": True})
        out = self.adapter.apply_storage_policy(self._batch("st_on"))
        event = [r for r in out if r.get("record_type") == "context_event"][0]
        self.assertEqual("t", event["summary_text"])

    def test_embeddings_off_takes_every_vector_by_either_route(self) -> None:
        policy.set_tenant_policy("em_off", {"generate_embeddings": False})
        out = self.adapter.apply_storage_policy(self._batch("em_off"))
        self.assertEqual([], [r for r in out if r.get("record_type") == "context_embedding"])
        self.assertFalse(any(r.get("vector") for r in out))

    def test_a_batch_nothing_applies_to_is_returned_unchanged(self) -> None:
        policy.set_tenant_policy("all_on", {"generate_embeddings": True,
                                            "node_path_embeddings": True,
                                            "store_event_summary_text": True})
        records = self._batch("all_on")
        self.assertIs(records, self.adapter.apply_storage_policy(records),
                      "an unaffected batch should not even be copied")

    def test_the_old_entry_point_delegates_rather_than_reimplementing(self) -> None:
        # Two copies of a storage gate is how one of them drifts and silently stops enforcing.
        policy.set_tenant_policy("delegate", {"generate_embeddings": False})
        records = self._batch("delegate")
        self.assertEqual(self.adapter.apply_storage_policy(records),
                         self.adapter.drop_vectors_for_opted_out_tenants(records))


class TraverseSiblingSessionsTest(unittest.TestCase):
    """The one read-path knob wired here, checked across two sessions.

    There is no new traversal logic: retrieval already takes `session_scope` (`prefer` or `only`)
    from the request, then the ranking config, then a hardcoded default. The knob is the tenant's
    answer, applied as a CEILING -- "whether retrieval descends into sessions other than the
    current one" describes the deployment, so a per-request argument must not widen it.

    Checked the way it is observed rather than by reading the resolved value: store in one session,
    ask from another, and see whether the memory comes back. A test asserting the gate returns
    False would pass even if nothing consumed it -- which is the exact defect this file exists for.
    """

    def _ask_across_sessions(self, tenant, knobs):
        import matrixark_mcp_server as mcp
        policy.set_tenant_policy(tenant, knobs)
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
            server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")
            first = {"tenant_id": tenant, "user_id": "u1", "session_id": "sessionA"}
            second = {"tenant_id": tenant, "user_id": "u1", "session_id": "sessionB"}
            for scope, text in ((first, "I am allergic to peanuts."),
                                (second, "Today I went cycling.")):
                server.call_tool("matrixark_ingest", {
                    "scope": scope, "finalize": True,
                    "messages": [{"role": "user", "content": text}]})
                server.call_tool("matrixark_session_commit", {"scope": scope})
            answer = server.call_tool("matrixark_retrieve",
                                      {"scope": second, "query": "what am I allergic to?"})
        return "peanut" in str(answer).lower()

    def test_the_knob_decides_whether_another_session_is_reachable(self) -> None:
        # Both directions asserted. "not found" alone would also hold if retrieval were broken
        # outright, and "found" alone would hold if the knob were ignored.
        self.assertTrue(
            self._ask_across_sessions("sib_yes", {"traverse_sibling_sessions": True}),
            "a memory in another session was unreachable even with sibling traversal ON, so the "
            "check below cannot distinguish the knob working from retrieval being broken")
        self.assertFalse(
            self._ask_across_sessions("sib_no", {"traverse_sibling_sessions": False}),
            "a tenant that declined sibling sessions still had another session searched")


if __name__ == "__main__":
    unittest.main()
