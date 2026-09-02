#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""A knob marked "live" must change the deployment without a restart -- checked by doing it.

`test_matrixark_gateway_config_audit` already scans for env reads captured at import and requires
those settings to be marked `restart`. That is a source check, and it answers a narrower question
than the portal's claim: "applies live" says the running deployment changes behaviour, and a knob
with no import-time read can still fail that if a value is cached anywhere between the registry and
the writer -- a module constant built from a call, a memoised resolver, a config object built once
per process.

So this changes each knob in a LIVE process -- same server object, same adapter, no re-import, no
restart -- and compares what is actually stored before and after.

Reading the counts needs one piece of care. `read_all()` is cumulative, so turning a knob ON shows
as a RISE; turning one OFF shows as "did not rise", and it may legitimately FALL, because a
re-appended owner record supersedes its earlier copy and the superseded version had the vector. An
earlier version of this measurement demanded no movement at all for the OFF case and reported a
knob that works perfectly as needing a restart.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matrixark_tenant_policy as policy  # noqa: E402


def _counts(adapter, tenant):
    """Count DISTINCT records, not log entries.

    `read_all()` returns an append log, so one segment can appear more than once when its owner is
    re-appended -- a supersede, not a new segment. Counting raw entries made this test report a
    segment written after the knob was turned off, roughly one run in four. The record was the same
    `segment_hash` both times; nothing new had been written and no knob had failed.
    """
    identities = {tenant, policy.tenant_hash_of(tenant)}
    segments, events_with_summary, vectored = set(), set(), set()
    total = 0
    for record in adapter.read_all():
        scope = (record.get("scope") or record.get("access_scope") or record.get("scope_key"))
        if policy.tenant_of(scope) not in identities:
            continue
        total += 1
        kind = str(record.get("record_type"))
        if kind == "context_segment":
            segments.add(record.get("segment_hash"))
        if record.get("vector"):
            vectored.add((kind, record.get("segment_hash") or record.get("event_id_hash")
                          or record.get("entity_hash") or record.get("node_hash") or id(record)))
        if kind == "context_event" and "summary_text" in record:
            events_with_summary.add(record.get("event_id_hash") or id(record))
    return {"segments": len(segments), "vectors": len(vectored),
            "summary_text": len(events_with_summary), "total": total}


def _change_mid_flight(knob, first, second, tenant):
    """Ingest under `first`, flip the knob in-process, ingest again. Returns (before, after)."""
    import matrixark_mcp_server as mcp

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        adapter = mcp.MatrixArkLocalAdapter(Path(tmp) / "memory.jsonl")
        server = mcp.MatrixArkMcpServer(adapter, access_mode="dev")

        # Each phase gets its OWN session. Ingest buffers and session_commit flushes, so work
        # extracted while the knob was ON can land after it is turned off -- that is deferred work
        # completing under the setting in force when it was buffered, not the knob failing to
        # apply. Sharing one session made this test fail roughly one run in five with
        # "segments still rose (1 -> 2)", which reads as a stuck knob and is not one.
        def ingest(text, session):
            scope = {"tenant_id": tenant, "user_id": "u1", "session_id": session}
            server.call_tool("matrixark_ingest", {
                "scope": scope, "finalize": True,
                "messages": [{"role": "user", "content": text}]})
            server.call_tool("matrixark_session_commit", {"scope": scope})

        # The two phases ingest STRUCTURALLY IDENTICAL text, differing only by a trailing marker
        # so the second is not deduplicated against the first. An earlier version used two
        # different sentences, which made the ON direction depend on the extractor happening to
        # segment that particular sentence -- the test passed almost always and failed
        # occasionally, which is worse than not having it.
        base = ("I am allergic to peanuts and I live in Kyoto. "
                "My favourite drink is matcha and I bike to work. "
                "I work on the storage team and my manager is Dana.")

        policy.set_tenant_policy(tenant, {knob: first})
        ingest(base + " (first)", "phase-one")
        before = _counts(adapter, tenant)

        # The change under test. Nothing is restarted, re-imported, or rebuilt.
        policy.set_tenant_policy(tenant, {knob: second})
        ingest(base + " (second)", "phase-two")
        after = _counts(adapter, tenant)
    return before, after


class KnobsApplyWithoutARestartTest(unittest.TestCase):
    """Each wired storage knob, flipped in a running process."""

    def _assert_live(self, knob, field, first, second):
        tenant = "live_%s_%s" % (knob, str(second).lower())
        before, after = _change_mid_flight(knob, first, second, tenant)
        self.assertGreater(after["total"], before["total"],
                           "the second ingest stored nothing, so this proves nothing about %s"
                           % knob)
        moved = after[field] - before[field]
        if second:
            # Both phases ingest the same structure, so the first phase having produced some of
            # this record kind proves the extractor CAN produce it -- which separates "the knob
            # did not apply" from "there was nothing to write either way".
            self.assertGreater(
                moved, 0,
                "%s was turned ON mid-flight and %s did not rise (%d -> %d). Both phases ingest "
                "the same text, so the extractor is not the variable here: the change needs a "
                "restart, and the portal must not report it as live"
                % (knob, field, before[field], after[field]))
        else:
            self.assertLessEqual(
                moved, 0,
                "%s was turned OFF mid-flight and %s still rose (%d -> %d): the process kept "
                "writing what the tenant declined"
                % (knob, field, before[field], after[field]))

    def test_generate_embeddings_applies_both_ways(self) -> None:
        self._assert_live("generate_embeddings", "vectors", True, False)
        self._assert_live("generate_embeddings", "vectors", False, True)

    def test_extract_segments_applies_both_ways(self) -> None:
        self._assert_live("extract_segments", "segments", False, True)
        self._assert_live("extract_segments", "segments", True, False)

    def test_store_event_summary_text_applies_both_ways(self) -> None:
        self._assert_live("store_event_summary_text", "summary_text", False, True)
        self._assert_live("store_event_summary_text", "summary_text", True, False)


class WhatThePortalClaimsIsWhatHappensTest(unittest.TestCase):
    """The portal prints "applies live" or "needs a restart" per setting. It has to be right.

    The audit next door proves no import-time read exists. This proves the other direction for the
    knobs that are actually wired: what the portal claims matches what the process does.
    """

    def _setting_for(self, env):
        import matrixark_gateway_config as gwconfig
        for setting in gwconfig.SETTINGS:
            if setting.env == env:
                return setting
        return None

    def test_the_wired_storage_knobs_are_advertised_live(self) -> None:
        for env in ("MATRIXARK_GENERATE_EMBEDDINGS", "MATRIXARK_EXTRACT_SEGMENTS",
                    "MATRIXARK_STORE_EVENT_SUMMARY_TEXT"):
            with self.subTest(env=env):
                setting = self._setting_for(env)
                self.assertIsNotNone(setting, "%s is not offered to the portal at all" % env)
                self.assertEqual(
                    "live", setting.applies,
                    "%s is advertised as %r, but flipping it mid-flight does change what is "
                    "stored -- the tests above measure that" % (env, setting.applies))

    def test_a_setting_frozen_at_import_is_advertised_as_needing_a_restart(self) -> None:
        # The other half: the five that genuinely cannot apply live must say so, or a customer
        # changes one, sees nothing happen, and has no way to learn why.
        import matrixark_gateway_config as gwconfig
        frozen = gwconfig._KNOB_ENV_FROZEN_AT_IMPORT
        self.assertTrue(frozen, "the frozen-at-import list is empty, so this test is vacuous")
        for env in sorted(frozen):
            with self.subTest(env=env):
                setting = self._setting_for(env)
                if setting is None:
                    continue
                self.assertEqual("restart", setting.applies,
                                 "%s is captured at import but advertised as live" % env)


if __name__ == "__main__":
    unittest.main()
