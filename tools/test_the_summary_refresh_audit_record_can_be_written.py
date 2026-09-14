# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Turning the summary-refresh audit on has to produce an audit record, not an exception.

`refresh_dirty_node_summaries` writes a `context_summary_refresh_audit` record under
`if ENABLE_SUMMARY_REFRESH_AUDIT:`, and that record read a `version_hash` which appeared exactly
once in the entire module -- as that read. Nothing bound it. So turning on
`MATRIXARK_SUMMARY_REFRESH_AUDIT`, a documented knob that defaults off, made the refresh raise
`NameError` at the moment it tried to describe what it had just done.

Nothing caught it because nothing here turns the flag on. Two tests in this suite do exercise that
line, but only when the flag is already set in the environment the suite happens to run in, and CI
does not set it -- so the branch was exercised on a developer's box or not at all.

This pins the flag itself rather than inheriting it, which is the whole point: a test that reads
the configuration of the box it runs on is not testing the configuration it claims to.

The value now comes from the same expression the sibling writer in `matrixark_mcp_summary_runtime`
uses, so the two copies of this record agree about what a summary version hash is.
"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest

TOOLS = pathlib.Path(__file__).resolve().parent
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

import matrixark_mcp_server as mcp  # noqa: E402
from matrixark_mcp_core import scope_key_from_hashes, stable_hash  # noqa: E402

AUDIT_FLAG = "ENABLE_SUMMARY_REFRESH_AUDIT"


class TheSummaryRefreshAuditRecordCanBeWrittenTest(unittest.TestCase):

    def _refresh_globals(self):
        """The namespace the refresh reads the flag from -- not a module named by guess."""
        return mcp.MatrixArkLocalAdapter.refresh_dirty_node_summaries.__globals__

    def _pin_audit(self, enabled: bool):
        namespace = self._refresh_globals()
        self.assertIn(AUDIT_FLAG, namespace,
                      "%s is not a global of the refresh; this test is pinning the wrong "
                      "namespace and would pass without exercising anything" % AUDIT_FLAG)
        previous = namespace[AUDIT_FLAG]
        namespace[AUDIT_FLAG] = enabled
        self.addCleanup(lambda: namespace.__setitem__(AUDIT_FLAG, previous))

    def _refresh_once(self, tmpdir: str):
        adapter = mcp.MatrixArkLocalAdapter(pathlib.Path(tmpdir) / "events.jsonl")
        scope = {"tenant_hash": 5150, "scope_key": scope_key_from_hashes(5150, 0, 0)}
        node_path = ["tenant:summary", "user:worker", "session:audit"]
        node_hash = stable_hash("/".join(node_path))
        common = {
            "node_hash": node_hash,
            "node_path": node_path,
            "scope": scope,
            "scope_key": scope["scope_key"],
            "tenant_hash": scope["tenant_hash"],
            "updated_at_ms": 1780000000000,
        }
        adapter.append(dict(common, record_type="context_node"))
        adapter.append(dict(common, record_type="context_event", event_id_hash=909,
                            text="A node with an event, so the refresh has something to do."))
        adapter.refresh_dirty_node_summaries(
            scope=scope, limit=8, refreshed_at_ms=1780000001000,
            max_raw_events_per_node=100, min_compression_event_age_ms=0)
        return [record for record in adapter.read_all()
                if record.get("record_type") == "context_summary_refresh_audit"]

    def test_the_flag_is_off_by_default(self) -> None:
        """The control on the default. If it ships on, the two tests below are describing the
        ordinary configuration rather than an opt-in one, and the risk is different."""
        import matrixark_mcp_runtime_config as runtime_config
        self.assertFalse(
            getattr(runtime_config, AUDIT_FLAG),
            "%s now defaults on. That is a bigger change than this file assumes -- the audit "
            "record is written on every refresh for everyone." % AUDIT_FLAG)

    def test_no_audit_record_when_the_flag_is_off(self) -> None:
        """The other half: with the knob off, nothing is written. Without this, a refresh that
        always wrote the record would satisfy the test below and nobody would notice."""
        self._pin_audit(False)
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertEqual(
                [], self._refresh_once(tmpdir),
                "an audit record was written with %s off" % AUDIT_FLAG)

    def test_an_audit_record_is_written_when_the_flag_is_on(self) -> None:
        self._pin_audit(True)
        with tempfile.TemporaryDirectory() as tmpdir:
            audits = self._refresh_once(tmpdir)
        self.assertTrue(
            audits,
            "%s is on and the refresh wrote no audit record. It used to raise NameError here, "
            "which the background worker swallows -- so the refresh reported nothing and left no "
            "trace either." % AUDIT_FLAG)
        for audit in audits:
            with self.subTest(node=audit.get("node_path")):
                self.assertIn("summary_version_hash", audit,
                              "the audit record carries no summary_version_hash")
                self.assertIsInstance(
                    audit["summary_version_hash"], int,
                    "summary_version_hash is %r, not the hash its sibling writer produces"
                    % (audit["summary_version_hash"],))

    def test_the_version_hash_distinguishes_two_refreshes_of_one_node(self) -> None:
        """A version hash that is the same for every refresh identifies nothing. The value is
        built from the node, the dirty marker and the refresh time, so two refreshes differ."""
        self._pin_audit(True)
        seen = set()
        for _ in range(2):
            with tempfile.TemporaryDirectory() as tmpdir:
                for audit in self._refresh_once(tmpdir):
                    seen.add(audit.get("summary_version_hash"))
        self.assertTrue(seen, "no audit records at all, so there is nothing to compare")
        self.assertNotIn(None, seen, "an audit record carried summary_version_hash=None")


if __name__ == "__main__":
    unittest.main()
