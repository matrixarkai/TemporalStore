#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The switch that decides whether anything is audited is one a customer can reach.

The portal's admin-key preset says the key it issues "Runs this portal: configuration, keys, usage
and the audit log". There is a scope for it, ``admin:audit``, and a tool that reads the rows back
with a tenant check on it. And ``MATRIXARK_AUDIT_MODE`` defaults to ``off``, so every record --
including the one the access layer writes each time somebody is refused at the tenant boundary --
is dropped before it reaches storage.

That switch was in neither half of the portal's configuration. Not in ``SETTINGS``, so it could not
be set; not in the read-only inventory either, so the deployment page did not report it. A customer
who needed an audit trail had no way to learn from this product that they did not have one.

Off is the right default -- audit records live in the main record log, so a read-heavy deployment
with this on grows the store without bound -- which is a reason to make the choice visible, not a
reason to hide it.

The label is ``restart``, and the two readers are why. ``matrixark_access`` reads the variable
inside ``_append_audit_record``, so the records it writes start on the next call. The MCP server
captures it into ``_audit_mode_default`` in its constructor, so its own auditing keeps whatever the
process was built with. Both halves are asserted here rather than argued: the AST audit in
``test_matrixark_gateway_config_audit`` decides the label from where the reads sit, and it could
only see the constructor once it was told a constructor runs once.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import matrixark_gateway_config as cfg
import matrixark_mcp_local_adapter as adapter_module
from matrixark_mcp_server import MatrixArkLocalAdapter, MatrixArkMcpServer, MatrixArkError

ADMIN_SCOPES = ["admin:account", "admin:user", "admin:api_key", "admin:audit", "portal:read"]
A = {"account_id": "acct_a", "tenant_id": "tenant_a"}
B = {"account_id": "acct_b", "tenant_id": "tenant_b"}


class TheSwitchIsInTheRegistryTest(unittest.TestCase):

    def setUp(self) -> None:
        self.setting = cfg.SETTINGS_BY_KEY.get("audit.mode")
        self.assertIsNotNone(self.setting, "the audit switch is not a setting at all")

    def test_it_drives_the_variable_the_code_reads(self) -> None:
        """A setting whose env name matches nothing is a control that does nothing."""
        self.assertEqual("MATRIXARK_AUDIT_MODE", self.setting.env)

    def test_it_still_defaults_to_off(self) -> None:
        """Making the choice visible must not make it for them: audit records go in the main
        record log, and a deployment that turns this on unaware grows the store without bound."""
        self.assertEqual("off", self.setting.default)

    def test_it_offers_the_modes_the_code_understands(self) -> None:
        self.assertEqual(["off", "async", "full", "sync"], list(self.setting.choices))

    def test_it_says_a_restart_is_needed(self) -> None:
        self.assertEqual("restart", self.setting.applies)

    def test_the_help_says_what_off_costs(self) -> None:
        """"off" reads like a neutral default. It means every refusal goes unrecorded."""
        self.assertIn("discards", self.setting.help)

    def test_a_customer_can_find_it(self) -> None:
        """An access control folded behind "advanced" is one the people who need it do not see."""
        group = cfg.GROUPS[self.setting.group]
        self.assertFalse(group.get("advanced"), group)
        self.assertTrue(group.get("title"))
        self.assertTrue(group.get("note"))


class WritingItThroughThePortalTakesEffectTest(unittest.TestCase):
    """End to end: the portal write reaches the process, and the records follow.

    A registry entry proves nothing on its own -- the point is whether setting it changes what is
    kept.
    """

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = Path(tmp.name) / "events.jsonl"

        # Both of these are process-global and both are restored however this ends. The config
        # file especially: a settings test that does not move it writes the real one.
        self._keep("MATRIXARK_RUNTIME_CONFIG_FILE", str(Path(tmp.name) / "runtime.json"))
        self._keep("MATRIXARK_AUDIT_MODE", "async")
        self.assertTrue(cfg.config_path().startswith(tmp.name),
                        "the config path did not move; this test would rewrite a live config")

        dev = self._server()
        self.admin_a = dev.call_tool("matrixark_admin_create_api_key",
                                     {"scope": A, **A, "role": "owner", "scopes": ADMIN_SCOPES})
        self.victim_b = dev.call_tool("matrixark_admin_create_api_key",
                                      {"scope": B, **B, "role": "service",
                                       "key_prefix": "sk_live", "scopes": ["context:ingest"]})
        dev.close(timeout_s=10.0)

    def _keep(self, name: str, value: str) -> None:
        previous = os.environ.get(name)

        def restore() -> None:
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous

        self.addCleanup(restore)
        os.environ[name] = value

    def _server(self, mode: str = "dev") -> MatrixArkMcpServer:
        return MatrixArkMcpServer(MatrixArkLocalAdapter(self.log), line_json=True,
                                  access_mode=mode)

    def _refusal_records(self) -> list:
        """One admin key reaching for another tenant, then whatever it left behind.

        The server is closed before reading: audit writes are buffered, and reading through the
        same handle can see an empty log and call it a missing record.
        """
        server = self._server("enforced")
        try:
            with self.assertRaises(MatrixArkError):
                server.call_tool("matrixark_admin_revoke_api_key",
                                 {"scope": A, **A, "api_key": self.admin_a["api_key"],
                                  "api_key_id": self.victim_b["api_key_id"]})
        finally:
            server.close(timeout_s=10.0)
        rows = []
        # A substring scan over the durable log, so it has to be handed decoded LINES: the
        # log is a stream of compressed blocks by default, and scanning the raw file would
        # match nothing and report an empty audit trail as a pass.
        for line in adapter_module._iter_shard_lines(self.log):
            if '"matrixark_audit_log"' in line:
                rows.append(line)
        return rows

    def test_setting_it_through_the_portal_starts_the_records(self) -> None:
        os.environ["MATRIXARK_AUDIT_MODE"] = "off"
        cfg.update({"audit.mode": "async"}, actor="test")
        self.assertEqual("async", os.environ["MATRIXARK_AUDIT_MODE"])
        self.assertTrue([r for r in self._refusal_records() if '"denied"' in r],
                        "the switch is on and the refusal was still not recorded")

    def test_the_write_is_reported_as_waiting_on_a_restart(self) -> None:
        """Half of it is live and half of it is not, so the honest report is the slower half."""
        result = cfg.update({"audit.mode": "async"}, actor="test")
        text = repr(result)
        self.assertIn("restart", text, text[:400])

    def test_turning_it_off_stops_them(self) -> None:
        """The direction that matters for the store: off has to mean nothing is written."""
        cfg.update({"audit.mode": "off"}, actor="test")
        self.assertEqual("off", os.environ["MATRIXARK_AUDIT_MODE"])
        self.assertEqual([], [r for r in self._refusal_records() if '"denied"' in r])

    def test_the_positive_control(self) -> None:
        """With it on, the very same call does leave a record -- so the check above is reading a
        log it can actually see records in."""
        cfg.update({"audit.mode": "async"}, actor="test")
        self.assertTrue(self._refusal_records(), "no audit records at all, on or off")


class WhyTheLabelSaysRestartTest(unittest.TestCase):
    """The claim behind ``applies: restart``, asserted rather than argued."""

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = Path(tmp.name) / "events.jsonl"
        previous = os.environ.get("MATRIXARK_AUDIT_MODE")

        def restore() -> None:
            if previous is None:
                os.environ.pop("MATRIXARK_AUDIT_MODE", None)
            else:
                os.environ["MATRIXARK_AUDIT_MODE"] = previous

        self.addCleanup(restore)

    def _server(self) -> MatrixArkMcpServer:
        return MatrixArkMcpServer(MatrixArkLocalAdapter(self.log), line_json=True,
                                  access_mode="dev")

    def test_the_server_takes_it_at_construction(self) -> None:
        os.environ["MATRIXARK_AUDIT_MODE"] = "async"
        server = self._server()
        self.addCleanup(lambda: server.close(timeout_s=10.0))
        self.assertEqual("async", server._audit_mode_default)

    def test_and_never_looks_again(self) -> None:
        """This is the whole reason the label is not "live": a running process keeps what it was
        built with, so a portal write reaches the access layer and not this."""
        os.environ["MATRIXARK_AUDIT_MODE"] = "async"
        server = self._server()
        self.addCleanup(lambda: server.close(timeout_s=10.0))
        os.environ["MATRIXARK_AUDIT_MODE"] = "off"
        self.assertEqual("async", server._audit_mode_default,
                         "it re-read the variable, so the setting could be labelled live")


if __name__ == "__main__":
    unittest.main()
