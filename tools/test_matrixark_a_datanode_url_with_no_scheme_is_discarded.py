#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""A datanode URL with no scheme is thrown away, host and port, and nothing said so.

`urlparse("datanode:9000")` reads `datanode` as the SCHEME and leaves the netloc empty, so both
`hostname` and `port` come back empty and the defaults win:

    configured  datanode:9000   ->  the gateway talks to 127.0.0.1:17102

Not a failure anybody sees. `127.0.0.1:17102` is a plausible address, so on a box where something
answers there the deployment appears to work against the wrong datanode, and where nothing answers
the checklist reports "could not connect to the datanode" and sends the reader to check that their
datanode process is running -- which it is, at the address they configured and the gateway never
used.

The fallback is deliberately unchanged. Pointing the connection at the host the operator wrote
would move live traffic on the strength of a string that has never parsed; saying where the calls
are actually going is the part that is unambiguously right, and it makes the misconfiguration
obvious in one line.

The parse lives in one function now. It was three lines inside `GatewayConfig.__init__`, and the
snapshot needed the same answer -- a second copy would have been a second thing to be wrong.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

TOOLS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TOOLS)

import matrixark_v1_gateway as gw  # noqa: E402


def snapshot(datanode_url=None) -> dict:
    script = ("import json, matrixark_v1_gateway as gw;"
              "print(json.dumps(gw._model_config_snapshot()))")
    environ = dict(os.environ)
    environ["MATRIXARK_RUNTIME_CONFIG_FILE"] = "/nonexistent/matrixark-datanode-test.json"
    if datanode_url is None:
        environ.pop("MATRIXARK_DATANODE_URL", None)
    else:
        environ["MATRIXARK_DATANODE_URL"] = datanode_url
    out = subprocess.run([sys.executable, "-c", script], cwd=TOOLS, env=environ,
                         capture_output=True, text=True, timeout=300)
    if out.returncode != 0:
        raise AssertionError(out.stderr[-800:])
    return json.loads(out.stdout)


def warned(snap: dict) -> str:
    hits = [w for w in snap.get("warnings", []) if "Datanode URL" in w]
    return hits[0] if hits else ""


class TheParseIsOneFunctionTest(unittest.TestCase):
    """`GatewayConfig` and the snapshot need the same answer; two copies is two answers."""

    def test_a_url_with_a_scheme_is_read_whole(self) -> None:
        self.assertEqual(("http", "datanode", 9000, True),
                         gw.datanode_target("http://datanode:9000"))

    def test_a_url_with_no_scheme_yields_no_host(self) -> None:
        scheme, host, port, usable = gw.datanode_target("datanode:9000")
        self.assertFalse(usable, "the defect is not reproduced")
        self.assertEqual("127.0.0.1", host)
        self.assertEqual(17102, port)

    def test_https_keeps_its_own_default_port(self) -> None:
        self.assertEqual(("https", "dn", 443, True), gw.datanode_target("https://dn"))

    def test_an_empty_url_is_the_local_default(self) -> None:
        self.assertEqual(("http", "127.0.0.1", 17102, False), gw.datanode_target(""))

    def test_a_malformed_port_does_not_raise(self) -> None:
        """`urlparse().port` raises on a bad port rather than returning None, and this runs while
        a request is being served."""
        scheme, host, port, usable = gw.datanode_target("http://dn:notaport")
        self.assertEqual(17102, port)
        self.assertTrue(usable)

    def test_the_config_object_uses_it(self) -> None:
        """The whole point of naming it: the connection and the report agree by construction."""
        cfg = gw.GatewayConfig(datanode_url="datanode:9000")
        self.assertEqual("127.0.0.1", cfg.blob_host)
        self.assertEqual(17102, cfg.blob_port)
        self.assertFalse(cfg.datanode_url_usable)


class TheSnapshotSaysWhereTheCallsGoTest(unittest.TestCase):

    def test_it_reports_both_the_configured_and_the_effective_address(self) -> None:
        block = snapshot("datanode:9000")["datanode_address"]
        self.assertEqual("datanode:9000", block["configured"])
        self.assertIn("127.0.0.1:17102", block["effective"])
        self.assertFalse(block["usable"])

    def test_a_good_url_reports_itself(self) -> None:
        block = snapshot("http://datanode:9000")["datanode_address"]
        self.assertEqual("http://datanode:9000", block["effective"])
        self.assertTrue(block["usable"])

    def test_the_warning_names_the_value_the_address_and_the_remedy(self) -> None:
        text = warned(snapshot("datanode:9000"))
        self.assertIn("datanode:9000", text)
        self.assertIn("127.0.0.1:17102", text)
        self.assertIn("http://datanode:9000", text, "the remedy is not spelled out")

    def test_a_usable_url_is_not_warned_about(self) -> None:
        """The floor. A warning on every deployment is a warning nobody reads."""
        self.assertEqual("", warned(snapshot("http://datanode:9000")))

    def test_an_unset_url_is_not_warned_about(self) -> None:
        """Unset is the documented default, not a misconfiguration."""
        self.assertEqual("", warned(snapshot(None)))
        self.assertTrue(snapshot(None)["datanode_address"]["usable"])


if __name__ == "__main__":
    unittest.main()
