#!/usr/bin/env python3

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import vars_to_prom


class VarsToPromTest(unittest.TestCase):
    def test_parse_line_sanitizes_metric_and_labels(self):
        parsed = vars_to_prom.parse_line(
            'bcache2.server.request.latency{partition-id=7,role="primary"} : 12.5',
            "server",
        )

        self.assertEqual(
            parsed,
            (
                "bcache2_server_request_latency",
                12.5,
                'partition_id="7",role="primary",source="server"',
            ),
        )

    def test_write_prom_emits_help_type_once_per_metric(self):
        samples = {
            ("bcache2_server_cpu", 1.0, 'source="server"'),
            ("bcache2_server_cpu", 2.0, 'source="metaserver"'),
            ("bcache2_server_memory", 3.0, 'source="server"'),
        }

        with tempfile.TemporaryDirectory() as tmp:
            vars_to_prom.write_prom(Path(tmp), "temporalstore-vars.prom", samples)
            payload = Path(tmp, "temporalstore-vars.prom").read_text(encoding="utf-8")

        self.assertEqual(payload.count("# HELP bcache2_server_cpu "), 1)
        self.assertEqual(payload.count("# TYPE bcache2_server_cpu "), 1)
        self.assertIn('bcache2_server_cpu{source="server"} 1.0', payload)
        self.assertIn('bcache2_server_cpu{source="metaserver"} 2.0', payload)
        self.assertEqual(payload.count("# HELP bcache2_server_memory "), 1)

    def test_parse_line_does_not_split_host_port_metric_names(self):
        parsed = vars_to_prom.parse_line(
            "__metaserver_raft_peers_1,127.0.0.1:18010,127.0.0.1:18020,0 : 1",
            "metaserver",
        )

        self.assertEqual(parsed[0], "__metaserver_raft_peers_1_127_0_0_1:18010_127_0_0_1:18020_0")
        self.assertEqual(parsed[1], 1.0)

    def test_scrape_target_uses_curl_user_agent_for_brpc_plain_text(self):
        captured = {}

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return b"bthread_count : 16\r\n"

        def fake_urlopen(req, timeout):
            captured["user_agent"] = req.headers.get("User-agent")
            captured["accept"] = req.headers.get("Accept")
            captured["timeout"] = timeout
            return Response()

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            lines = vars_to_prom.scrape_target("metaserver", "http://127.0.0.1:18000/vars", 2.5)

        self.assertEqual(lines, ["bthread_count : 16"])
        self.assertEqual(captured["user_agent"], "curl/7.81.0")
        self.assertEqual(captured["accept"], "text/plain")


if __name__ == "__main__":
    unittest.main()
