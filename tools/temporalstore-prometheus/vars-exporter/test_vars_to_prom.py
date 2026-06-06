#!/usr/bin/env python3

from pathlib import Path
import tempfile
import unittest

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


if __name__ == "__main__":
    unittest.main()
