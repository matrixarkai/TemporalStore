#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import types
import unittest


ROOT = Path(__file__).resolve().parents[1]
RENDERER_PATH = ROOT / "tools" / "temporalstore-monitoring-ui" / "render_health_from_results.py"


def load_renderer():
    spec = importlib.util.spec_from_file_location("render_health_from_results", RENDERER_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class RenderHealthFromResultsTest(unittest.TestCase):
    def test_production_gap_csv_clears_component_pending_states(self):
        renderer = load_renderer()
        with tempfile.TemporaryDirectory() as tmp:
            result_dir = Path(tmp)
            (result_dir / "gaps.csv").write_text(
                "\n".join(
                    [
                        "id,title,level,status,owner_gate",
                        '01,"Metaserver failover during membership change",pr,covered,"gate"',
                        '02,"Data primary kill during scale up/down",pr,covered,"gate"',
                        '03,"Removed data-node restart/rejoin safety",pr,covered,"gate"',
                        '04,"Removed metaserver restart/rejoin safety",pr,covered,"gate"',
                        '06,"Live raft gauges from data node/metaserver",quick,covered,"gate"',
                        '07,"Client/proxy retry metrics",quick,covered,"gate"',
                        '08,"Ingestion queue/backpressure metrics",quick,covered,"gate"',
                        '09,"Cache/storage fallback metrics",quick,covered,"gate"',
                        '10,"5-node data raft local scale",nightly,covered,"gate"',
                        '11,"3-replica sustained write/read lag",pr,covered,"gate"',
                        '13,"Metaserver snapshot restore gate",pr,covered,"gate"',
                        '18,"Prometheus alert rules",quick,covered,"gate"',
                        '19,"CI full-gate mode for Docker Prometheus",quick,covered,"gate"',
                        '20,"Build/test CI with dependency cache",quick,covered,"gate"',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            args = types.SimpleNamespace(
                result_dir=str(result_dir),
                template="",
                production_gap_csv="",
                cluster_name="local",
                environment="local",
                metaservers=3,
                data_nodes=5,
                shared_store="",
                blockcache="",
                replication_mode="raft",
                secondary_lag_ms="-",
                replay_source="raft",
                temporalaggregate_qps="-",
                temporalaggregate_p50_ms="-",
                temporalaggregate_p99_ms="-",
                release_build="pending",
            )

            health = renderer.build_health(args)

        self.assertEqual("ok", health["health"]["metaserver"]["status"])
        self.assertEqual("ok", health["health"]["proxy"]["status"])
        self.assertEqual("ok", health["health"]["exporter"]["status"])
        self.assertEqual("ok", health["health"]["data_nodes"]["status"])
        self.assertEqual("ok", health["health"]["blockcache"]["status"])
        self.assertEqual("ok", health["diagnostics"]["release_build"])
        self.assertEqual("ok", health["diagnostics"]["proxy_sdk"])
        self.assertEqual("ok", health["diagnostics"]["direct_sdk"])


if __name__ == "__main__":
    unittest.main()
