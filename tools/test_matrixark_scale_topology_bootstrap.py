#!/usr/bin/env python3
from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_matrixark_cpp_rust_scale_report as scale


def _args(**overrides):
    values = {
        "metaserver": "127.0.0.1:18000",
        "no_auto_start_local_topology": False,
        "local_topology_start_timeout_sec": 5,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class MatrixArkScaleTopologyBootstrapTest(unittest.TestCase):
    def test_skips_non_loopback_metaserver(self) -> None:
        result = scale.ensure_local_topology(_args(metaserver="10.0.0.9:18000"))

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "non_loopback_metaserver")

    def test_skips_when_metaserver_is_already_reachable(self) -> None:
        with patch.object(scale, "metaserver_reachable", return_value={"ok": True}):
            result = scale.ensure_local_topology(_args())

        self.assertEqual(result["status"], "already_ready")

    def test_starts_canonical_release_for_missing_loopback_topology(self) -> None:
        with tempfile.TemporaryDirectory(prefix="matrixark-topology-bootstrap-") as tmpdir:
            root = Path(tmpdir)
            deploy = root / "tools" / "deploy_local_ubuntu22.sh"
            deploy.parent.mkdir(parents=True)
            deploy.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
            lib = root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"
            lib.parent.mkdir(parents=True)
            lib.write_text("", encoding="utf-8")

            class Completed:
                returncode = 0
                stdout = "TemporalStore local deployment is running\n"
                stderr = ""

            with patch.object(scale, "CANONICAL_UBUNTU_REPO", root), patch.object(
                scale,
                "metaserver_reachable",
                side_effect=[{"ok": False, "error": "refused"}, {"ok": True}],
            ), patch.object(scale.subprocess, "run", return_value=Completed()) as run:
                result = scale.ensure_local_topology(_args())

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["returncode"], 0)
        self.assertEqual(run.call_args.args[0], ["bash", str(deploy), "start"])
        self.assertEqual(run.call_args.kwargs["cwd"], root)
        self.assertEqual(run.call_args.kwargs["env"]["OUT_DIR"], str(root / "output-ubuntu22" / "release"))


if __name__ == "__main__":
    unittest.main()
