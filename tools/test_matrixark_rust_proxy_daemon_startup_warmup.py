# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DAEMON = REPO_ROOT / "tools" / "matrixark_rust_proxy_daemon.py"


class RustProxyDaemonStartupWarmupTest(unittest.TestCase):
    def _write_fake_proxy(self, root: Path) -> Path:
        proxy = root / "fake_proxy.py"
        proxy.write_text(
            """#!/usr/bin/env python3
import json
import os
import sys

if "--serve" not in sys.argv:
    raise SystemExit(2)
for line in sys.stdin:
    req = json.loads(line or "{}")
    if req.get("op", "").startswith("matrixark_retrieve_context_pack"):
        print(json.dumps({
            "ok": True,
            "retrieval_metrics": {
                "candidate_cache_hit": False,
                "context_pack_cache_hit": False,
                "serving_memory_cache_layer": "durable_read_through",
                "serving_memory_promoted": True,
                "serving_memory_promoted_record_count": 7
            },
            "eager_cache_warm_on_load": os.environ.get("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD")
        }), flush=True)
    else:
        print(json.dumps({"ok": True, "echo_op": req.get("op")}), flush=True)
""",
            encoding="utf-8",
        )
        proxy.chmod(0o755)
        return proxy

    def _send(self, socket_path: Path, payload: dict) -> dict:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2.0)
            client.connect(str(socket_path))
            client.sendall((json.dumps(payload) + "\n").encode("utf-8"))
            return json.loads(client.makefile("rb").readline().decode("utf-8"))

    def test_startup_warmup_promotes_context_without_blocking_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            proxy = self._write_fake_proxy(root)
            socket_path = root / "proxy.sock"
            log_path = root / "daemon.log"
            env = dict(os.environ)
            env.update(
                MATRIXARK_RUST_PROXY_STARTUP_WARMUP="1",
                MATRIXARK_RUST_PROXY_STARTUP_WARMUP_DELAY_MS="0",
                MATRIXARK_RUST_PROXY_STARTUP_WARMUP_PREFIX="matrixark:test:warmup",
            )
            proc = subprocess.Popen(
                ["python3", str(DAEMON), "--proxy", str(proxy), "--socket", str(socket_path), "--log", str(log_path)],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                deadline = time.time() + 5
                while time.time() < deadline:
                    if socket_path.exists():
                        try:
                            health = self._send(socket_path, {"op": "__daemon_health"})
                            if health.get("ok"):
                                break
                        except Exception:
                            pass
                    time.sleep(0.05)
                else:
                    self.fail("daemon socket did not become healthy")

                deadline = time.time() + 5
                log_text = ""
                while time.time() < deadline:
                    log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
                    if "startup_warmup_completed" in log_text:
                        break
                    time.sleep(0.05)
                self.assertIn("startup_warmup_completed", log_text)
                self.assertIn('"startup_warmup_allowed":true', log_text)
                self.assertIn('"eager_cache_warm_on_load":"0"', log_text)
                self.assertIn('"serving_memory_promoted":true', log_text)
                self.assertIn('"serving_memory_promoted_record_count":7', log_text)
                response = self._send(socket_path, {"op": "user_request"})
                self.assertTrue(response.get("ok"), response)
            finally:
                if socket_path.exists():
                    try:
                        self._send(socket_path, {"op": "__daemon_shutdown"})
                    except Exception:
                        pass
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()


if __name__ == "__main__":
    unittest.main()
