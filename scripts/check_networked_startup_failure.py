#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Require a configured shared-store replay failure to stop before serving."""
import http.server
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time


class FailedStore(http.server.BaseHTTPRequestHandler):
    def reply(self, status, body=b""):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        self.reply(200)

    def do_GET(self):
        if self.path == "/healthz":
            self.reply(200, b"ok")
        else:
            self.reply(500, b"injected shared-store read failure")

    def log_message(self, *_args):
        pass


def main():
    server_binary = str(Path(sys.argv[1]).resolve())
    store = http.server.ThreadingHTTPServer(("127.0.0.1", 0), FailedStore)
    thread = threading.Thread(target=store.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    process = None
    try:
        with tempfile.TemporaryDirectory(prefix="shared-startup-failure-") as temporary:
            root = Path(temporary)
            env = {k: v for k, v in os.environ.items() if not k.startswith(("TS_", "MATRIXOBJECT_"))}
            env.update(
                TS_SERVER_BIND_ADDR=f"127.0.0.1:{port}",
                TS_META_ADDR="",
                TS_SHARD_ID="1",
                TS_STORAGE_BACKEND="raft-replication",
                TS_MATRIXOBJECT_MODE="gateway",
                TS_SHARED_STORE_URI=f"matrixobject://127.0.0.1:{store.server_port}",
                TS_CLUSTER_ID="startup-failure",
                TS_CACHE_DIR=str(root / "cache"),
                TS_BLOCK_STORE_DIR=str(root / "blocks"),
                TS_INDEX_DIR=str(root / "indexes"),
            )
            with (root / "server.log").open("w") as output:
                process = subprocess.Popen([server_binary], env=env, stdin=subprocess.DEVNULL,
                                           stdout=output, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 20
                accepted = False
                while process.poll() is None and time.monotonic() < deadline:
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                            accepted = True
                            break
                    except OSError:
                        time.sleep(0.05)
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
            log = (root / "server.log").read_text()
            if accepted or process.returncode == 0 or "refusing startup: configured networked durability" not in log:
                print(log)
                raise AssertionError("shared-store read failure did not refuse startup before listening")
            print("PASS: shared-store read failure refused startup before listening")
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=3)
        store.shutdown()
        store.server_close()
        thread.join(timeout=3)


if __name__ == "__main__":
    main()
