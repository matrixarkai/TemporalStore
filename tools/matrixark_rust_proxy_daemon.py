#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Long-lived MatrixArk Rust proxy daemon.

The Rust proxy binary speaks newline-delimited JSON on stdio in ``--serve``
mode. Hooks are short-lived, so spawning that binary from every hook loses warm
engine/cache state. This daemon keeps one Rust proxy process alive and exposes a
small Unix-socket JSON-lines bridge for hook/client processes.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


Json = dict[str, Any]


class RustProxyDaemon:
    def __init__(self, *, proxy_path: Path, socket_path: Path, log_path: Path) -> None:
        self.proxy_path = proxy_path
        self.socket_path = socket_path
        self.log_path = log_path
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._proc: subprocess.Popen[str] | None = None
        self._log_file = None

    def start(self) -> None:
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.socket_path.exists():
            self.socket_path.unlink()
        self._log_file = self.log_path.open("a", encoding="utf-8")
        self._start_proxy()
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(str(self.socket_path))
        server.listen(64)
        server.settimeout(0.5)
        self._write_log({"event": "daemon_started", "socket": str(self.socket_path), "proxy": str(self.proxy_path)})
        try:
            while not self._stop.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    self._ensure_proxy()
                    continue
                threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()
        finally:
            server.close()
            try:
                self.socket_path.unlink()
            except FileNotFoundError:
                pass
            self._stop_proxy()
            if self._log_file is not None:
                self._log_file.close()

    def stop(self) -> None:
        self._stop.set()

    def _start_proxy(self) -> None:
        env = os.environ.copy()
        proxy_dir = str(self.proxy_path.resolve().parent)
        old_ld = env.get("LD_LIBRARY_PATH", "")
        env["LD_LIBRARY_PATH"] = proxy_dir if not old_ld else f"{proxy_dir}:{old_ld}"
        self._proc = subprocess.Popen(
            [str(self.proxy_path), "--serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log_file or subprocess.DEVNULL,
            text=True,
            bufsize=1,
            env=env,
        )
        self._write_log({"event": "proxy_started", "pid": self._proc.pid})

    def _stop_proxy(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
        self._write_log({"event": "proxy_stopped", "returncode": proc.returncode})

    def _ensure_proxy(self) -> None:
        proc = self._proc
        if proc is not None and proc.poll() is None:
            return
        self._write_log({"event": "proxy_restarting", "returncode": None if proc is None else proc.returncode})
        self._stop_proxy()
        self._start_proxy()

    def _handle_conn(self, conn: socket.socket) -> None:
        with conn:
            file = conn.makefile("rwb")
            line = file.readline()
            if not line:
                return
            try:
                request = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError as exc:
                self._send(file, {"ok": False, "error": f"invalid json: {exc}"})
                return
            if request.get("op") == "__daemon_health":
                proc = self._proc
                self._send(
                    file,
                    {
                        "ok": bool(proc is not None and proc.poll() is None),
                        "mode": "rust_proxy_daemon",
                        "proxy_pid": None if proc is None else proc.pid,
                        "socket": str(self.socket_path),
                    },
                )
                return
            if request.get("op") == "__daemon_shutdown":
                self._send(file, {"ok": True, "status": "shutdown"})
                self.stop()
                return
            response = self._call_proxy(request)
            self._send(file, response)

    def _call_proxy(self, request: Json) -> Json:
        started = time.monotonic()
        with self._lock:
            self._ensure_proxy()
            proc = self._proc
            if proc is None or proc.stdin is None or proc.stdout is None:
                return {"ok": False, "error": "rust proxy is not available"}
            try:
                proc.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
                proc.stdin.flush()
                deadline = time.monotonic() + max(2.0, float(request.get("request_timeout_ms") or 60000) / 1000.0 + 2.0)
                while time.monotonic() < deadline:
                    line = proc.stdout.readline()
                    if not line:
                        if proc.poll() is not None:
                            return {"ok": False, "error": f"rust proxy exited: {proc.returncode}"}
                        continue
                    if not line.strip().startswith("{"):
                        continue
                    response = json.loads(line)
                    response.setdefault("rust_proxy_daemon", True)
                    response.setdefault("daemon_elapsed_ms", int((time.monotonic() - started) * 1000))
                    return response
                return {"ok": False, "error": "rust proxy daemon timed out waiting for proxy response"}
            except Exception as exc:  # noqa: BLE001 - bridge must fail closed into JSON.
                self._write_log({"event": "proxy_call_error", "error": str(exc)})
                self._stop_proxy()
                return {"ok": False, "error": str(exc)}

    @staticmethod
    def _send(file: Any, payload: Json) -> None:
        file.write((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
        file.flush()

    def _write_log(self, payload: Json) -> None:
        if self._log_file is None:
            return
        payload.setdefault("ts", time.time())
        self._log_file.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self._log_file.flush()


def ping(socket_path: Path) -> Json:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2.0)
        client.connect(str(socket_path))
        client.sendall(b'{"op":"__daemon_health"}\n')
        return json.loads(client.makefile("rb").readline().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--log", required=True)
    parser.add_argument("--ping", action="store_true")
    args = parser.parse_args()
    socket_path = Path(args.socket)
    if args.ping:
        try:
            print(json.dumps(ping(socket_path), separators=(",", ":")))
            return 0
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")))
            return 1

    daemon = RustProxyDaemon(proxy_path=Path(args.proxy), socket_path=socket_path, log_path=Path(args.log))
    signal.signal(signal.SIGTERM, lambda _signum, _frame: daemon.stop())
    signal.signal(signal.SIGINT, lambda _signum, _frame: daemon.stop())
    daemon.start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
