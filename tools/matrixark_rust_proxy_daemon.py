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
        self._maybe_start_startup_warmup()
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
        self._maybe_start_startup_warmup()

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

    def _health_ok(self) -> bool:
        """Whether this socket can serve the next request.

        Deliberately lock-free: a health check must not queue behind an in-flight request,
        or a cold load turns every ping into a timeout -- which callers read as "no daemon"
        and answer by starting their own. A live engine is healthy; no engine is still
        healthy, because `_ensure_proxy` starts one on the next request. The one condition
        worth reporting is an engine that cannot be started at all.
        """
        proc = self._proc
        if proc is not None and proc.poll() is None:
            return True
        return os.access(str(self.proxy_path), os.X_OK)

    def _ensure_proxy(self) -> None:
        proc = self._proc
        if proc is not None and proc.poll() is None:
            return
        self._write_log({"event": "proxy_restarting", "returncode": None if proc is None else proc.returncode})
        self._stop_proxy()
        self._start_proxy()

    @staticmethod
    def _env_disabled(value: str | None) -> bool:
        return (value or "").strip().lower() in {"0", "false", "no", "off"}

    @staticmethod
    def _env_enabled(value: str | None) -> bool:
        return (value or "").strip().lower() in {"1", "true", "yes", "on"}

    @classmethod
    def _startup_warmup_allowed(cls) -> bool:
        setting = os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP", "auto")
        if cls._env_disabled(setting):
            return False
        if cls._env_enabled(setting):
            return True
        local_values = " ".join(
            os.environ.get(name, "")
            for name in (
                "MATRIXARK_LOCAL_MODE",
                "MATRIXARK_TEMPORALSTORE_METASERVER",
                "TEMPORALSTORE_METASERVER",
            )
        ).lower()
        if any(token in local_values for token in ("no-metaserver", "local", "single", "single-node", "single_node")):
            return True
        mode_values = " ".join(
            os.environ.get(name, "")
            for name in (
                "MATRIXARK_TEMPORALSTORE_MODE",
                "MATRIXARK_TEMPORALSTORE_STORAGE_MODE",
                "MATRIXARK_STORAGE_MODE",
                "MATRIXARK_HOOK_STORAGE_ROUTE",
                "TEMPORALSTORE_STORAGE_MODE",
            )
        ).lower()
        if any(token in mode_values for token in ("distributed", "replicated", "raft", "shared_store", "cluster", "production")):
            return False
        return True

    @staticmethod
    def _startup_warmup_storage_prefix() -> str:
        return (
            os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_PREFIX")
            or os.environ.get("MATRIXARK_STORAGE_PREFIX")
            or os.environ.get("MATRIXARK_TEMPORALSTORE_PREFIX")
            or "matrixark:codex-hook:rust-live-v2"
        )

    def _maybe_start_startup_warmup(self) -> None:
        if not self._startup_warmup_allowed():
            self._write_log({"event": "startup_warmup_skipped", "reason": "mode_gate"})
            return
        if getattr(self, "_startup_warmup_started", False):
            return
        self._startup_warmup_started = True
        threading.Thread(target=self._startup_warmup_loop, daemon=True).start()

    def _startup_warmup_loop(self) -> None:
        delay_ms = int(os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_DELAY_MS", "50") or "50")
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        storage_prefix = self._startup_warmup_storage_prefix()
        op = "matrixark_retrieve_context_pack_full_scan"
        if self._env_disabled(os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_FULL_SCAN", "1")):
            op = "matrixark_retrieve_context_pack"
        try:
            max_selected_refs = int(os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_MAX_SELECTED_REFS", "1") or "1")
        except ValueError:
            max_selected_refs = 1
        request: Json = {
            "op": op,
            "storage_prefix": storage_prefix,
            "count_key": f"{storage_prefix}:record_count",
            "record_hash_key": f"{storage_prefix}:records",
            "query": os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_QUERY", "__matrixark_startup_context_warmup__"),
            "max_selected_refs": max(1, max_selected_refs),
            "request_timeout_ms": 120000,
        }
        try:
            request["request_timeout_ms"] = int(os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_TIMEOUT_MS", "120000") or "120000")
        except ValueError:
            pass
        self._write_log({"event": "startup_warmup_started", "op": op, "storage_prefix": storage_prefix})
        started = time.monotonic()
        response = self._call_proxy(request)
        metrics = response.get("retrieval_metrics") if isinstance(response, dict) else None
        metrics = metrics if isinstance(metrics, dict) else {}
        self._write_log(
            {
                "event": "startup_warmup_completed",
                "ok": bool(response.get("ok", False)) if isinstance(response, dict) else False,
                "elapsed_ms": int((time.monotonic() - started) * 1000),
                "storage_prefix": storage_prefix,
                "op": op,
                "candidate_cache_hit": metrics.get("candidate_cache_hit"),
                "context_pack_cache_hit": metrics.get("context_pack_cache_hit"),
                "serving_memory_cache_layer": metrics.get("serving_memory_cache_layer"),
                "serving_memory_promoted": metrics.get("serving_memory_promoted"),
                "serving_memory_promoted_record_count": metrics.get("serving_memory_promoted_record_count"),
                "error": response.get("error") if isinstance(response, dict) else "invalid_response",
            }
        )

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
                        # Health is about whether this socket will serve the next request,
                        # not about whether an engine happens to be running right now. The
                        # engine is restarted on demand (`_ensure_proxy`), and callers treat
                        # an unhealthy answer as "no daemon" and start their OWN -- which
                        # unlinks this socket on bind and leaves them reloading the store
                        # per invocation. Reporting "dead" during a recycle is what turns a
                        # one-second gap into a spawn storm, so spawn and answer for the
                        # socket. Starting the process is cheap: the store is loaded lazily
                        # on the first real request, not here.
                        "ok": self._health_ok(),
                        "mode": "rust_proxy_daemon",
                        "proxy_pid": None if self._proc is None else self._proc.pid,
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


def ping_timeout_seconds() -> float:
    """How long a health check waits before calling the daemon dead.

    Callers treat a failed ping as "no daemon": the hooks then start one, and a second
    daemon unlinks the first one's socket on bind, so the loser cold-spawns its own
    proxy and replays the store per invocation. A fixed 2 s made that happen on a merely
    busy box -- a ping measured 2.4 s here under load, with the daemon perfectly
    healthy. Bounded, but generous enough that "busy" is not read as "dead".
    """
    raw = os.environ.get("MATRIXARK_RUST_PROXY_PING_TIMEOUT_MS", "10000")
    try:
        return max(0.5, int(str(raw).strip()) / 1000.0)
    except ValueError:
        return 10.0


def ping(socket_path: Path) -> Json:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(ping_timeout_seconds())
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
