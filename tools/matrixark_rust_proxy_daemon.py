#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Long-lived MatrixArk Rust proxy daemon.

The Rust proxy binary speaks newline-delimited JSON on stdio in ``--serve``
mode. Hooks are short-lived, so spawning that binary from every hook loses warm
engine/cache state. This daemon keeps one Rust proxy process alive and exposes a
small Unix-socket JSON-lines bridge for hook/client processes.

A pipe has ONE reader, so that bridge must serialize every caller behind one lock --
and on a live one-box deployment that is the dominant cost of a request, not the
work:

    queue wait   n=44,200   p50 6,352 ms   p90 434,871 ms   p99 1,167,374 ms
    actual work  n=34,290   p50    44 ms   p90   2,554 ms   p99    24,280 ms

The median caller waits 6.4 s to do 44 ms of work. 9,910 requests spent their whole
budget queueing and were abandoned without being started.

``MATRIXARK_PROXY_DAEMON_HTTP=1`` starts the proxy in ``--serve-http`` mode instead
and bridges over HTTP, which removes the lock: the proxy serves concurrent callers
itself. The Unix socket stays exactly where it is, so no client changes and either
transport can be run on the same box for comparison.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any


Json = dict[str, Any]


_ARENA_TRIM_THRESHOLD_BYTES = 8388608
_LIBC_FOR_TRIM: Any = None
_LIBC_LOOKED_UP = False


def _libc_for_trim() -> Any:
    """glibc handle used to hand freed arenas back to the OS, or None where that is not a thing."""
    global _LIBC_FOR_TRIM, _LIBC_LOOKED_UP
    if not _LIBC_LOOKED_UP:
        _LIBC_LOOKED_UP = True
        try:
            import ctypes

            candidate = ctypes.CDLL("libc.so.6")
            candidate.malloc_trim  # raises AttributeError off glibc
            _LIBC_FOR_TRIM = candidate
        except Exception:  # noqa: BLE001 - trimming is an optimisation, never a requirement.
            _LIBC_FOR_TRIM = None
    return _LIBC_FOR_TRIM


def release_arenas_after_large_payload(payload_bytes: int) -> bool:
    """Return freed heap to the OS after a big request or response.

    This process is a bridge that stores nothing, yet it was observed holding 2.9 GB while the
    engine it fronts held 1.1 GB. A whole payload is encoded and decoded here, so a large one
    leaves arenas CPython has freed but never returns -- measured at 124 MB resident from a 105 MB
    document, falling to 12 MB after malloc_trim(0).

    Bounded to large payloads on purpose: malloc_trim walks the arenas, so running it for every
    small request would spend time on the hot path reclaiming nothing.
    """
    if _ARENA_TRIM_THRESHOLD_BYTES == 0 or payload_bytes < _ARENA_TRIM_THRESHOLD_BYTES:
        return False
    libc = _libc_for_trim()
    if libc is None:
        return False
    try:
        libc.malloc_trim(0)
        return True
    except Exception:  # noqa: BLE001 - never let housekeeping break a served request.
        return False



class RustProxyDaemon:
    def __init__(self, *, proxy_path: Path, socket_path: Path, log_path: Path) -> None:
        self.proxy_path = proxy_path
        self.socket_path = socket_path
        self.log_path = log_path
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._proc: subprocess.Popen[str] | None = None
        self._log_file = None
        # Set when the proxy is serving HTTP; None means this daemon is on the stdio pipe.
        self._http_addr: tuple[str, int] | None = None
        # Where the address is published, so a client can skip this daemon entirely.
        self.http_addr_path = socket_path.with_suffix(socket_path.suffix + ".http")

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
        startup_warmup_allowed = self._startup_warmup_allowed()
        if startup_warmup_allowed:
            # Local hook mode should not block first serving on page-cache warming.
            # The daemon starts a background warmup immediately after the proxy is live.
            env.setdefault("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD", "0")
        http_addr = self._http_listen_addr()
        if http_addr is not None:
            host, port = http_addr
            # Concurrency is the POINT, and it is a separate switch on the engine: HTTP mode
            # starts `concurrent=false` by default, which reproduces the pipe's serialization
            # on a different transport and would move the queue rather than remove it.
            env.setdefault("MATRIXARK_RUST_PROXY_HTTP_CONCURRENT", "1")
            self._proc = subprocess.Popen(
                [str(self.proxy_path), "--serve-http", f"{host}:{port}"],
                stdin=subprocess.DEVNULL,
                stdout=self._log_file or subprocess.DEVNULL,
                stderr=self._log_file or subprocess.DEVNULL,
                text=True,
                env=env,
            )
            self._http_addr = (host, port)
        else:
            self._http_addr = None
            self._proc = subprocess.Popen(
                [str(self.proxy_path), "--serve"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self._log_file or subprocess.DEVNULL,
                text=True,
                bufsize=1,
                env=env,
            )
        if self._http_addr is not None:
            self._await_http_ready()
            self._publish_http_addr()
        self._write_log(
            {
                "event": "proxy_started",
                "transport": "http" if self._http_addr else "stdio",
                "http_addr": None if self._http_addr is None else "%s:%d" % self._http_addr,
                "pid": self._proc.pid,
                "startup_warmup_allowed": startup_warmup_allowed,
                "eager_cache_warm_on_load": env.get("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD"),
            }
        )
        self._maybe_start_startup_warmup()

    @staticmethod
    def _http_listen_addr() -> "tuple[str, int] | None":
        """Where to serve HTTP, or None to stay on the stdio pipe.

        Off by default. The transport is well covered on its own
        (`test_the_gateway_talks_to_the_proxy_over_http` runs every case down BOTH transports and
        compares), but flipping the default changes how a live deployment is reached, and that is
        a deployment decision rather than a code one. `MATRIXARK_PROXY_DAEMON_HTTP=1` turns it on;
        `MATRIXARK_PROXY_DAEMON_HTTP_ADDR` overrides host:port.

        Port 0 asks the OS for a free one, which is the right default for a daemon that may share
        a box with other instances -- a fixed port turns a second daemon into a silent failure to
        bind.
        """
        if not RustProxyDaemon._env_enabled(os.environ.get("MATRIXARK_PROXY_DAEMON_HTTP")):
            return None
        raw = (os.environ.get("MATRIXARK_PROXY_DAEMON_HTTP_ADDR") or "127.0.0.1:0").strip()
        host, _, port = raw.rpartition(":")
        host = host or "127.0.0.1"
        try:
            port_number = int(port)
        except ValueError:
            port_number = 0
        if port_number == 0:
            probe = socket.socket()
            try:
                probe.bind((host, 0))
                port_number = probe.getsockname()[1]
            finally:
                probe.close()
        return (host, port_number)

    def _await_http_ready(self, timeout_s: float = 30.0) -> bool:
        """Block until the listener accepts, so the first caller does not race the bind.

        A connection refused here is not "the proxy is broken" -- it is "the proxy has not bound
        yet", and the two are indistinguishable to a caller. Waiting once at start is cheaper than
        making every caller handle a startup race.
        """
        if self._http_addr is None:
            return True
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            proc = self._proc
            if proc is not None and proc.poll() is not None:
                self._write_log({"event": "proxy_http_exited_before_ready", "rc": proc.returncode})
                return False
            probe = socket.socket()
            probe.settimeout(0.5)
            try:
                probe.connect(self._http_addr)
                return True
            except OSError:
                time.sleep(0.05)
            finally:
                probe.close()
        self._write_log({"event": "proxy_http_never_bound", "addr": "%s:%d" % self._http_addr})
        return False

    def _call_proxy_http(self, request: Json, started: float) -> Json:
        """One HTTP round trip, with NO daemon lock held.

        This is the whole point of the transport. The stdio path below takes `self._lock` because
        a pipe has one reader and one writer; HTTP does not, so concurrent callers reach the
        engine concurrently and the queue that dominated every request disappears. The engine's
        own `MATRIXARK_RUST_PROXY_HTTP_CONCURRENT` is what makes that true on its side, and
        `_start_proxy` sets it.
        """
        assert self._http_addr is not None
        budget_s = max(2.0, float(request.get("request_timeout_ms") or 60000) / 1000.0 + 2.0)
        body = json.dumps(request, separators=(",", ":")).encode("utf-8")
        conn = http.client.HTTPConnection(self._http_addr[0], self._http_addr[1], timeout=budget_s)
        try:
            conn.request("POST", "/", body=body,
                         headers={"Content-Type": "application/json",
                                  "Content-Length": str(len(body))})
            raw = conn.getresponse().read()
        except Exception as exc:  # noqa: BLE001 - bridge must fail closed into JSON
            self._write_log({"event": "proxy_http_call_error", "error": str(exc)})
            return {"ok": False, "error": str(exc)}
        finally:
            conn.close()
        try:
            response = json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": "proxy returned non-JSON over http: %s" % exc}
        if not isinstance(response, dict):
            return {"ok": False, "error": "proxy returned a non-object over http"}
        elapsed_ms = int((time.monotonic() - started) * 1000)
        response.setdefault("rust_proxy_daemon", True)
        response.setdefault("daemon_elapsed_ms", elapsed_ms)
        # Reported as zero rather than omitted: a reader charting queue wait across a transport
        # change needs the series to keep its shape, and "no queue" is the result, not missing data.
        response.setdefault("daemon_queue_wait_ms", 0)
        response.setdefault("daemon_work_ms", elapsed_ms)
        response.setdefault("daemon_transport", "http")
        return response

    def _publish_http_addr(self) -> None:
        """Write the address where a client can find it, and remove it when there is none.

        The client side of this already exists and has since #1364: `MATRIXARK_RUST_PROXY_HTTP`
        makes `MatrixArkRustProxyClient` talk to the proxy directly, with per-thread keep-alive
        connections, skipping this daemon and its lock. What was missing was any way to LEARN the
        address -- the port is chosen at start, so a launcher cannot hardcode it.

        A file beside the socket, because that is where a client already looks for this daemon.
        Written after the listener accepts, so its existence means connectable rather than
        intended.
        """
        if self._http_addr is None:
            self._unpublish_http_addr()
            return
        try:
            self.http_addr_path.write_text("%s:%d\n" % self._http_addr, encoding="utf-8")
        except OSError as exc:
            self._write_log({"event": "http_addr_publish_failed", "error": str(exc)})

    def _unpublish_http_addr(self) -> None:
        """Remove a stale address: pointing a client at a dead port is worse than no answer."""
        try:
            self.http_addr_path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            self._write_log({"event": "http_addr_unpublish_failed", "error": str(exc)})

    def _stop_proxy(self) -> None:
        proc = self._proc
        self._proc = None
        self._http_addr = None
        self._unpublish_http_addr()
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

        Deliberately lock-free. Taking the daemon lock here would queue the health check
        behind an in-flight request, and a cold load takes tens of seconds -- long enough
        that every ping times out and reads as a dead daemon, which is the very thing this
        answer exists to prevent. A live engine is healthy; no engine is still healthy,
        because `_ensure_proxy` starts one on the next request. The condition worth
        reporting is an engine that cannot be started at all.
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
        setting = (os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP", "").strip() or "auto")
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
            "query": (os.environ.get("MATRIXARK_RUST_PROXY_STARTUP_WARMUP_QUERY", "").strip() or "__matrixark_startup_context_warmup__"),
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
                self._send(
                    file,
                    {
                        # Health is about whether this socket will serve the next request,
                        # not about whether an engine happens to be running right now. The
                        # engine is restarted on demand by `_ensure_proxy`, and callers read
                        # an unhealthy answer as "no daemon" and start their OWN -- which
                        # unlinks this socket on bind and leaves them reloading the store per
                        # invocation. Reporting "dead" while the engine restarts is what
                        # turns a one-second gap into a spawn storm.
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
            # Both payloads are finished with here. Hand the arenas back before the next caller
            # arrives, rather than carrying this one's high-water mark for the process lifetime.
            release_arenas_after_large_payload(len(line))

    def _call_proxy(self, request: Json) -> Json:
        started = time.monotonic()
        if self._http_addr is not None:
            # No lock: see `_call_proxy_http`.
            self._ensure_proxy()
            if self._http_addr is not None:
                return self._call_proxy_http(request, started)
        with self._lock:
            lock_acquired = time.monotonic()
            waited_ms = int((lock_acquired - started) * 1000)
            self._ensure_proxy()
            proc = self._proc
            if proc is None or proc.stdin is None or proc.stdout is None:
                return {"ok": False, "error": "rust proxy is not available"}
            try:
                # The budget runs from ARRIVAL, not from winning the lock. One lock serializes
                # every caller, so a lock-relative deadline let this process spend a full minute
                # on an answer whose caller had already timed out -- holding that lock while the
                # next caller queued behind a result nobody would read, which made the next
                # caller more likely to be abandoned in turn.
                budget_s = max(2.0, float(request.get("request_timeout_ms") or 60000) / 1000.0 + 2.0)
                deadline = started + budget_s
                if time.monotonic() >= deadline:
                    # Already spent queueing. Free the lock now rather than start work that no
                    # one is waiting for.
                    self._write_log(
                        {
                            "event": "proxy_call_abandoned",
                            "op": request.get("op"),
                            "queue_wait_ms": waited_ms,
                            "budget_ms": int(budget_s * 1000),
                        }
                    )
                    return {
                        "ok": False,
                        "error": (
                            f"request spent its whole {budget_s:.1f}s budget queueing for the "
                            f"shared proxy ({waited_ms}ms); not started"
                        ),
                        "daemon_queue_wait_ms": waited_ms,
                        "daemon_abandoned": True,
                    }
                proc.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
                proc.stdin.flush()
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
                    held_ms = int((time.monotonic() - lock_acquired) * 1000)
                    response.setdefault("daemon_queue_wait_ms", waited_ms)
                    response.setdefault("daemon_work_ms", held_ms)
                    if waited_ms > 1000 or held_ms > 5000:
                        self._write_log(
                            {
                                "event": "proxy_call_slow",
                                "op": request.get("op"),
                                "queue_wait_ms": waited_ms,
                                "work_ms": held_ms,
                            }
                        )
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

    Callers treat a failed ping as "no daemon": they then start one, and a second daemon
    unlinks the first one's socket on bind, so the loser spawns its own proxy and reloads
    the store per invocation. A fixed 2 s made that happen on a merely busy box -- a ping
    measured 2.4 s under load with the daemon perfectly healthy. Bounded, but generous
    enough that "busy" is not read as "dead".
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
