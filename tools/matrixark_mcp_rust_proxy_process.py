#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Process lifecycle helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import collections
import os
import subprocess
import threading
from pathlib import Path, PurePosixPath
from typing import Any


Json = dict[str, Any]

# Lines of proxy stderr kept per lane for error reporting. Only the tail is ever quoted (the
# error paths take the last 1000 characters), so this is generous.
PROXY_STDERR_TAIL_LINES = 200


def _drain_proxy_stderr(proc: subprocess.Popen[str], sink: collections.deque) -> None:
    """Continuously read the proxy's stderr so it can never block writing to it.

    The proxy is spawned with `stderr=subprocess.PIPE` and nothing read that pipe outside the
    error paths, which only run once the process has already exited or the stdin pipe has broken.
    A pipe holds 64KB. A proxy that logs past that blocks forever inside its next stderr write --
    it stops answering, sits near-idle because it is blocked rather than working, holds its lane,
    and never recovers. Downstream that shows up as one request hanging to its timeout and every
    later one rejected on the lane, which reads like a storage problem and is not one.

    The busier the proxy, the sooner it happens: work that makes it log more brings the wedge
    within a handful of requests.

    Draining into a bounded deque keeps the error paths working -- they quote the tail -- without
    keeping an unbounded log in memory. Daemon thread, so it never holds up interpreter exit.
    """
    stream = proc.stderr
    if stream is None:
        return
    # Debug seam: MATRIXARK_PROXY_STDERR_LOG=<path> mirrors the drained stream to a file, so a
    # proxy that stops answering can be read after the fact instead of guessed at.
    log_path = os.environ.get("MATRIXARK_PROXY_STDERR_LOG", "").strip()
    log = None
    if log_path:
        try:
            log = open(f"{log_path}.{proc.pid}", "a", encoding="utf-8", buffering=1)
        except OSError:
            log = None
    try:
        for line in iter(stream.readline, ""):
            sink.append(line)
            if log is not None:
                log.write(line)
    except Exception:  # noqa: BLE001 - a closed pipe on shutdown must not raise here
        pass
    finally:
        if log is not None:
            try:
                log.close()
            except OSError:
                pass


def proxy_stderr_tail(lane: Json | None, limit: int = 1000) -> str:
    """The drained stderr tail for a lane, for error messages."""
    if not isinstance(lane, dict):
        return ""
    sink = lane.get("stderr_tail")
    if not sink:
        return ""
    return "".join(sink)[-limit:]


def close_proxy_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        try:
            if stream is not None:
                stream.close()
        except Exception:
            pass


def close_proxy_lanes(target: Any) -> None:
    seen: set[int] = set()
    for lanes in getattr(target, "_lanes", {}).values():
        for lane in lanes:
            proc = lane.get("proc")
            lane["proc"] = None
            if proc is None or id(proc) in seen:
                continue
            seen.add(id(proc))
            close_proxy_process(proc)
    proc = getattr(target, "_proc", None)
    target._proc = None
    if proc is not None and id(proc) not in seen:
        close_proxy_process(proc)


def _library_parent(path: str) -> str:
    if path.startswith("/"):
        return str(PurePosixPath(path).parent)
    return str(Path(path).resolve().parent)


def rust_proxy_library_search_path(cli_path: str, env: Json | None = None) -> str:
    source_env = os.environ if env is None else env
    paths: list[str] = [_library_parent(cli_path)]
    temporalstore_lib = str(source_env.get("TEMPORALSTORE_LIB") or "").strip()
    if temporalstore_lib:
        lib_dir = _library_parent(temporalstore_lib)
        if lib_dir not in paths:
            paths.append(lib_dir)
    existing_ld_path = str(source_env.get("LD_LIBRARY_PATH") or "").strip()
    for item in existing_ld_path.split(":"):
        if item and item not in paths:
            paths.append(item)
    return ":".join(paths)


def ensure_lane_process(target: Any, lane: Json) -> subprocess.Popen[str]:
    proc = lane.get("proc")
    if proc is not None and proc.poll() is None:
        return proc
    if proc is not None:
        close_proxy_process(proc)
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = rust_proxy_library_search_path(target.cli_path, env)
    if env.get("MATRIXARK_RUST_PROXY_NATIVE_MATRIXARK_C_API_COMPAT", "0").strip().lower() in {"1", "true", "yes", "on"}:
        env.setdefault("TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API", "1")
    lane["proc"] = subprocess.Popen(
        [target.cli_path, "--serve"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env=env,
    )
    # Start draining stderr immediately -- see _drain_proxy_stderr. Without this the proxy
    # deadlocks on its own log output once it has written a pipe buffer's worth.
    sink: collections.deque = collections.deque(maxlen=PROXY_STDERR_TAIL_LINES)
    lane["stderr_tail"] = sink
    drain = threading.Thread(
        target=_drain_proxy_stderr,
        args=(lane["proc"], sink),
        name="matrixark-proxy-stderr-drain",
        daemon=True,
    )
    lane["stderr_drain"] = drain
    drain.start()
    return lane["proc"]
