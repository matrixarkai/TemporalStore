#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Process lifecycle helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any


Json = dict[str, Any]


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
    return lane["proc"]
