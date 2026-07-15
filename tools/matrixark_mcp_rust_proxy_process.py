#!/usr/bin/env python3
"""Process lifecycle helpers for the MatrixArk Rust proxy client."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
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


def ensure_lane_process(target: Any, lane: Json) -> subprocess.Popen[str]:
    proc = lane.get("proc")
    if proc is not None and proc.poll() is None:
        return proc
    if proc is not None:
        close_proxy_process(proc)
    env = os.environ.copy()
    proxy_dir = str(Path(target.cli_path).resolve().parent)
    existing_ld_path = env.get("LD_LIBRARY_PATH", "")
    env["LD_LIBRARY_PATH"] = proxy_dir if not existing_ld_path else f"{proxy_dir}:{existing_ld_path}"
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
