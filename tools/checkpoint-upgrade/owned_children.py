# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Contain hosted CI descendants, including children orphaned by a build failure."""
import ctypes
from dataclasses import dataclass
import os
from pathlib import Path
import signal
import time


@dataclass(frozen=True)
class Child:
    pid: int
    parent: int
    started: int
    state: str


def enable():
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("identity-safe Linux child supervision is unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.prctl.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    libc.prctl.restype = ctypes.c_int
    # PR_SET_CHILD_SUBREAPER reparents orphaned descendants to this driver.
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "could not supervise CI descendants")


def read_child(pid, proc=Path("/proc")):
    raw = (proc / str(pid) / "stat").read_text(errors="surrogateescape")
    fields = raw.rpartition(") ")[2].split()
    if len(fields) < 20:
        raise RuntimeError("incomplete process identity")
    return Child(pid, int(fields[1]), int(fields[19]), fields[0])


def children(proc=Path("/proc")):
    result = []
    for entry in proc.iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            child = read_child(int(entry.name), proc)
        except (FileNotFoundError, ProcessLookupError):
            continue
        if child.parent == os.getpid():
            result.append(child)
    return result


def reap(rows, leader):
    for child in rows:
        if child.state == "Z" and child.pid != leader:
            try:
                os.waitpid(child.pid, os.WNOHANG)
            except ChildProcessError:
                pass


def send(child, value):
    try:
        descriptor = os.pidfd_open(child.pid, 0)
    except ProcessLookupError:
        return
    try:
        current = read_child(child.pid)
        if current.started == child.started and current.parent == os.getpid():
            signal.pidfd_send_signal(descriptor, value, None, 0)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(descriptor)


def terminate(leader):
    deadline = time.monotonic() + 10
    while True:
        leader.poll()  # Popen owns the launcher's wait status.
        rows = children()
        reap(rows, leader.pid)
        live = [child for child in rows if child.state != "Z"]
        if not live:
            leader.wait(timeout=1)
            reap(children(), leader.pid)
            return
        now = time.monotonic()
        if now > deadline + 5:
            raise RuntimeError("owned CI descendants did not terminate")
        for child in live:
            send(child, signal.SIGTERM if now < deadline else signal.SIGKILL)
        time.sleep(0.05)
