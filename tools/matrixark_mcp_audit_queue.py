#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""The pool async audit writes run on, and the bounded drain close() needs.

Kept beside the server rather than inside it: this is background-worker plumbing, and the
entrypoint carries a size budget that test_mcp_entrypoint_stays_small enforces.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, wait as wait_for_futures
from typing import Any, Callable, Set


class AuditWriteQueue:
    """An audit write pool that can be drained within a deadline.

    The executor's own shutdown(wait=False) neither cancels queued writes nor waits for
    in-flight ones, so a closed server could still be appending audit records -- into a
    directory its caller has already started removing. shutdown(wait=True) is not the answer
    either: it takes no timeout, so a single wedged write would hang shutdown forever. Track
    what was submitted and wait for exactly that, bounded.
    """

    def __init__(self, max_workers: int) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max(1, max_workers))
        self._pending: Set[Any] = set()
        self._lock = threading.Lock()

    @property
    def pending(self) -> Set[Any]:
        with self._lock:
            return set(self._pending)

    def submit(self, write: Callable[[], Any]) -> Any:
        future = self._executor.submit(write)
        with self._lock:
            self._pending.add(future)
        future.add_done_callback(self._forget)
        return future

    def _forget(self, future: Any) -> None:
        with self._lock:
            self._pending.discard(future)

    def drain(self, timeout_s: float) -> None:
        with self._lock:
            pending = list(self._pending)
        if pending:
            wait_for_futures(pending, timeout=max(0.0, timeout_s))
        self._executor.shutdown(wait=False, cancel_futures=False)
