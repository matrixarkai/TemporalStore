#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Shutting the MCP server down inside the budget its caller is willing to wait.

Lives beside `matrixark_mcp_server` rather than inside it, the way `AuditWriteQueue` does: that
module is held to 750 lines on purpose, and shutdown sequencing is a self-contained concern with its
own rationale to carry.

## Why the sequencing needs stating

A server close is four waits -- two background threads, the adapter, the audit queue -- and every one
of them used to receive the caller's FULL budget. The caller waits once. So a close could need four
times what it was given, and the FIRST wait alone could spend all of it.

Measured on the live box, that is not a corner case but the norm: every one of 2,811 hook closes hit
its 750 ms budget exactly (min = p50 = p90 = max = 750). The first wait is a join against a
1000 ms-interval summary poller, so it spent the whole budget, and the two steps that actually FLUSH
-- the adapter close and the audit drain -- were reached only after the caller had abandoned the
thread. The latency was the smaller half of it.

So the budget here is a DEADLINE every step measures itself against, and the two thread joins take a
bounded share of it. They are daemon threads in a process that is exiting, which makes joining them
a courtesy; the flushes are the reason a caller waits at all.
"""

from __future__ import annotations

import time
from typing import Any

#: The share of a close budget the two background-thread joins may spend between them.
#:
#: Capping it is what stops a poller that will not stop from consuming everything and leaving the
#: flushes with nothing. A quarter is enough for a thread that is merely between iterations, and
#: cheap enough to lose when one is mid-request.
CLOSE_JOIN_BUDGET_SHARE = 0.25


def close_server_within_budget(server: Any, timeout_s: float) -> None:
    """Stop `server`'s background work and flush it, in `timeout_s` TOTAL.

    Total, not per step: each stage asks what is LEFT rather than helping itself to the whole budget
    again. A caller that waits `timeout_s` gets a close that tried to finish in `timeout_s`.
    """
    deadline = time.monotonic() + max(0.0, timeout_s)

    def remaining() -> float:
        return max(0.0, deadline - time.monotonic())

    server._summary_stop.set()
    server._stream_materialize_stop.set()

    # The joins share a slice; whatever they leave goes to the flushes below.
    join_deadline = time.monotonic() + remaining() * CLOSE_JOIN_BUDGET_SHARE
    for thread in (server._summary_thread, server._stream_materialize_thread):
        if thread is None:
            continue
        thread.join(timeout=max(0.0, join_deadline - time.monotonic()))

    adapter_close = getattr(server.adapter, "close", None)
    if callable(adapter_close):
        adapter_close(timeout_s=remaining())
    server._audit_queue.drain(remaining())
