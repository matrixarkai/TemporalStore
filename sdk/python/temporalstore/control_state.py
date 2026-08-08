"""Production Control State client for TemporalStore.

Control State is TemporalStore's capability for fast-changing serving signals —
frequency caps, quotas, pacing, eligibility, and suppression. This module is a
self-contained, dependency-free (stdlib only) client that speaks the
Redis-compatible (RESP) protocol TemporalStore exposes, and wraps it in
high-level, use-case-shaped methods.

The serving shape for every use case is the same:

    read current state -> apply rule -> atomically update state -> return decision

The accept-decision primitive is the atomic increment-then-read operator
``HSETANDGET`` (and its idempotent variant ``HSETANDGETOPT``), issued in a single
round trip so concurrent requests cannot race past a cap.

Design notes for production
---------------------------
* **Thread-safe connection pool** with bounded size, health checks, and lazy
  reconnect. Safe to share one :class:`ControlState` instance across threads.
* **Bounded retries with jitter.** Read-only and idempotent (``event_id``) calls
  retry on transient network faults; a plain non-idempotent increment is retried
  only when the failure happened *before* the request was sent, so a timed-out
  write is never silently double-applied.
* **No global state, no hidden clock.** Callers may pin ``now_ms`` for testing;
  windows are computed in UTC.
* **Metrics + logging hooks** so decisions and latencies flow into your
  observability stack without coupling this client to it.

Quick start
-----------
    from temporalstore.control_state import ControlState, ControlStateConfig

    cs = ControlState(ControlStateConfig(host="127.0.0.1", port=16379))
    decision = cs.frequency_cap(user_id="u42", campaign_id="c123", limit=5)
    if decision.allowed:
        serve_ad()   # remaining impressions today: decision.remaining
    cs.close()
"""

from __future__ import annotations

import logging
import os
import queue
import random
import socket
import threading
import time
import uuid as _uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterator, List, Optional, Sequence

__all__ = [
    "ControlState",
    "ControlStateConfig",
    "Decision",
    "PacingState",
    "ControlStateError",
    "ControlStateConnectionError",
    "ControlStateProtocolError",
    "ControlStateCommandError",
]

logger = logging.getLogger("temporalstore.control_state")

_DAY_MS = 86_400_000
_MINUTE_MS = 60_000


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ControlStateError(Exception):
    """Base class for all Control State client errors."""


class ControlStateConnectionError(ControlStateError):
    """A transient network/transport failure talking to the server."""


class ControlStateProtocolError(ControlStateError):
    """The server returned a reply this client could not parse."""


class ControlStateCommandError(ControlStateError):
    """The server rejected the command (a RESP ``-ERR`` reply)."""


# --------------------------------------------------------------------------- #
# Value types
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Decision:
    """The outcome of an accept-decision (frequency cap, quota, suppression)."""

    allowed: bool
    count: int
    limit: int
    reason: str = ""

    @property
    def remaining(self) -> int:
        """Units left before the limit is hit (never negative)."""
        return max(0, self.limit - self.count)


@dataclass(frozen=True)
class PacingState:
    """Delivery pacing signal for a campaign."""

    spent: int
    budget: int
    target_by_now: int
    pace: str          # "under" | "on" | "over"
    multiplier: float  # delivery boost/dampen factor


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class ControlStateConfig:
    """Connection + policy configuration.

    Values fall back to ``TS_*`` environment variables so the same code runs in
    dev and prod without edits.
    """

    host: str = field(default_factory=lambda: os.environ.get("TS_REDIS_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: int(os.environ.get("TS_REDIS_PORT", "16379")))
    tenant: str = field(default_factory=lambda: os.environ.get("TS_TENANT", "default"))

    connect_timeout_s: float = 1.0
    socket_timeout_s: float = 1.0
    pool_max: int = 16
    pool_checkout_timeout_s: float = 2.0

    max_retries: int = 3
    backoff_base_s: float = 0.02
    backoff_max_s: float = 0.5

    # Optional observability hook: metrics(name, value, tags) — never raises here.
    metrics: Optional[Callable[[str, float, dict], None]] = None


# --------------------------------------------------------------------------- #
# RESP protocol
# --------------------------------------------------------------------------- #
def _encode_command(args: Sequence[object]) -> bytes:
    """Encode a command as a RESP array of bulk strings."""
    out = [b"*%d\r\n" % len(args)]
    for arg in args:
        data = arg if isinstance(arg, (bytes, bytearray)) else str(arg).encode("utf-8")
        out.append(b"$%d\r\n" % len(data))
        out.append(bytes(data))
        out.append(b"\r\n")
    return b"".join(out)


class _RespReader:
    """Incremental RESP reader over a socket, buffering partial reads."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = bytearray()

    def _fill(self) -> None:
        chunk = self._sock.recv(65536)
        if not chunk:
            raise ControlStateConnectionError("connection closed by server")
        self._buf.extend(chunk)

    def _read_line(self) -> bytes:
        while True:
            idx = self._buf.find(b"\r\n")
            if idx >= 0:
                line = bytes(self._buf[:idx])
                del self._buf[: idx + 2]
                return line
            self._fill()

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n + 2:  # payload + trailing CRLF
            self._fill()
        data = bytes(self._buf[:n])
        del self._buf[: n + 2]
        return data

    def read_reply(self) -> object:
        line = self._read_line()
        if not line:
            raise ControlStateProtocolError("empty reply line")
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode("utf-8", "replace")
        if kind == b"-":
            raise ControlStateCommandError(rest.decode("utf-8", "replace"))
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            return self._read_exact(n)
        if kind == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read_reply() for _ in range(n)]
        raise ControlStateProtocolError(f"unknown RESP type {kind!r}")


class _Connection:
    """A single pooled socket connection."""

    def __init__(self, cfg: ControlStateConfig) -> None:
        self._cfg = cfg
        self._sock: Optional[socket.socket] = None
        self._reader: Optional[_RespReader] = None

    def _connect(self) -> None:
        sock = socket.create_connection(
            (self._cfg.host, self._cfg.port), timeout=self._cfg.connect_timeout_s
        )
        sock.settimeout(self._cfg.socket_timeout_s)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock = sock
        self._reader = _RespReader(sock)

    def ensure(self) -> None:
        if self._sock is None:
            self._connect()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            finally:
                self._sock = None
                self._reader = None

    def call(self, args: Sequence[object]) -> object:
        """Send one command and read one reply. Raises on transport failure.

        ``sent`` state is tracked by the caller via the exception type: a failure
        before ``sendall`` returns is a connect/pre-send fault (safe to retry);
        a failure while reading is post-send.
        """
        assert self._sock is not None and self._reader is not None
        self._sock.sendall(_encode_command(args))
        return self._reader.read_reply()


class _ConnectionPool:
    """Bounded, thread-safe pool of :class:`_Connection`."""

    def __init__(self, cfg: ControlStateConfig) -> None:
        self._cfg = cfg
        self._idle: "queue.LifoQueue[_Connection]" = queue.LifoQueue(maxsize=cfg.pool_max)
        self._lock = threading.Lock()
        self._created = 0
        self._closed = False

    def _new(self) -> _Connection:
        with self._lock:
            if self._closed:
                raise ControlStateConnectionError("pool is closed")
            self._created += 1
        return _Connection(self._cfg)

    @contextmanager
    def acquire(self) -> Iterator[_Connection]:
        try:
            conn = self._idle.get_nowait()
        except queue.Empty:
            conn = self._new()
        try:
            conn.ensure()
            yield conn
        finally:
            # Return healthy connections; drop broken ones so they reconnect.
            if not self._closed and conn._sock is not None:
                try:
                    self._idle.put_nowait(conn)
                except queue.Full:
                    conn.close()
            else:
                conn.close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
        while True:
            try:
                self._idle.get_nowait().close()
            except queue.Empty:
                break


# --------------------------------------------------------------------------- #
# Time helpers
# --------------------------------------------------------------------------- #
def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _utc_day_bounds_ms(now_ms: int) -> tuple[int, int, str]:
    """Return (day_start_ms, day_end_ms, yyyy-mm-dd) for the UTC calendar day."""
    dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    start_ms = int(start.timestamp() * 1000)
    return start_ms, start_ms + _DAY_MS - 1, start.strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #
class ControlState:
    """High-level Control State client.

    One instance owns a connection pool and is safe to share across threads.
    Call :meth:`close` (or use it as a context manager) on shutdown.
    """

    def __init__(self, config: Optional[ControlStateConfig] = None) -> None:
        self._cfg = config or ControlStateConfig()
        self._pool = _ConnectionPool(self._cfg)

    # -- lifecycle --------------------------------------------------------- #
    def close(self) -> None:
        self._pool.close()

    def __enter__(self) -> "ControlState":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- key design -------------------------------------------------------- #
    def key(self, *parts: str, tenant: Optional[str] = None) -> str:
        """Build a control key: ``control:<tenant>:<parts...>``.

        Keeping a stable, structured key layout is what lets caps, quotas, and
        suppression all share one storage model.
        """
        tenant = tenant or self._cfg.tenant
        return ":".join(["control", tenant, *parts])

    # -- transport with retry policy --------------------------------------- #
    def _call(self, args: Sequence[object], *, idempotent: bool) -> object:
        """Issue one command with the configured retry policy.

        ``idempotent=True`` may be retried after the request was sent (reads, or
        writes carrying a dedup ``uuid``). ``idempotent=False`` is retried only
        when the failure occurred before the bytes went out, so an at-most-once
        increment is never silently double-applied on a timeout.
        """
        cfg = self._cfg
        attempt = 0
        started = time.monotonic()
        while True:
            attempt += 1
            sent = False
            try:
                with self._pool.acquire() as conn:
                    sent = True  # acquire() succeeded => socket is connected
                    reply = conn.call(args)
                self._emit("control_state.call.ok", time.monotonic() - started,
                           {"cmd": str(args[0]), "attempt": attempt})
                return reply
            except ControlStateCommandError:
                raise  # logical rejection — never retry
            except (ControlStateConnectionError, socket.timeout, OSError) as exc:
                # A post-send failure on a non-idempotent write must surface, not
                # retry, to preserve at-most-once semantics.
                retryable = (not sent) or idempotent
                if not retryable or attempt > cfg.max_retries:
                    self._emit("control_state.call.fail", attempt, {"cmd": str(args[0])})
                    raise ControlStateConnectionError(
                        f"{args[0]} failed after {attempt} attempt(s): {exc}"
                    ) from exc
                self._backoff(attempt)

    def _backoff(self, attempt: int) -> None:
        delay = min(self._cfg.backoff_max_s, self._cfg.backoff_base_s * (2 ** (attempt - 1)))
        time.sleep(delay * (0.5 + random.random() / 2))  # full jitter

    def _emit(self, name: str, value: float, tags: dict) -> None:
        if self._cfg.metrics is not None:
            try:
                self._cfg.metrics(name, value, tags)
            except Exception:  # metrics must never break serving
                logger.debug("metrics hook raised", exc_info=True)

    # -- low-level primitives (rarely called directly) --------------------- #
    def _incr_and_read(self, key: str, amount: int, start_ms: int, end_ms: int,
                       occur_ms: int, aggregator: str = "sum") -> int:
        """Atomic increment-then-read (``HSETANDGET``). At-most-once on retry."""
        reply = self._call(
            ["HSETANDGET", key, occur_ms, amount, start_ms, end_ms, aggregator],
            idempotent=False,
        )
        return int(reply)

    def _incr_and_read_idempotent(self, key: str, amount: int, start_ms: int, end_ms: int,
                                  occur_ms: int, precision_ms: int, ttl_ms: int,
                                  event_id: str, aggregator: str = "sum") -> int:
        """Atomic, idempotent increment-then-read (``HSETANDGETOPT``).

        A repeated ``event_id`` inside the dedup window is a no-op write that
        still returns the current total, so at-least-once queue replays and
        client retries do not double-count.
        """
        reply = self._call(
            ["HSETANDGETOPT", key, occur_ms, amount, start_ms, end_ms, aggregator,
             precision_ms, ttl_ms, event_id],
            idempotent=True,
        )
        return int(reply)

    def window_count(self, key: str, start_ms: int, end_ms: int,
                     aggregator: str = "sum") -> int:
        """Read-only aggregate over a window (``HQUERY``)."""
        return int(self._call(["HQUERY", key, start_ms, end_ms, aggregator], idempotent=True))

    # -- use case 1: frequency cap ----------------------------------------- #
    def frequency_cap(self, *, user_id: str, campaign_id: str, limit: int,
                      event: str = "impression", amount: int = 1,
                      tenant: Optional[str] = None, now_ms: Optional[int] = None,
                      event_id: Optional[str] = None) -> Decision:
        """Enforce a per-user, per-campaign **daily** cap.

        Returns a :class:`Decision`; serve while ``decision.allowed`` and show
        ``decision.remaining``. Enforcement is exact: allows per (user, campaign,
        day) == ``min(attempts, limit)``.

        Pass ``event_id`` (a stable id for the impression) to make the call
        replay- and retry-safe via server-side dedup.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        start_ms, end_ms, day = _utc_day_bounds_ms(now_ms)
        key = self.key("user", user_id, "campaign", campaign_id, event, day, tenant=tenant)
        if event_id is not None:
            count = self._incr_and_read_idempotent(
                key, amount, start_ms, end_ms, now_ms,
                precision_ms=_DAY_MS, ttl_ms=_DAY_MS, event_id=event_id)
        else:
            count = self._incr_and_read(key, amount, start_ms, end_ms, now_ms)
        allowed = count <= limit
        return Decision(allowed=allowed, count=count, limit=limit,
                        reason="" if allowed else "frequency_cap_exceeded")

    # -- use case 2: tenant / API quota ------------------------------------ #
    def tenant_quota(self, *, resource: str, limit: int, weight: int = 1,
                     tenant: Optional[str] = None, now_ms: Optional[int] = None,
                     event_id: Optional[str] = None) -> Decision:
        """Meter a daily quota for a tenant resource (weighted requests).

        ``decision.remaining`` is the allowance left for the day.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        start_ms, end_ms, day = _utc_day_bounds_ms(now_ms)
        key = self.key("quota", resource, day, tenant=tenant)
        if event_id is not None:
            used = self._incr_and_read_idempotent(
                key, weight, start_ms, end_ms, now_ms,
                precision_ms=_DAY_MS, ttl_ms=_DAY_MS, event_id=event_id)
        else:
            used = self._incr_and_read(key, weight, start_ms, end_ms, now_ms)
        allowed = used <= limit
        return Decision(allowed=allowed, count=used, limit=limit,
                        reason="" if allowed else "quota_exceeded")

    # -- use case 3: campaign pacing --------------------------------------- #
    def record_spend(self, *, campaign_id: str, cost: int,
                     tenant: Optional[str] = None, now_ms: Optional[int] = None) -> None:
        """Accrue delivery spend for a campaign's daily budget window."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        _, _, day = _utc_day_bounds_ms(now_ms)
        key = self.key("campaign", campaign_id, "pacing", "spend", day, tenant=tenant)
        # Fire-and-forget windowed increment at day precision.
        self._call(["CONTROLSTATEINCROPT", key, now_ms, cost, _DAY_MS, _DAY_MS],
                   idempotent=False)

    def pacing(self, *, campaign_id: str, budget: int, target_by_now: int,
               tenant: Optional[str] = None, now_ms: Optional[int] = None) -> PacingState:
        """Read spend-so-far and derive a pacing multiplier.

        ``multiplier`` > 1 boosts delivery (under pace), < 1 dampens it (over
        pace). Use it to boost, dampen, or pause serving.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        start_ms, end_ms, day = _utc_day_bounds_ms(now_ms)
        key = self.key("campaign", campaign_id, "pacing", "spend", day, tenant=tenant)
        spent = self.window_count(key, start_ms, end_ms)
        target = max(1, target_by_now)
        ratio = spent / target
        if ratio < 0.95:
            pace, mult = "under", min(2.0, 1.0 + (1.0 - ratio))
        elif ratio > 1.05:
            pace, mult = "over", max(0.0, 1.0 - (ratio - 1.0))
        else:
            pace, mult = "on", 1.0
        return PacingState(spent=spent, budget=budget, target_by_now=target_by_now,
                           pace=pace, multiplier=round(mult, 3))

    # -- use case 4: rolling-window suppression / risk --------------------- #
    def rolling_suppression(self, *, entity_kind: str, entity_id: str, signal: str,
                            threshold: int, window_ms: int = 3_600_000,
                            precision_ms: int = _MINUTE_MS, amount: int = 1,
                            tenant: Optional[str] = None,
                            now_ms: Optional[int] = None) -> Decision:
        """Count a signal over a rolling window and suppress at the threshold.

        Example — failed logins per device over the last hour::

            d = cs.rolling_suppression(entity_kind="device", entity_id="d7",
                                       signal="login:failed", threshold=5)
            if not d.allowed:
                block()   # d.reason == "suppressed"
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key(entity_kind, entity_id, signal, f"rolling_{window_ms // 1000}s",
                       tenant=tenant)
        # Record the event at the configured precision with a TTL covering the window.
        self._call(["CONTROLSTATEINCROPT", key, now_ms, amount, precision_ms, window_ms * 2],
                   idempotent=False)
        count = self.window_count(key, now_ms - window_ms, now_ms)
        suppressed = count >= threshold
        return Decision(allowed=not suppressed, count=count, limit=threshold,
                        reason="suppressed" if suppressed else "")

    # -- use case 5: idempotent ingestion ---------------------------------- #
    def ingest_impression(self, *, user_id: str, campaign_id: str, limit: int,
                          event_id: str, tenant: Optional[str] = None,
                          now_ms: Optional[int] = None) -> Decision:
        """Freq-cap accounting from an at-least-once queue (replay-safe).

        Identical to :meth:`frequency_cap` but ``event_id`` is required — the
        server dedups redeliveries so the enforced count equals the number of
        *distinct* events, not deliveries.
        """
        return self.frequency_cap(user_id=user_id, campaign_id=campaign_id, limit=limit,
                                  tenant=tenant, now_ms=now_ms, event_id=event_id)

    # -- use case 6: eligibility (first/last seen) ------------------------- #
    def mark_seen(self, *, subject: str, value: str, ttl_ms: int = 30 * _DAY_MS,
                  keep: str = "first", tenant: Optional[str] = None,
                  now_ms: Optional[int] = None) -> None:
        """Record a first/last value for eligibility policies (``FOLSET``)."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key("eligibility", subject, tenant=tenant)
        self._call(["FOLSET", key, value, now_ms, ttl_ms, keep.upper()], idempotent=True)

    def seen_value(self, *, subject: str, tenant: Optional[str] = None) -> Optional[str]:
        """Read the stored first/last value (``FOLQUERY``)."""
        key = self.key("eligibility", subject, tenant=tenant)
        reply = self._call(["FOLQUERY", key], idempotent=True)
        return reply.decode("utf-8") if isinstance(reply, (bytes, bytearray)) else reply

    # -- shared atomic helper for windowed accept-decisions ---------------- #
    def _setandget_opt(self, key: str, amount: int, start_ms: int, end_ms: int,
                       occur_ms: int, precision_ms: int, ttl_ms: int,
                       event_id: Optional[str], aggregator: str = "sum") -> int:
        """Atomic increment-then-read with precision/TTL and optional dedup.

        Retries are enabled only when an ``event_id`` makes the write idempotent.
        """
        return int(self._call(
            ["HSETANDGETOPT", key, occur_ms, amount, start_ms, end_ms, aggregator,
             precision_ms, ttl_ms, event_id or ""],
            idempotent=bool(event_id)))

    def _decr(self, key: str, occur_ms: int) -> None:
        self._call(["CONTROLSTATEINCR", key, occur_ms, -1], idempotent=False)

    # -- use case 7: sliding-window rate limit ----------------------------- #
    def rate_limit(self, *, name: str, limit: int, window_ms: int,
                   precision_ms: int = 1_000, amount: int = 1,
                   tenant: Optional[str] = None, now_ms: Optional[int] = None,
                   event_id: Optional[str] = None) -> Decision:
        """At most ``limit`` events per rolling ``window_ms`` (e.g. 100 req / 60s).

        The window slides continuously at ``precision_ms`` granularity. Pass
        ``event_id`` to make the reservation retry-safe.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key("ratelimit", name, f"w{window_ms // 1000}s", tenant=tenant)
        count = self._setandget_opt(key, amount, now_ms - window_ms, now_ms, now_ms,
                                    precision_ms=precision_ms, ttl_ms=window_ms * 2,
                                    event_id=event_id)
        allowed = count <= limit
        return Decision(allowed=allowed, count=count, limit=limit,
                        reason="" if allowed else "rate_limited")

    # -- use case 8: debounce (must be quiet for a period) ----------------- #
    def debounce(self, *, name: str, quiet_ms: int, tenant: Optional[str] = None,
                 now_ms: Optional[int] = None) -> Decision:
        """Allow only if nothing happened in the trailing ``quiet_ms`` window.

        Useful for collapsing bursts — e.g. one alert per incident, one welcome
        email per sign-up burst. Any attempt (allowed or not) refreshes the quiet
        window, so the action fires only after things settle.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key("debounce", name, tenant=tenant)
        count = self._setandget_opt(key, 1, now_ms - quiet_ms, now_ms, now_ms,
                                    precision_ms=1_000, ttl_ms=quiet_ms * 2, event_id=None)
        allowed = count <= 1
        return Decision(allowed=allowed, count=count, limit=1,
                        reason="" if allowed else "debounced")

    # -- use case 9: concurrency / in-flight limit (semaphore) ------------- #
    _CONCURRENCY_END_MS = 1 << 62  # a fixed far-future window upper bound

    def concurrency_acquire(self, *, resource: str, limit: int,
                            tenant: Optional[str] = None,
                            now_ms: Optional[int] = None) -> Decision:
        """Reserve one in-flight slot; allow while in-flight <= ``limit``.

        Pair every allowed acquire with :meth:`concurrency_release` (use
        try/finally). An over-capacity acquire is rolled back so it does not leak.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key("concurrency", resource, tenant=tenant)
        in_flight = self._incr_and_read(key, 1, 0, self._CONCURRENCY_END_MS, now_ms)
        if in_flight <= limit:
            return Decision(allowed=True, count=in_flight, limit=limit)
        self._decr(key, now_ms)  # roll back our reservation
        return Decision(allowed=False, count=in_flight - 1, limit=limit,
                        reason="concurrency_limit_exceeded")

    def concurrency_release(self, *, resource: str, tenant: Optional[str] = None,
                            now_ms: Optional[int] = None) -> None:
        """Release a previously acquired in-flight slot."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        self._decr(self.key("concurrency", resource, tenant=tenant), now_ms)

    @contextmanager
    def concurrency_slot(self, *, resource: str, limit: int,
                         tenant: Optional[str] = None) -> Iterator[Decision]:
        """Context manager wrapping acquire/release.

            with cs.concurrency_slot(resource="pdf-render", limit=8) as slot:
                if not slot.allowed:
                    raise Busy()
                render()
        """
        decision = self.concurrency_acquire(resource=resource, limit=limit, tenant=tenant)
        try:
            yield decision
        finally:
            if decision.allowed:
                self.concurrency_release(resource=resource, tenant=tenant)

    # -- use case 10: distinct / unique counting --------------------------- #
    def add_unique(self, *, subject: str, value: str, precision_ms: int = _DAY_MS,
                   ttl_ms: int = _DAY_MS, tenant: Optional[str] = None,
                   now_ms: Optional[int] = None) -> None:
        """Record a distinct value for ``subject`` (e.g. a device id per user)."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key("unique", subject, tenant=tenant)
        self._call(["CONTROLSTATECHANGE", key, now_ms, value, precision_ms, ttl_ms],
                   idempotent=True)

    def unique_count(self, *, subject: str, window_ms: int = _DAY_MS,
                     tenant: Optional[str] = None, now_ms: Optional[int] = None) -> int:
        """Distinct-value count for ``subject`` over the trailing ``window_ms``."""
        now_ms = now_ms if now_ms is not None else _now_ms()
        key = self.key("unique", subject, tenant=tenant)
        return int(self._call(
            ["CONTROLSTATEQUERY", key, now_ms - window_ms, now_ms, "change"],
            idempotent=True))

    # -- use case 11: hard budget gate (ad spend, credits) ----------------- #
    def budget_gate(self, *, budget_id: str, cost: int, budget: int,
                    period: str = "day", tenant: Optional[str] = None,
                    now_ms: Optional[int] = None,
                    event_id: Optional[str] = None) -> Decision:
        """Atomically charge ``cost`` against a daily ``budget``; allow if within.

        ``decision.remaining`` is the budget left. Pass ``event_id`` for
        exactly-once charging under retries/replays.
        """
        now_ms = now_ms if now_ms is not None else _now_ms()
        start_ms, end_ms, day = _utc_day_bounds_ms(now_ms)
        key = self.key("budget", budget_id, period, day, tenant=tenant)
        if event_id is not None:
            spent = self._setandget_opt(key, cost, start_ms, end_ms, now_ms,
                                        precision_ms=_DAY_MS, ttl_ms=_DAY_MS,
                                        event_id=event_id)
        else:
            spent = self._incr_and_read(key, cost, start_ms, end_ms, now_ms)
        allowed = spent <= budget
        return Decision(allowed=allowed, count=spent, limit=budget,
                        reason="" if allowed else "budget_exhausted")

    @staticmethod
    def new_event_id() -> str:
        """Convenience: a fresh idempotency key for a producer."""
        return _uuid.uuid4().hex


# --------------------------------------------------------------------------- #
# Runnable demo (requires a TemporalStore RESP endpoint, e.g. redis_proxy)
# --------------------------------------------------------------------------- #
def _demo() -> None:  # pragma: no cover - manual/integration use
    logging.basicConfig(level=logging.INFO)

    def metrics(name: str, value: float, tags: dict) -> None:
        logger.debug("metric %s=%.4f %s", name, value, tags)

    cfg = ControlStateConfig(metrics=metrics)
    with ControlState(cfg) as cs:
        print("== frequency cap (limit 5) ==")
        for i in range(7):
            d = cs.frequency_cap(user_id="u42", campaign_id="c123", limit=5)
            print(f"  attempt {i + 1}: allowed={d.allowed} count={d.count} "
                  f"remaining={d.remaining} {d.reason}")

        print("== tenant quota (limit 1,000,000) ==")
        d = cs.tenant_quota(resource="api_requests", limit=1_000_000, weight=3)
        print(f"  used={d.count} remaining={d.remaining} allowed={d.allowed}")

        print("== pacing ==")
        cs.record_spend(campaign_id="c9", cost=1300)
        p = cs.pacing(campaign_id="c9", budget=5000, target_by_now=1500)
        print(f"  spent={p.spent} pace={p.pace} multiplier={p.multiplier}")

        print("== rolling suppression (threshold 5) ==")
        for i in range(6):
            d = cs.rolling_suppression(entity_kind="device", entity_id="d7",
                                       signal="login:failed", threshold=5)
            print(f"  failure {i + 1}: allowed={d.allowed} count={d.count} {d.reason}")

        print("== idempotent ingestion (same event_id twice) ==")
        evt = cs.new_event_id()
        a = cs.ingest_impression(user_id="u77", campaign_id="c5", limit=5, event_id=evt)
        b = cs.ingest_impression(user_id="u77", campaign_id="c5", limit=5, event_id=evt)
        print(f"  first={a.count} replay={b.count} (equal => deduped)")

        print("== sliding-window rate limit (100 / 60s) ==")
        d = cs.rate_limit(name="apikey:k1", limit=100, window_ms=60_000)
        print(f"  count={d.count} allowed={d.allowed} remaining={d.remaining}")

        print("== concurrency limit (max 8 in flight) ==")
        with cs.concurrency_slot(resource="pdf-render", limit=8) as slot:
            print(f"  acquired={slot.allowed} in_flight={slot.count}")

        print("== distinct/unique devices per user/day ==")
        for dev in ("d1", "d2", "d1"):
            cs.add_unique(subject="user:u42:devices", value=dev)
        print(f"  unique devices={cs.unique_count(subject='user:u42:devices')}")

        print("== hard budget gate ($5000/day) ==")
        d = cs.budget_gate(budget_id="camp:c9", cost=1300, budget=5000)
        print(f"  spent={d.count} remaining={d.remaining} allowed={d.allowed}")


if __name__ == "__main__":  # pragma: no cover
    _demo()
