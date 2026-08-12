# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Production-ready TemporalStore usage patterns.

A batteries-included facade over the TemporalStore Python SDK (native ``Client``
*or* HTTP ``ProxyClient``) that demonstrates how to run long-sequence features and
the common companion use cases in a real service, with the operational concerns a
production deployment needs:

* configuration from environment / dataclass, native-or-proxy backend selection;
* connection lifecycle (context manager) and safe reuse across a request handler;
* bounded retries with exponential backoff + jitter, applied only to operations
  that are safe to retry (reads, and appends that are idempotent by timestamp);
* request batching that respects the SDK's per-request limits;
* input validation (key / value sizes, positive timestamps);
* structured logging and a pluggable metrics sink (latency + error counters);
* idempotent counter increments for frequency caps (via a dedup ``uuid``);
* a readiness/health check and typed errors.

Use cases shown (see ``FeatureStore`` and ``demo()``):

1. **Long-sequence behavior features** — per-entity interaction histories with
   bounded window reads and server-side filters (recommendation / ads / search).
2. **Numeric observation features** — timestamped values for serving-time reads.
3. **Frequency caps / rate counters** — control-state counters over time windows.
4. **Entity metadata** — KV / hash / set records with TTL.

This module is dependency-free (stdlib only) beyond the ``temporalstore`` SDK.

Run the self-contained demo (defaults to the local proxy endpoint)::

    TS_BACKEND=proxy TS_ENDPOINT=http://127.0.0.1:8080 \
    TS_NAMESPACE=reco TS_TABLE=user_features \
        python -m examples.production_feature_store

Nothing runs on import; the demo is guarded by ``__main__``.
"""

from __future__ import annotations

import logging
import os
import random
import time
import uuid as _uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Iterable, Iterator, List, Optional, Protocol, Sequence, runtime_checkable

# The SDK ships two interchangeable clients; import the concrete types from the
# submodules so this works regardless of what the package __init__ re-exports.
from temporalstore.client import (
    Client,
    FeatureFilter,
    FeatureFilterOp,
    FeaturePoint,
    Options,
    RiskPrecision as ControlPrecision,
    SequenceFeatureRow,
    TemporalStoreError,
    WindowUnit,
)
from temporalstore.proxy_client import ProxyClient, ProxyOptions

logger = logging.getLogger("temporalstore.feature_store")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential backoff with jitter for transient failures."""

    max_attempts: int = 3
    base_delay_s: float = 0.05
    max_delay_s: float = 1.0
    # SDK error codes / conditions treated as transient (safe to retry).
    # Code 0 in this SDK is "no code / transport error"; treat network-level
    # failures as transient. Application errors (bad args) are not retried.
    transient_codes: frozenset = frozenset({0, 503, 504, 429})

    def delay_for(self, attempt: int) -> float:
        raw = min(self.max_delay_s, self.base_delay_s * (2 ** (attempt - 1)))
        # full jitter: sleep in [0, raw]
        return random.uniform(0.0, raw)


@dataclass(frozen=True)
class StoreConfig:
    """Everything needed to construct and operate a client.

    Prefer :meth:`from_env` in services so config lives in the deployment, not
    the code.
    """

    backend: str = "proxy"  # "proxy" (HTTP) or "native" (ctypes .so)
    namespace: str = "default"
    table: str = "features"

    # proxy backend
    endpoint: str = "http://127.0.0.1:8080"
    api_key: str = ""

    # native backend
    metaserver_addr: str = "127.0.0.1:17101"
    library_path: Optional[str] = None

    # limits / timeouts (shared)
    request_timeout_ms: int = 5000
    max_points_per_request: int = 1000
    max_query_count: int = 5000
    max_key_bytes: int = 4096
    max_value_bytes: int = 16 * 1024 * 1024

    retry: RetryPolicy = field(default_factory=RetryPolicy)

    @classmethod
    def from_env(cls, prefix: str = "TS_") -> "StoreConfig":
        def env(name: str, default: str) -> str:
            return os.environ.get(prefix + name, default)

        return cls(
            backend=env("BACKEND", "proxy").lower(),
            namespace=env("NAMESPACE", "default"),
            table=env("TABLE", "features"),
            endpoint=env("ENDPOINT", "http://127.0.0.1:8080"),
            api_key=env("API_KEY", ""),
            metaserver_addr=env("METASERVER", "127.0.0.1:17101"),
            library_path=os.environ.get(prefix + "LIBRARY_PATH") or None,
            request_timeout_ms=int(env("TIMEOUT_MS", "5000")),
            max_points_per_request=int(env("MAX_POINTS", "1000")),
            max_query_count=int(env("MAX_QUERY_COUNT", "5000")),
        )


# ---------------------------------------------------------------------------
# Client protocol + metrics
# ---------------------------------------------------------------------------


@runtime_checkable
class StoreClient(Protocol):
    """The subset of the SDK surface this facade relies on.

    Both ``temporalstore.Client`` and ``temporalstore.ProxyClient`` satisfy this
    structurally, which also makes the facade trivially unit-testable with a fake.
    """

    def add_sequence_feature_rows(self, key: str, rows: Iterable[SequenceFeatureRow]) -> None: ...
    def query_sequence_feature_rows(
        self, key: str, start_ts: int, end_ts: int, count: int, filters: Iterable[FeatureFilter] = ()
    ) -> List[SequenceFeatureRow]: ...
    def add_feature_points(self, key: str, points: Iterable[FeaturePoint]) -> None: ...
    def query_feature_points(
        self, key: str, start_ts: int, end_ts: int, count: int, filters: Iterable[FeatureFilter] = ()
    ) -> List[FeaturePoint]: ...
    def control_state_increment(self, key: str, amount: int = 1, ttl_seconds: int = 86400,
                       precision: ControlPrecision = ControlPrecision.ONE_MINUTE, uuid: str = "",
                       occur_time_seconds: int = 0) -> None: ...
    def control_state_count(self, key: str, precision: ControlPrecision = ControlPrecision.ONE_MINUTE,
                  window_start: int = -1, window_end: int = 0,
                  window_unit: WindowUnit = WindowUnit.HOUR) -> int: ...
    def hset(self, key: str, field: str, value: str) -> None: ...
    def hget(self, key: str, field: str) -> str: ...
    def sadd(self, key: str, member: str) -> None: ...
    def smembers(self, key: str) -> List[str]: ...
    def delete_object(self, key: str) -> None: ...
    def expire(self, key: str, ttl_ms: int) -> None: ...


class MetricsSink(Protocol):
    def timing(self, name: str, seconds: float, ok: bool) -> None: ...


class _NullMetrics:
    def timing(self, name: str, seconds: float, ok: bool) -> None:  # noqa: D401
        return None


# ---------------------------------------------------------------------------
# Client construction
# ---------------------------------------------------------------------------


def build_client(config: StoreConfig) -> StoreClient:
    """Construct the concrete client for ``config.backend``.

    The proxy client is pure-Python (stdlib HTTP) and needs no shared library —
    the right default for portable services. The native client is faster but
    requires ``libtemporalstore.so`` (set ``library_path`` / ``TEMPORALSTORE_LIB``).
    """

    if config.backend == "native":
        options = Options(
            metaserver_addr=config.metaserver_addr,
            namespace_name=config.namespace,
            table_name=config.table,
            request_timeout_ms=config.request_timeout_ms,
            max_feature_points_per_request=config.max_points_per_request,
            max_feature_query_count=config.max_query_count,
            max_key_bytes=config.max_key_bytes,
            max_value_bytes=config.max_value_bytes,
        )
        return Client(options, library_path=config.library_path)

    if config.backend == "proxy":
        return ProxyClient(
            ProxyOptions(
                endpoint=config.endpoint,
                namespace_name=config.namespace,
                table_name=config.table,
                api_key=config.api_key,
                timeout_seconds=config.request_timeout_ms / 1000.0,
            )
        )

    raise ValueError(f"unknown backend {config.backend!r}; expected 'proxy' or 'native'")


# ---------------------------------------------------------------------------
# Key naming — keep the grain in the key, not in a new capability.
# ---------------------------------------------------------------------------


def sequence_key(namespace: str, feature: str, entity: str, *grain: str) -> str:
    """``feature:<ns>:<feature>:<entity>[:g:v...]`` — multi-cardinality by suffix."""
    parts = ["feature", namespace, feature, entity, *grain]
    return ":".join(parts)


def counter_key(namespace: str, name: str, entity: str) -> str:
    return ":".join(["control", namespace, name, entity])


# ---------------------------------------------------------------------------
# The facade
# ---------------------------------------------------------------------------


def _now_ms() -> int:
    return int(time.time() * 1000)


def _window_ms(lookback: timedelta, end_ms: Optional[int] = None) -> tuple[int, int]:
    end = end_ms if end_ms is not None else _now_ms()
    start = end - int(lookback.total_seconds() * 1000)
    return max(0, start), end


class FeatureStore:
    """High-level, production-oriented API over the TemporalStore SDK.

    Construct once per process and reuse (the underlying clients are cheap to
    share within a thread; for multi-threaded servers build one per worker or
    guard with your own pool). Use as a context manager to release the handle::

        with FeatureStore.from_env() as store:
            store.record_interactions("user:u42", rows)
    """

    def __init__(
        self,
        config: StoreConfig,
        client: Optional[StoreClient] = None,
        metrics: Optional[MetricsSink] = None,
    ) -> None:
        self.config = config
        self._client: StoreClient = client if client is not None else build_client(config)
        self._metrics: MetricsSink = metrics or _NullMetrics()

    @classmethod
    def from_env(cls, metrics: Optional[MetricsSink] = None) -> "FeatureStore":
        return cls(StoreConfig.from_env(), metrics=metrics)

    # -- lifecycle -------------------------------------------------------
    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:  # pragma: no cover - best-effort cleanup
                logger.warning("error closing TemporalStore client", exc_info=True)

    def __enter__(self) -> "FeatureStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- internal helpers ------------------------------------------------
    def _validate_key(self, key: str) -> None:
        n = len(key.encode("utf-8"))
        if n == 0:
            raise ValueError("key must be non-empty")
        if n > self.config.max_key_bytes:
            raise ValueError(f"key too large: {n} > {self.config.max_key_bytes} bytes")

    @contextmanager
    def _observe(self, op: str) -> Iterator[None]:
        start = time.perf_counter()
        ok = True
        try:
            yield
        except Exception:
            ok = False
            raise
        finally:
            self._metrics.timing(op, time.perf_counter() - start, ok)

    def _call(self, op: str, fn: Callable[[], object], *, retry: bool) -> object:
        """Run ``fn`` with metrics, logging, and (optionally) bounded retries.

        Retries are only for idempotent operations (reads, timestamp-keyed
        appends, uuid-deduped increments). A non-transient error fails fast.
        """
        policy = self.config.retry
        attempts = policy.max_attempts if retry else 1
        last: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            with self._observe(op):
                try:
                    return fn()
                except TemporalStoreError as exc:
                    last = exc
                    transient = getattr(exc, "code", 0) in policy.transient_codes
                    if not (retry and transient) or attempt == attempts:
                        logger.error("%s failed (code=%s, attempt=%d): %s",
                                     op, getattr(exc, "code", "?"), attempt, exc)
                        raise
                    delay = policy.delay_for(attempt)
                    logger.warning("%s transient failure (attempt %d/%d), retrying in %.3fs: %s",
                                   op, attempt, attempts, delay, exc)
            time.sleep(delay)
        assert last is not None
        raise last

    @staticmethod
    def _chunks(items: Sequence, size: int) -> Iterator[Sequence]:
        for i in range(0, len(items), max(1, size)):
            yield items[i : i + size]

    # ------------------------------------------------------------------
    # Use case 1: long-sequence behavior features
    # ------------------------------------------------------------------
    def record_interactions(self, key: str, rows: Sequence[SequenceFeatureRow]) -> int:
        """Append a batch of behavior events to an entity's sequence.

        Rows are keyed by timestamp, so re-appending the same timestamps is
        idempotent (safe to retry). Large batches are split to respect the SDK's
        per-request limit. Returns the number of rows written.
        """
        self._validate_key(key)
        rows = [r for r in rows if r.timestamp > 0]
        if not rows:
            return 0
        for chunk in self._chunks(rows, self.config.max_points_per_request):
            self._call("sequence_add", lambda c=chunk: self._client.add_sequence_feature_rows(key, c),
                       retry=True)
        return len(rows)

    def recent_interactions(
        self,
        key: str,
        lookback: timedelta,
        limit: int = 100,
        filters: Iterable[FeatureFilter] = (),
        end_ms: Optional[int] = None,
    ) -> List[SequenceFeatureRow]:
        """Read the most recent events in a bounded window (the hot serving path).

        Keep ``lookback`` and ``limit`` deliberately bounded; this is not a full
        history scan. ``filters`` are applied server-side (e.g. clicks only).
        """
        self._validate_key(key)
        start_ms, end = _window_ms(lookback, end_ms)
        count = min(limit, self.config.max_query_count)
        return self._call(  # type: ignore[return-value]
            "sequence_query",
            lambda: self._client.query_sequence_feature_rows(key, start_ms, end, count, filters),
            retry=True,
        )

    # ------------------------------------------------------------------
    # Use case 2: numeric observation features
    # ------------------------------------------------------------------
    def record_observations(self, key: str, points: Sequence[FeaturePoint]) -> int:
        self._validate_key(key)
        points = [p for p in points if p.timestamp > 0]
        for p in points:
            if len(p.value.encode("utf-8")) > self.config.max_value_bytes:
                raise ValueError("feature value exceeds max_value_bytes")
        if not points:
            return 0
        for chunk in self._chunks(points, self.config.max_points_per_request):
            self._call("feature_add", lambda c=chunk: self._client.add_feature_points(key, c), retry=True)
        return len(points)

    def window_values(
        self,
        key: str,
        lookback: timedelta,
        limit: int = 1000,
        filters: Iterable[FeatureFilter] = (),
        end_ms: Optional[int] = None,
    ) -> List[FeaturePoint]:
        self._validate_key(key)
        start_ms, end = _window_ms(lookback, end_ms)
        count = min(limit, self.config.max_query_count)
        return self._call(  # type: ignore[return-value]
            "feature_query",
            lambda: self._client.query_feature_points(key, start_ms, end, count, filters),
            retry=True,
        )

    # ------------------------------------------------------------------
    # Use case 3: frequency caps / rate counters (control-state)
    # ------------------------------------------------------------------
    def increment_counter(
        self,
        key: str,
        amount: int = 1,
        ttl: timedelta = timedelta(days=1),
        precision: ControlPrecision = ControlPrecision.ONE_MINUTE,
        dedup_id: Optional[str] = None,
    ) -> None:
        """Increment a windowed counter.

        Counter increments are **not** idempotent, so retries are enabled only
        when a ``dedup_id`` is supplied (the store de-duplicates by uuid). Pass a
        stable id derived from the event (e.g. the impression id) for safe
        at-least-once producers.
        """
        self._validate_key(key)
        self._call(
            "control_state_increment",
            lambda: self._client.control_state_increment(
                key,
                amount=amount,
                ttl_seconds=int(ttl.total_seconds()),
                precision=precision,
                uuid=dedup_id or "",
            ),
            retry=dedup_id is not None,
        )

    def counter_in_window(
        self,
        key: str,
        lookback_units: int = 1,
        unit: WindowUnit = WindowUnit.HOUR,
        precision: ControlPrecision = ControlPrecision.ONE_MINUTE,
    ) -> int:
        """Count in a rolling window, e.g. impressions in the last N hours.

        Backed by precision rollups, so a wide window stays cheap. Returns the
        aggregate count.
        """
        self._validate_key(key)
        return int(self._call(  # type: ignore[arg-type]
            "control_state_count",
            lambda: self._client.control_state_count(
                key, precision=precision, window_start=-abs(lookback_units), window_end=0, window_unit=unit
            ),
            retry=True,
        ))

    def is_within_cap(self, key: str, cap: int, lookback_units: int = 1,
                     unit: WindowUnit = WindowUnit.HOUR) -> bool:
        """Convenience: has this entity stayed under ``cap`` in the window?"""
        return self.counter_in_window(key, lookback_units, unit) < cap

    # ------------------------------------------------------------------
    # Use case 4: entity metadata (KV / hash / set)
    # ------------------------------------------------------------------
    def set_metadata(self, key: str, field: str, value: str) -> None:
        self._validate_key(key)
        self._call("hset", lambda: self._client.hset(key, field, value), retry=True)

    def get_metadata(self, key: str, field: str) -> str:
        self._validate_key(key)
        return str(self._call("hget", lambda: self._client.hget(key, field), retry=True))

    def add_tag(self, key: str, tag: str) -> None:
        self._validate_key(key)
        self._call("sadd", lambda: self._client.sadd(key, tag), retry=True)

    def tags(self, key: str) -> List[str]:
        self._validate_key(key)
        return list(self._call("smembers", lambda: self._client.smembers(key), retry=True))  # type: ignore[arg-type]

    def expire(self, key: str, ttl: timedelta) -> None:
        self._validate_key(key)
        self._call("expire", lambda: self._client.expire(key, int(ttl.total_seconds() * 1000)), retry=True)

    # ------------------------------------------------------------------
    # Readiness
    # ------------------------------------------------------------------
    def health_check(self) -> bool:
        """Cheap readiness probe: a bounded read on a synthetic key.

        Returns True if the store answers; a missing key is still "healthy".
        """
        probe = sequence_key(self.config.namespace, "_healthcheck", "probe")
        try:
            self._client.query_sequence_feature_rows(probe, 0, 1, 1, ())
            return True
        except TemporalStoreError as exc:
            logger.error("health check failed: %s", exc)
            return False


# ---------------------------------------------------------------------------
# Demo — a realistic recommendation-serving flow
# ---------------------------------------------------------------------------

# action_type convention for the demo
VIEW, CLICK, PURCHASE = 1, 3, 5


def _synthetic_history(n: int, span: timedelta) -> List[SequenceFeatureRow]:
    now = _now_ms()
    step = max(1, int(span.total_seconds() * 1000) // max(1, n))
    rng = random.Random(42)
    rows = []
    for i in range(n):
        ts = now - int(span.total_seconds() * 1000) + i * step + rng.randint(0, step - 1 if step > 1 else 0)
        rows.append(SequenceFeatureRow(
            timestamp=ts,
            gid=rng.randint(1, 100_000),
            action_type=rng.choice([VIEW, VIEW, CLICK, PURCHASE]),
            duration=rng.randint(1, 300),
            author_id=rng.randint(1, 1000),
        ))
    return rows


def demo() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    config = StoreConfig.from_env()
    logger.info("connecting: backend=%s ns=%s table=%s", config.backend, config.namespace, config.table)

    with FeatureStore(config) as store:
        if not store.health_check():
            logger.error("store not ready; is the %s endpoint up?", config.backend)
            return

        user = "user:u42"
        seq = sequence_key(config.namespace, "content_interaction", user)

        # 1) Ingest a long behavior history (idempotent, auto-batched).
        history = _synthetic_history(20_000, span=timedelta(days=30))
        written = store.record_interactions(seq, history)
        logger.info("ingested %d interactions into %s", written, seq)

        # 2) Recent tail for a sequence model — last 50 events in the last day.
        tail = store.recent_interactions(seq, lookback=timedelta(days=1), limit=50)
        logger.info("recent tail: %d rows", len(tail))

        # 3) Clicks only, last hour (server-side filter).
        clicks = store.recent_interactions(
            seq,
            lookback=timedelta(hours=1),
            limit=200,
            filters=[FeatureFilter("action_type", FeatureFilterOp.EQUAL, CLICK)],
        )
        logger.info("clicks in last hour: %d", len(clicks))

        # 4) Numeric observation feature + windowed read (e.g. dwell time).
        dwell_key = sequence_key(config.namespace, "dwell_ms", user)
        store.record_observations(dwell_key, [FeaturePoint(_now_ms(), "91"), FeaturePoint(_now_ms() + 1, "42")])
        dwell = store.window_values(dwell_key, lookback=timedelta(hours=1), limit=100)
        logger.info("dwell observations in window: %d", len(dwell))

        # 5) Frequency cap: count impressions in the last hour, gate at 5/hour.
        cap_key = counter_key(config.namespace, "ad_impressions", user)
        store.increment_counter(cap_key, dedup_id=str(_uuid.uuid4()))
        seen = store.counter_in_window(cap_key, lookback_units=1, unit=WindowUnit.HOUR)
        logger.info("impressions last hour: %d (under cap: %s)", seen, seen < 5)

        # 6) Entity metadata with TTL.
        meta_key = ":".join(["meta", config.namespace, user])
        store.set_metadata(meta_key, "segment", "high_value")
        store.add_tag(":".join(["tags", config.namespace, user]), "beta_ranker")
        store.expire(meta_key, ttl=timedelta(days=7))
        logger.info("segment=%s", store.get_metadata(meta_key, "segment"))

    logger.info("done")


if __name__ == "__main__":
    demo()
