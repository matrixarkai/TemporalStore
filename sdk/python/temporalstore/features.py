"""
temporalstore_features — production client for TemporalStore aggregated features.

A hardened, dependency-optional Python client for the TemporalStore proxy that
implements the aggregated-feature serving patterns:

  * raw Feature long-sequence append + windowed / filtered reads
  * exact serving-time FeatureAggregate (count/sum/min/max/avg/first/last)
  * the long-window HYBRID: sealed Control-State rollup buckets + a bounded
    raw-sequence tail, composed with the no-double-count boundary invariant
  * Control-State serving controls: frequency caps, quotas, rolling counts,
    latest-value (FOL)

Design goals (production readiness):
  - Zero hard dependencies (stdlib urllib), with automatic use of `requests`
    (connection pooling) when it is installed.
  - Bounded retries with exponential backoff + jitter on transient failures.
  - Per-call timeouts, structured logging, typed errors, idempotency keys.
  - Config from explicit args or environment; usable as a context manager.
  - Pluggable transport so the whole surface is unit-testable without a server.

Wire contract (verified against origin/main):
  POST /ProxyService/<Op>  body = {namespace, table_name, key, ...}
  reply = {"status":{"ok":bool,"message":str}, "response":{"kind":..., ...}}
  aggregate/integer response -> {"kind":"aggregate"|"integer","value":<int>}

Command kinds are the current ControlState* surface (the older risk_* kinds are
no longer accepted by the engine).
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

log = logging.getLogger("temporalstore.features")

# Optional high-performance transport.
try:  # pragma: no cover - exercised only when requests is installed
    import requests
    from requests.adapters import HTTPAdapter

    try:
        from urllib3.util.retry import Retry
    except Exception:  # urllib3 shipped with requests
        Retry = None  # type: ignore
    _HAVE_REQUESTS = True
except Exception:
    _HAVE_REQUESTS = False


# --------------------------------------------------------------------------- errors
class TemporalStoreError(RuntimeError):
    """Raised when the proxy returns a non-ok status or transport fails."""

    def __init__(self, code: int, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message


class TransientError(TemporalStoreError):
    """A retryable transport/server error (timeout, connection reset, 5xx)."""


# --------------------------------------------------------------------------- config
@dataclass
class Config:
    endpoint: str = "http://127.0.0.1:17102"
    namespace: str = "default"
    table: str = "features"
    api_key: str = ""
    timeout_s: float = 5.0
    max_retries: int = 4
    backoff_base_s: float = 0.05
    backoff_max_s: float = 2.0
    pool_connections: int = 32

    @classmethod
    def from_env(cls, prefix: str = "TEMPORALSTORE_") -> "Config":
        g = lambda k, d: os.environ.get(prefix + k, d)
        return cls(
            endpoint=g("ENDPOINT", cls.endpoint),
            namespace=g("NAMESPACE", cls.namespace),
            table=g("TABLE", cls.table),
            api_key=g("API_KEY", ""),
            timeout_s=float(g("TIMEOUT_S", cls.timeout_s)),
            max_retries=int(g("MAX_RETRIES", cls.max_retries)),
        )


# ------------------------------------------------------------------------- transport
class Transport(Protocol):
    def post(self, url: str, body: bytes, headers: Dict[str, str], timeout_s: float) -> Tuple[int, bytes]:
        ...

    def close(self) -> None:
        ...


class _UrllibTransport:
    """Stdlib transport. No pooling; retries are handled by the client."""

    def post(self, url, body, headers, timeout_s):
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise TransientError(0, f"transport: {exc}") from exc

    def close(self):
        pass


class _RequestsTransport:
    """requests.Session with a connection pool (keep-alive)."""

    def __init__(self, pool: int):
        self.s = requests.Session()
        adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=0)
        self.s.mount("http://", adapter)
        self.s.mount("https://", adapter)

    def post(self, url, body, headers, timeout_s):
        try:
            r = self.s.post(url, data=body, headers=headers, timeout=timeout_s)
            return r.status_code, r.content
        except requests.RequestException as exc:
            raise TransientError(0, f"transport: {exc}") from exc

    def close(self):
        self.s.close()


def _default_transport(cfg: Config) -> Transport:
    if _HAVE_REQUESTS:
        return _RequestsTransport(cfg.pool_connections)
    return _UrllibTransport()


# ------------------------------------------------------------------------- helpers
def now_ms() -> int:
    return int(time.time() * 1000)


def bucket_floor(ts_ms: int, precision_ms: int) -> int:
    """Start of the precision bucket containing ts_ms (the sealing boundary)."""
    return ts_ms - (ts_ms % precision_ms)


def _metric_bytes(value: Any) -> List[int]:
    """Feature/CS values are stored as bytes; numeric aggregates parse ASCII."""
    if isinstance(value, (bytes, bytearray)):
        return list(value)
    if isinstance(value, int):
        return list(str(value).encode())
    return list(str(value).encode())


def _bytes_to_str(value: Any) -> str:
    if isinstance(value, list):
        return bytes(int(b) & 0xFF for b in value).decode("utf-8", "replace")
    return str(value)


@dataclass
class Observation:
    timestamp_ms: int
    value: Any = 1  # the metric; use 1 for pure counting


@dataclass
class CapDecision:
    allowed: bool
    count: int
    limit: int
    remaining: int
    reason: str = ""


# ------------------------------------------------------------------------- client
class TemporalFeatureStore:
    """High-level, production-hardened client for aggregated-feature workloads."""

    AGGREGATORS = ("count", "sum", "min", "max", "avg", "first", "last")

    def __init__(self, config: Optional[Config] = None, transport: Optional[Transport] = None):
        self.cfg = config or Config.from_env()
        self.endpoint = self.cfg.endpoint.rstrip("/")
        self.transport = transport or _default_transport(self.cfg)

    # -- context manager -------------------------------------------------------
    def __enter__(self) -> "TemporalFeatureStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        try:
            self.transport.close()
        except Exception:
            pass

    # -- transport core --------------------------------------------------------
    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.cfg.api_key:
            h["Authorization"] = "Bearer " + self.cfg.api_key
        return h

    def _post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST with bounded exponential-backoff retry; returns flattened response."""
        raw = json.dumps(body).encode("utf-8")
        url = self.endpoint + path
        attempt = 0
        while True:
            attempt += 1
            try:
                status, payload = self.transport.post(url, raw, self._headers(), self.cfg.timeout_s)
                if status >= 500:
                    raise TransientError(status, payload[:200].decode("utf-8", "replace"))
                if status >= 400:
                    raise TemporalStoreError(status, payload[:500].decode("utf-8", "replace"))
                return self._unwrap(json.loads(payload.decode("utf-8")))
            except TransientError as exc:
                if attempt > self.cfg.max_retries:
                    log.error("giving up on %s after %d attempts: %s", path, attempt, exc)
                    raise
                delay = min(self.cfg.backoff_max_s, self.cfg.backoff_base_s * (2 ** (attempt - 1)))
                delay += random.uniform(0, delay)  # full jitter
                log.warning("retrying %s (attempt %d): %s; sleeping %.3fs", path, attempt, exc, delay)
                time.sleep(delay)

    @staticmethod
    def _unwrap(payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        status = payload.get("status") or {}
        if "status" in payload and not status.get("ok", False):
            raise TemporalStoreError(0, str(status.get("message", "request failed")))
        resp = payload.get("response", payload)
        if not isinstance(resp, dict):
            return {}
        kind = resp.get("kind")
        if kind in {"integer", "aggregate"}:
            v = int(resp.get("value", 0))
            return {"value": v}
        if kind == "feature_points":
            return {"points": [
                {"timestamp_ms": int(p.get("timestamp_ms", p.get("timestamp", 0))),
                 "value": _bytes_to_str(p.get("value", ""))}
                for p in resp.get("points", [])
            ]}
        if kind == "bytes":
            return {"value": _bytes_to_str(resp.get("value", []))}
        return resp

    def _base(self, key: str) -> Dict[str, Any]:
        return {"namespace": self.cfg.namespace, "table_name": self.cfg.table, "key": key}

    # -- health ----------------------------------------------------------------
    def health(self) -> bool:
        try:
            status, _ = self.transport.post(
                self.endpoint + "/health", b"", {}, self.cfg.timeout_s
            )
            return 200 <= status < 300
        except Exception:
            # /health may be GET-only; fall back to a cheap agg on a sentinel key.
            try:
                self.aggregate("__health__", 0, 1, "count")
                return True
            except TemporalStoreError:
                return False

    # ======================================================= raw Feature sequence
    def append(self, feature_key: str, timestamp_ms: int, value: Any = 1) -> None:
        self.append_batch(feature_key, [Observation(timestamp_ms, value)])

    def append_batch(self, feature_key: str, observations: Sequence[Observation]) -> None:
        body = self._base(feature_key)
        body["points"] = [
            {"timestamp_ms": int(o.timestamp_ms), "value": _metric_bytes(o.value)}
            for o in observations
        ]
        self._post("/ProxyService/FeatureAdd", body)

    def ingest_parallel(
        self, items: Iterable[Tuple[str, Sequence[Observation]]], workers: int = 8
    ) -> int:
        """Bulk-ingest many (key, observations) pairs concurrently. Returns #keys."""
        items = list(items)
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {pool.submit(self.append_batch, k, obs): k for k, obs in items}
            for fut in as_completed(futs):
                fut.result()  # re-raise on failure
                done += 1
        return done

    def window(
        self,
        feature_key: str,
        start_ms: int,
        end_ms: int,
        count: Optional[int] = None,
        filters: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        body = self._base(feature_key)
        body.update({"start_ms": int(start_ms), "end_ms": int(end_ms)})
        if count is not None:
            body["count"] = int(count)
        if filters:
            body["filters"] = filters
        return self._post("/ProxyService/FeatureQuery", body).get("points", [])

    def aggregate(self, feature_key: str, start_ms: int, end_ms: int, op: str = "count") -> int:
        if op not in self.AGGREGATORS:
            raise ValueError(f"unknown aggregator {op!r}; use one of {self.AGGREGATORS}")
        body = self._base(feature_key)
        body.update({"start_ms": int(start_ms), "end_ms": int(end_ms), "aggregator": op})
        return int(self._post("/ProxyService/FeatureAggQuery", body).get("value", 0))

    # ============================================================= Control State
    def cs_increment(
        self,
        cs_key: str,
        timestamp_ms: int,
        amount: int = 1,
        precision_ms: Optional[int] = None,
        ttl_ms: Optional[int] = None,
    ) -> None:
        body = self._base(cs_key)
        body.update({"timestamp_ms": int(timestamp_ms), "amount": int(amount)})
        if precision_ms is not None:
            body["precision_ms"] = int(precision_ms)
        if ttl_ms is not None:
            body["ttl_ms"] = int(ttl_ms)
        self._post("/ProxyService/ControlStateIncrement", body)

    def cs_count(self, cs_key: str, start_ms: int, end_ms: int) -> int:
        body = self._base(cs_key)
        body.update({"start_ms": int(start_ms), "end_ms": int(end_ms)})
        return int(self._post("/ProxyService/ControlStateCount", body).get("value", 0))

    def cs_family_agg(
        self, cs_key: str, start_ms: int, end_ms: int, aggregator: str, family: str = "h"
    ) -> int:
        """Rollup aggregate (sum/min/max/change) via the generic table command.

        ``family`` is the on-the-wire family tag and stays ``"h"`` / ``"cpc"`` /
        ``"fol"`` (the Counter / Distinct / Selection families, respectively);
        these serde-pinned tags are the correct wire values and are unchanged by
        the family rename.
        """
        body = self._base(cs_key)
        body["command"] = {
            "kind": "control_state_family_query",
            "family": family,
            "key": cs_key,
            "start_ms": int(start_ms),
            "end_ms": int(end_ms),
            "aggregator": aggregator,
        }
        return int(self._post("/ProxyService/ExecuteTableCmd", body).get("value", 0))

    def fol_set(self, cs_key: str, value: Any, occur_time_ms: int, ttl_ms: int, fol_type: str = "last") -> None:
        """Set the first/last value for a key (the **Selection** family).

        Maps to the ``ControlStateSelectionSet`` route (formerly ``...FolSet``);
        the ``selection_type`` field (formerly ``fol_type``) selects first vs.
        last. The method name is kept for API stability.
        """
        body = self._base(cs_key)
        body.update({
            "value": _metric_bytes(value),
            "occur_time_ms": int(occur_time_ms),
            "ttl_ms": int(ttl_ms),
            "selection_type": fol_type,
        })
        self._post("/ProxyService/ControlStateSelectionSet", body)

    def fol_get(self, cs_key: str) -> Optional[str]:
        """Read the stored first/last value (the **Selection** family).

        Maps to the ``ControlStateSelectionQuery`` route (formerly ``...FolQuery``).
        """
        v = self._post("/ProxyService/ControlStateSelectionQuery", self._base(cs_key)).get("value")
        return None if v in (None, "") else _bytes_to_str(v)

    # ==================================================== dual-write + hybrid read
    def record_event(
        self,
        feature_key: str,
        cs_key: str,
        timestamp_ms: int,
        metric: int = 1,
        precision_ms: int = 60_000,
        ttl_ms: int = 8 * 24 * 3_600_000,
    ) -> None:
        """Dual-write one event: raw observation + rollup increment (by `metric`)."""
        self.append(feature_key, timestamp_ms, metric)
        self.cs_increment(cs_key, timestamp_ms, amount=int(metric), precision_ms=precision_ms, ttl_ms=ttl_ms)

    def aggregate_long_window(
        self,
        feature_key: str,
        cs_key: str,
        window_start_ms: int,
        now_ms_: Optional[int] = None,
        op: str = "count",
        precision_ms: int = 60_000,
        fallback_raw: bool = True,
    ) -> int:
        """
        Exact long-window aggregate = sealed Control-State buckets + raw tail.

        Partition: sealed=[window_start, tail_start), raw=[tail_start, now]. The
        boundary is a bucket edge so the two never overlap and never gap.
        Supports count/sum/min/max/avg.
        """
        end = now_ms() if now_ms_ is None else int(now_ms_)
        tail_start = bucket_floor(end, precision_ms)
        # never let the sealed range invert if the whole window is inside the tail
        sealed_end = max(window_start_ms, tail_start)  # exclusive boundary

        def raw_tail(o: str) -> int:
            return self.aggregate(feature_key, tail_start, end, o)

        try:
            if op == "count":
                sealed = self.cs_count(cs_key, window_start_ms, sealed_end - 1) if sealed_end > window_start_ms else 0
                return sealed + raw_tail("count")
            if op == "sum":
                sealed = self.cs_family_agg(cs_key, window_start_ms, sealed_end - 1, "sum") if sealed_end > window_start_ms else 0
                return sealed + raw_tail("sum")
            if op in ("min", "max"):
                vals = []
                if sealed_end > window_start_ms:
                    vals.append(self.cs_family_agg(cs_key, window_start_ms, sealed_end - 1, op))
                vals.append(raw_tail(op))
                return (min if op == "min" else max)(vals)
            if op == "avg":
                s_sum = self.cs_family_agg(cs_key, window_start_ms, sealed_end - 1, "sum") if sealed_end > window_start_ms else 0
                s_cnt = self.cs_count(cs_key, window_start_ms, sealed_end - 1) if sealed_end > window_start_ms else 0
                t_sum, t_cnt = raw_tail("sum"), raw_tail("count")
                total = s_cnt + t_cnt
                return (s_sum + t_sum) // total if total else 0
            raise ValueError(f"hybrid does not support op {op!r}")
        except TemporalStoreError:
            if not fallback_raw:
                raise
            log.warning("rollup read failed; falling back to bounded raw scan for %s", feature_key)
            return self.aggregate(feature_key, window_start_ms, end, op)

    # ============================================================== use-case sugar
    def rolling_count(self, cs_key: str, window_ms: int, now_ms_: Optional[int] = None) -> int:
        end = now_ms() if now_ms_ is None else int(now_ms_)
        return self.cs_count(cs_key, end - window_ms, end)

    def frequency_cap(
        self,
        cs_key: str,
        timestamp_ms: int,
        limit: int,
        window_ms: int,
        precision_ms: int = 60_000,
        ttl_ms: Optional[int] = None,
    ) -> CapDecision:
        """
        Frequency cap: read current count in the window, allow-and-increment if
        under the limit. The single-key increment is atomic server-side; for
        strict correctness under heavy contention prefer a server set-and-get.
        """
        count = self.cs_count(cs_key, timestamp_ms - window_ms, timestamp_ms)
        if count < limit:
            self.cs_increment(cs_key, timestamp_ms, 1, precision_ms, ttl_ms or window_ms * 2)
            return CapDecision(True, count + 1, limit, limit - count - 1)
        return CapDecision(False, count, limit, 0, "frequency_cap_exceeded")

    def quota_remaining(self, cs_key: str, start_ms: int, end_ms: int, limit: int) -> int:
        return max(0, limit - self.cs_count(cs_key, start_ms, end_ms))
