#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Edge request metrics for the `/v1/*` gateway, in Prometheus text format.

``GET /v1/metrics`` existed but carried only the ingestion-job registry: how many bulk imports had
run. The customer-facing API surface itself -- every ingest, retrieve, and memory call, their
latency, and their failures -- was invisible to the dashboards, while the engine underneath was
fully instrumented. A customer watching Grafana could therefore see storage healthy and have no way
to see their own requests timing out at the edge.

This module is the edge half. It records one observation per HTTP request and renders the standard
Prometheus shapes (counter, histogram) that a stock dashboard already knows how to chart.

Two deliberate constraints:

* **Bounded label cardinality.** The route label is the ROUTE TEMPLATE, not the path: a memory id
  or blob key in the URL collapses to ``/v1/memory/{id}``. Paths carry customer-chosen identifiers,
  so labelling by raw path would let a client create unbounded series in the operator's Prometheus
  -- a denial of service against the monitoring, not just untidy data.
* **No tenant identity.** Counters only, aggregate across keys: no key, no key id, no tenant, no
  user. That is what makes the endpoint safe to scrape without credentials, the way an exporter
  normally is. Per-key usage stays behind the admin-gated ``/v1/admin/api_key_usage``.
"""
from __future__ import annotations

import os
import threading
from collections import deque
import time
from typing import Deque, Any, Dict, List, Optional, Tuple
import sys

Json = Dict[str, Any]

# Latency buckets in seconds. Chosen for this workload rather than copied from a web default: an
# ingest fast-acks in single-digit milliseconds and a retrieve is tens of milliseconds, so the
# resolution has to be low down; the long tail matters because a silent extraction timeout shows up
# as a ~30s request.
# How many recent failures to keep. Enough to see a burst and its shape; small enough that
# the memory is a rounding error on a process that lives for weeks.
RECENT_FAILURES = 50

_BUCKETS: Tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)

# Route templates the gateway serves. Longest-prefix wins, so /v1/memories is not swallowed by the
# /v1/memory/ prefix. Anything unmatched is reported as "other" -- one series, never the raw path.
_PREFIX_ROUTES: Tuple[Tuple[str, str], ...] = (
    ("/v1/admin/ingestion/jobs/", "/v1/admin/ingestion/jobs/{id}"),
    ("/v1/admin/monitoring/", "/v1/admin/monitoring/{asset}"),
    ("/v1/memory/by-key", "/v1/memory/by-key"),
    ("/v1/memory/", "/v1/memory/{id}"),
    ("/v1/blob/", "/v1/blob/{key}"),
)
_EXACT_ROUTES = frozenset({
    "/v1", "/v1/healthz", "/v1/readyz", "/v1/metrics",
    "/v1/ingest", "/v1/ingest_file", "/v1/retrieve", "/v1/session/commit", "/v1/mcp",
    "/v1/forget", "/v1/delete", "/v1/reset", "/v1/memories", "/v1/users",
    "/v1/skills", "/v1/skills/update", "/v1/resources", "/v1/resource/content",
    "/v1/update", "/v1/memory/feedback",
    "/v1/admin", "/v1/admin/", "/v1/admin/portal", "/v1/admin/setup", "/v1/admin/catalog",
    "/v1/admin/explore", "/v1/admin/ingestion", "/v1/admin/overview",
    "/v1/admin/config", "/v1/admin/config/test", "/v1/admin/config/preset",
    "/v1/admin/scopes", "/v1/admin/embeddings",
    "/v1/admin/models",
    "/v1/admin/policy",
    "/v1/admin/ingestion/records",
    "/v1/admin/events",
    "/v1/admin/config/export",
    "/v1/admin/api", "/v1/admin/routes",
    # Documented and served, and until now unmeasured: both fell through to "other", so the
    # traffic a customer generates composing a deployment was indistinguishable from every
    # other unmatched path.
    "/v1/admin/deployment", "/v1/admin/deployment/plan",
    "/v1/admin/api_key_usage", "/v1/admin/ingestion/jobs",
    "/v1/admin/audit",
})


# Routes that stream. Their wall-clock duration is the length of a subscription, not a latency.
STREAMING_ROUTES = frozenset({"/v1/admin/events"})


def _as_ms(seconds: Optional[float]) -> Optional[float]:
    """Seconds to milliseconds, keeping None as None: unmeasured is not zero."""
    return None if seconds is None else round(seconds * 1000.0, 2)


def route_label(path: str) -> str:
    """Collapse a request path to a bounded route template (see the cardinality note above)."""
    if path in _EXACT_ROUTES:
        return path
    for prefix, template in _PREFIX_ROUTES:
        if path.startswith(prefix):
            return template
    return "other"


def bucket_quantile(buckets: List[float], quantile: float, observed_max_s: float) -> Optional[float]:
    """The bucket edge at or below which `quantile` of the observations fell, in seconds.

    A bucket edge, not an exact quantile. A histogram supports "95% finished within 250 ms"; it
    does not support "the 95th percentile was 212 ms", and a precise-looking number derived from
    bucketed counts is a claim the data cannot make.

    The last bucket is the overflow one and has no upper edge, so when the quantile lands there the
    observed maximum is returned: "slower than the largest bucket" and "unbounded" are different
    statements and only the first is true.

    None when nothing has been observed -- an empty histogram has no tail, and a zero here would
    read as a route that is very fast rather than one nobody has called.
    """
    total = sum(buckets)
    if total <= 0:
        return None
    target = total * quantile
    seen = 0.0
    for index, count in enumerate(buckets):
        seen += count
        if seen >= target:
            if index < len(_BUCKETS):
                return _BUCKETS[index]
            return observed_max_s
    return observed_max_s


class GatewayMetrics:
    """Process-wide edge counters. Every method is cheap and lock-guarded; recording never raises,
    because a metrics bug must not be able to fail a customer request."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._start = time.time()
        # (route, method, status) -> count
        self._requests: Dict[Tuple[str, str, int], int] = {}
        # route -> [bucket counts..., +Inf], sum, count, max
        self._latency: Dict[str, List[float]] = {}
        self._sum: Dict[str, float] = {}
        self._count: Dict[str, int] = {}
        self._max: Dict[str, float] = {}
        self._req_bytes: Dict[str, int] = {}
        self._resp_bytes: Dict[str, int] = {}
        self._in_flight = 0
        # The last few failures, newest last. Bounded because this is a process-lifetime structure
        # on a hot path: an unbounded list of every failure is a memory leak that only shows up on
        # the deployments having the worst day.
        #
        # Route label, method, status, time. Nothing that identifies who made the request: the
        # portal shows this to anyone who can read it, and identity added here would be invisible
        # in the panel and permanent in the process.
        self._failures: Deque[Tuple[float, str, str, int]] = deque(maxlen=RECENT_FAILURES)

        # The last datanode probe: (state, when). Set by the readiness route, published on scrape.
        # Not probed here -- a metrics endpoint that opens an outbound connection is one that can
        # hang, and scraping would put the datanode's health check on Prometheus's cadence times
        # every worker.
        self._datanode: Optional[Tuple[str, float]] = None

    # ---- recording -----------------------------------------------------------------------------
    def begin(self) -> None:
        with self._lock:
            self._in_flight += 1

    def end(self) -> None:
        with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1

    def record(self, path: str, method: str, status: int, duration_s: float,
               request_bytes: int = 0, response_bytes: int = 0) -> None:
        route = route_label(path)
        # A long-lived stream is one request that lasts minutes. Its duration is not a latency and
        # putting it in the histogram poisons every quantile drawn across routes -- a p99 of ten
        # minutes, describing nothing anyone waited for. Count the request and the bytes; skip the
        # clock.
        if route in STREAMING_ROUTES:
            duration_s = 0.0
        method = (method or "GET").upper()[:8]
        try:
            status = int(status)
        except (TypeError, ValueError):
            status = 0
        with self._lock:
            key = (route, method, status)
            self._requests[key] = self._requests.get(key, 0) + 1
            buckets = self._latency.get(route)
            if buckets is None:
                buckets = [0.0] * (len(_BUCKETS) + 1)
                self._latency[route] = buckets
            placed = False
            for index, edge in enumerate(_BUCKETS):
                if duration_s <= edge:
                    buckets[index] += 1
                    placed = True
                    break
            if not placed:
                buckets[len(_BUCKETS)] += 1
            self._sum[route] = self._sum.get(route, 0.0) + duration_s
            self._count[route] = self._count.get(route, 0) + 1
            if duration_s > self._max.get(route, 0.0):
                self._max[route] = duration_s
            if request_bytes:
                self._req_bytes[route] = self._req_bytes.get(route, 0) + int(request_bytes)
            if response_bytes:
                self._resp_bytes[route] = self._resp_bytes.get(route, 0) + int(response_bytes)
            if status >= 400:
                self._failures.append((time.time(), route, method, status))

    def note_datanode(self, state: str) -> None:
        """Record what the readiness probe just found."""
        with self._lock:
            self._datanode = (str(state), time.time())

    # ---- reading -------------------------------------------------------------------------------
    def snapshot(self) -> Json:
        """A JSON view for the portal's metrics panel (the same numbers Prometheus scrapes)."""
        with self._lock:
            routes: Dict[str, Json] = {}
            for route, count in self._count.items():
                total = float(count) or 1.0
                routes[route] = {
                    "requests": count,
                    "avg_ms": round(self._sum.get(route, 0.0) / total * 1000.0, 2),
                    "max_ms": round(self._max.get(route, 0.0) * 1000.0, 2),
                    # The tail, which the mean cannot show: fifty requests at 3 ms and one at nine
                    # seconds average out to something describing neither.
                    "p95_ms": _as_ms(bucket_quantile(self._latency.get(route, []), 0.95,
                                                     self._max.get(route, 0.0))),
                    "request_bytes": self._req_bytes.get(route, 0),
                    "response_bytes": self._resp_bytes.get(route, 0),
                    "errors": 0,
                    # What this route actually answers. Summed into one "errors" figure, a route
                    # returning 401 to a customer with the wrong key was indistinguishable from one
                    # returning 500, and both read as the gateway being broken.
                    "statuses": {},
                }
            for (route, _method, status), count in self._requests.items():
                if route not in routes:
                    continue
                statuses = routes[route]["statuses"]
                statuses[str(status)] = statuses.get(str(status), 0) + count
                if status >= 400:
                    routes[route]["errors"] += count
            return {
                "uptime_s": round(time.time() - self._start, 1),
                "in_flight": self._in_flight,
                "routes": routes,
                "total_requests": sum(self._count.values()),
                "total_errors": sum(c for (_r, _m, s), c in self._requests.items() if s >= 400),
                # Newest first, because a panel reads top-down and the useful one is the last thing
                # that happened.
                "recent_failures": [
                    {"at": round(at, 3), "route": route, "method": method, "status": status}
                    for at, route, method, status in reversed(self._failures)
                ],
            }

    def prometheus_lines(self) -> List[str]:
        with self._lock:
            requests = dict(self._requests)
            latency = {k: list(v) for k, v in self._latency.items()}
            sums = dict(self._sum)
            counts = dict(self._count)
            req_bytes = dict(self._req_bytes)
            resp_bytes = dict(self._resp_bytes)
            in_flight = self._in_flight
            start = self._start

        lines: List[str] = [
            "# HELP matrixark_gateway_start_time_seconds Unix start time of this gateway worker.",
            "# TYPE matrixark_gateway_start_time_seconds gauge",
            "matrixark_gateway_start_time_seconds %g" % start,
            "# HELP matrixark_gateway_requests_in_flight Requests currently being served.",
            "# TYPE matrixark_gateway_requests_in_flight gauge",
            "matrixark_gateway_requests_in_flight %g" % in_flight,
            "# HELP matrixark_gateway_requests_total Edge requests by route, method and status.",
            "# TYPE matrixark_gateway_requests_total counter",
        ]
        for (route, method, status), count in sorted(requests.items()):
            lines.append('matrixark_gateway_requests_total{route="%s",method="%s",status="%d"} %g'
                         % (route, method, status, count))

        lines += [
            "# HELP matrixark_gateway_request_duration_seconds Edge request latency by route.",
            "# TYPE matrixark_gateway_request_duration_seconds histogram",
        ]
        for route in sorted(latency):
            buckets = latency[route]
            cumulative = 0.0
            for index, edge in enumerate(_BUCKETS):
                cumulative += buckets[index]
                lines.append(
                    'matrixark_gateway_request_duration_seconds_bucket{route="%s",le="%g"} %g'
                    % (route, edge, cumulative))
            cumulative += buckets[len(_BUCKETS)]
            lines.append(
                'matrixark_gateway_request_duration_seconds_bucket{route="%s",le="+Inf"} %g'
                % (route, cumulative))
            lines.append('matrixark_gateway_request_duration_seconds_sum{route="%s"} %g'
                         % (route, sums.get(route, 0.0)))
            lines.append('matrixark_gateway_request_duration_seconds_count{route="%s"} %g'
                         % (route, counts.get(route, 0)))

        lines += [
            "# HELP matrixark_gateway_request_bytes_total Request body bytes accepted, by route.",
            "# TYPE matrixark_gateway_request_bytes_total counter",
        ]
        for route in sorted(req_bytes):
            lines.append('matrixark_gateway_request_bytes_total{route="%s"} %g'
                         % (route, req_bytes[route]))
        lines += [
            "# HELP matrixark_gateway_response_bytes_total Response body bytes served, by route.",
            "# TYPE matrixark_gateway_response_bytes_total counter",
        ]
        for route in sorted(resp_bytes):
            lines.append('matrixark_gateway_response_bytes_total{route="%s"} %g'
                         % (route, resp_bytes[route]))
        with self._lock:
            probed = self._datanode

        # The families are DECLARED always and SAMPLED only once something has looked.
        #
        # Those are different claims and both matter. A 0 before any probe would say the datanode
        # is down when the truth is that nobody has asked, and an alert cannot tell those apart. But
        # a family that appears only after the first probe is a family the conformance check cannot
        # find, and an alert on a series nothing declares is an alert that can never fire -- which
        # is exactly what that check refused, correctly, when this was written the other way.
        #
        # Prometheus allows HELP and TYPE with no samples, which says precisely this: the metric
        # exists and has no value yet.
        lines += [
            "# HELP matrixark_gateway_datanode_reachable Whether the datanode answered the last "
            "readiness probe (1) or could not serve (0). No sample until readiness has run.",
            "# TYPE matrixark_gateway_datanode_reachable gauge",
        ]
        if probed is not None:
            lines.append("matrixark_gateway_datanode_reachable %d"
                         % (1 if probed[0] == "ok" else 0))
        lines += [
            "# HELP matrixark_gateway_datanode_probe_age_seconds Age of that answer. A gauge with "
            "no age reads as current, and a stale 1 would say fine for ever.",
            "# TYPE matrixark_gateway_datanode_probe_age_seconds gauge",
        ]
        if probed is not None:
            lines.append("matrixark_gateway_datanode_probe_age_seconds %.3f"
                         % max(0.0, time.time() - probed[1]))

        return lines


METRICS = GatewayMetrics()


# ================================================================================================
# Configuration health, as metrics
# ================================================================================================
def config_health_lines(snapshot: Optional[Json] = None) -> List[str]:
    """Export the model-config warnings as gauges.

    The whole point of those warnings is that a misconfigured deployment is indistinguishable from a
    healthy one at the API surface: extraction and embedding both fall back to a deterministic path
    that answers 200. A dashboard that only charts request rate and latency will show such a
    deployment as perfectly healthy for months. As a gauge it is alertable:
    ``matrixark_gateway_embedding_semantic == 0`` means retrieval is running on hash vectors.
    """
    warnings: List[str] = []
    extraction_provider = (os.environ.get("MATRIXARK_UNDERSTANDING_PROVIDER")
                           or os.environ.get("MATRIXARK_EXTRACTION_PROVIDER") or "deterministic")
    embedding_provider = os.environ.get("MATRIXARK_EMBEDDING_PROVIDER", "deterministic")
    if isinstance(snapshot, dict):
        raw = snapshot.get("warnings")
        if isinstance(raw, list):
            warnings = [str(w) for w in raw]
        extraction_provider = str((snapshot.get("extraction") or {}).get("provider")
                                  or extraction_provider)
        embedding_provider = str((snapshot.get("embedding") or {}).get("provider")
                                 or embedding_provider)
    deterministic = {"", "deterministic", "rules", "local"}
    return [
        "# HELP matrixark_gateway_config_warnings Model-configuration warnings currently raised.",
        "# TYPE matrixark_gateway_config_warnings gauge",
        "matrixark_gateway_config_warnings %d" % len(warnings),
        "# HELP matrixark_gateway_extraction_model_active 1 when ingest calls a real extraction "
        "model, 0 when it runs local rules only.",
        "# TYPE matrixark_gateway_extraction_model_active gauge",
        "matrixark_gateway_extraction_model_active %d"
        % (0 if extraction_provider.strip().lower() in deterministic else 1),
        "# HELP matrixark_gateway_embedding_semantic 1 when retrieval uses a real encoder, 0 when "
        "it uses hash vectors.",
        "# TYPE matrixark_gateway_embedding_semantic gauge",
        "matrixark_gateway_embedding_semantic %d"
        % (0 if embedding_provider.strip().lower() in deterministic else 1),
    ]


def config_change_lines() -> List[str]:
    """When the stored configuration was last written, and how many settings it overrides.

    A step change in latency or recall is very often a configuration change, and the question
    "did anyone touch this deployment on Tuesday" is otherwise answered by asking people. The
    timestamp makes it a line on the same dashboard as the effect. No values, just the fact and
    the time -- the settings themselves stay behind the admin-gated read.
    """
    changed_at = 0.0
    overridden = 0
    try:
        try:
            from tools import matrixark_gateway_config as _config  # type: ignore
        except ImportError:
            import matrixark_gateway_config as _config  # type: ignore
        document = _config.load()
        changed_at = float(document.get("updated_at") or 0.0)
        overridden = len(document.get("values") or {})
    except Exception:  # pragma: no cover - metrics must never fail the scrape
        pass
    return [
        "# HELP matrixark_gateway_config_changed_timestamp_seconds Unix time the stored "
        "configuration was last written from the portal; 0 if it never was.",
        "# TYPE matrixark_gateway_config_changed_timestamp_seconds gauge",
        "matrixark_gateway_config_changed_timestamp_seconds %g" % changed_at,
        "# HELP matrixark_gateway_settings_overridden Settings whose value comes from the stored "
        "configuration rather than a built-in default.",
        "# TYPE matrixark_gateway_settings_overridden gauge",
        "matrixark_gateway_settings_overridden %d" % overridden,
    ]


def prometheus_text(config_snapshot: Optional[Json] = None,
                    extra_lines: Optional[List[str]] = None) -> str:
    """The gateway's own metrics, plus config health, plus whatever the caller appends."""
    lines = METRICS.prometheus_lines()
    lines += config_health_lines(config_snapshot)
    lines += config_change_lines()
    if extra_lines:
        lines += extra_lines
    return "\n".join(lines) + "\n"


# tools/ is importable flat (`matrixark_gateway_metrics`) and as a package
# (`tools.matrixark_gateway_metrics`). Python keeps those as two module objects, each with its own
# METRICS, so the gateway can record into one while a reader observes another and sees a route it
# served as having no samples. Which spelling loads first depends on what imported first, which is
# why the affected tests pass alone and fail inside a suite.
#
# Registering under both names makes the singleton genuinely single. `setdefault` so the first
# spelling to load wins and a later import binds to it instead of replacing it.
_MODULE_ALIASES = ("matrixark_gateway_metrics", "tools.matrixark_gateway_metrics")
for _alias in _MODULE_ALIASES:
    if _alias != __name__:
        sys.modules.setdefault(_alias, sys.modules[__name__])
