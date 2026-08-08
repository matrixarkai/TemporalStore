# TemporalStore — Python SDK

A small, dependency-optional Python client for TemporalStore. Talks to the
proxy over HTTP/JSON; no build step required.

- **`TemporalFeatureStore`** — high-level, production-hardened client for
  **aggregated features** (this is what most serving code should use).
- **`ProxyClient`** — thin wrapper over the raw `/ProxyService/*` endpoints.
- **`Client`** — direct in-process client over the native library (ctypes FFI).

Requires Python 3.7+. Uses only the standard library; if [`requests`] is
installed it is used automatically for connection pooling.

## Install

```bash
# from a checkout
export PYTHONPATH=$PWD/sdk/python
# optional, for pooled connections:
pip install requests
```

## Aggregated features in 20 lines

```python
from temporalstore import TemporalFeatureStore, FeatureStoreConfig

MINUTE = 60_000
fs = TemporalFeatureStore(FeatureStoreConfig(
    endpoint="http://127.0.0.1:17102", namespace="default", table="features"))

feature_key = "feature:content_interaction:user:u42"
cs_key      = "cs:clicks:user:u42"

# dual-write each event: raw observation + Control-State rollup counter
for ts, dwell in [(1000, 12), (2000, 91), (3000, 42)]:
    fs.record_event(feature_key, cs_key, ts, metric=dwell, precision_ms=MINUTE)

# exact serving-time aggregates over any window
fs.aggregate(feature_key, 0, 10_000, "count")   # -> 3
fs.aggregate(feature_key, 0, 10_000, "sum")      # -> 145  (dwell)
fs.aggregate(feature_key, 0, 10_000, "max")      # -> 91

# long window: sealed rollup buckets + a bounded raw tail (no double count)
fs.aggregate_long_window(feature_key, cs_key,
                         window_start_ms=now-7*86_400_000, now_ms_=now,
                         op="count", precision_ms=MINUTE)

# serving control: frequency cap (allow up to 5 / day)
decision = fs.frequency_cap(cs_key, now, limit=5, window_ms=86_400_000)
# CapDecision(allowed=..., count=..., limit=5, remaining=..., reason=...)
```

### What `TemporalFeatureStore` gives you

| method | purpose |
| --- | --- |
| `append` / `append_batch` / `ingest_parallel` | write raw observations (single, batched, or concurrent) |
| `aggregate(key, s, e, op)` | exact `count/sum/min/max/avg/first/last` over a window |
| `window(key, s, e, count, filters)` | raw rows with post-decode filters |
| `record_event` | dual-write raw + Control-State rollup |
| `aggregate_long_window` | hybrid: sealed rollup buckets + exact raw tail |
| `cs_increment` / `cs_count` / `cs_family_agg` / `fol_set` / `fol_get` | Control-State counters, rollup reads, first/last |
| `frequency_cap` / `quota_remaining` / `rolling_count` | serving controls |

Production features: bounded exponential-backoff retries with jitter, per-call
timeouts, connection pooling (with `requests`), idempotency-friendly writes,
structured logging, config from `TEMPORALSTORE_*` env vars, and a pluggable
transport so the whole surface is unit-testable without a server.

## Control State (formerly "Risk")

The serving-control capability was renamed **Risk → Control State**. Use the
canonical names: `control_state_increment` / `control_state_count` on `Client`
and `ProxyClient`, and the `cs_*` / `frequency_cap` helpers on
`TemporalFeatureStore`. The engine no longer accepts the old `risk_*` command
kinds.

## Examples & tests

```bash
# runnable example (needs a local node on :17102)
TEMPORALSTORE_ENDPOINT=http://127.0.0.1:17102 \
  python sdk/python/examples/aggregated_features.py

# offline unit tests (mock transport, no server)
python tools/test_temporalstore_features.py
```

[`requests`]: https://pypi.org/project/requests/
