# TemporalStore SDKs

TemporalStore is a Rust engine. Its open-source SDK surface is **Rust and
Python**, both **Proxy SDKs** — pure-language HTTP/JSON clients that call a
TemporalStore proxy. No native library to load; the proxy owns routing, topology
refresh, retries, auth, quotas, and observability.

## Layout

| Language | Path | Binding |
|---|---|---|
| Python proxy | `sdk/python/temporalstore/proxy_client.py` | pure HTTP client |
| Python features | `sdk/python/temporalstore/features.py` | high-level aggregated-feature client |
| Rust proxy | `sdk/rust/temporalstore` (`--features proxy`) | pure HTTP client |

The HTTP/JSON contract the proxy SDKs speak is in `sdk/proxy/openapi.yaml`.

## Capabilities

The wrappers expose the TemporalStore product surface over the proxy:

- STRING / COMMON: `put_string` / `get_string`, `delete`, `expire`, `ttl`
- HASH: `hset` / `hget` / `hdel`
- SET: `sadd` / `smembers`
- FEATURE: raw feature-point add/query with filters, and exact serving-time
  aggregates (`count/sum/min/max/avg/first/last`)
- SEQUENCE FEATURE: typed long-sequence row add/query with filters
- CONTROL STATE: counters, caps, quotas, windowed counts, first/last (FOL)

The Python `TemporalFeatureStore` (`features.py`) is the production entry point
for aggregated features: append/aggregate/window, the long-window hybrid
(sealed Control-State rollup buckets + a bounded raw tail), and frequency caps.
See `sdk/python/README.md`.

## Python

```bash
export PYTHONPATH=/path/to/repo/sdk/python

# proxy SDK (no native library)
python3 sdk/python/examples/aggregated_features.py
python3 sdk/python/examples/proxy_sequence_features.py
```

Offline unit tests (mock transport, no server):

```bash
python3 tools/test_temporalstore_features.py
```

## Rust

```bash
# proxy SDK, no native linking
cd sdk/rust/temporalstore
cargo run --no-default-features --features proxy --example proxy_sequence_features
```

## Proxy API

The proxy API contract is `sdk/proxy/openapi.yaml`.

## Smoke test

Proxy SDK smoke against a mock HTTP gateway (Python + Rust):

```bash
tools/run_proxy_sdk_smoke_ubuntu22.sh
```
