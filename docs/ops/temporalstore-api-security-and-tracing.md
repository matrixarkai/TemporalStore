# TemporalStore API Security And Tracing

This page records the production replacement contract for non-Raft service APIs.
Raft peer transport has a separate mTLS/auth readiness gate; this page covers
client, proxy, metaserver, data-node, Redis-compatible, ingestion, and admin
HTTP/JSON or tonic-facing APIs.

## Required API Controls

- `TS_API_AUTH_TOKEN`: shared bearer token for HTTP/JSON admin and service APIs
  in local or Docker production validation.
- TLS termination: required at the service edge for proxy, server, metaserver,
  Redis-compatible ingress, ingestion, and admin paths. Local plaintext is only
  allowed for developer harnesses.
- `trace_id`: accepted on command requests and propagated through client,
  proxy, data-node, storage, Raft proposal evidence, shared-store replay, and
  readiness logs.
- `request_id`: required in API gateway/proxy logs, retry logs, scheduler task
  execution records, and ingestion dead-letter reports.
- OpenTelemetry: spans should use service names `temporalstore-client`,
  `temporalstore-proxy`, `temporalstore-metaserver`, `temporalstore-datanode`,
  and `temporalstore-ingestion`.

## Evidence Commands

```bash
tools/run_ops_scale_readiness.sh
tools/run_ops_scale_readiness.sh --run-local-scale
tools/run_ops_scale_readiness.sh --run-distributed-raft
```

The readiness harness validates that the non-Raft auth/TLS contract, tracing
fields, dashboard, alerts, runbooks, Docker/local scale path, distributed Raft
load path, and unified C++/Rust workload corpus are present.

## Release Checklist

- Proxy and server reject missing or invalid `TS_API_AUTH_TOKEN` when the
  deployment requires API authentication.
- TLS is terminated before traffic reaches proxy, data-node, metaserver,
  ingestion, Redis-compatible, and admin endpoints.
- Traces carry `trace_id` and `request_id` across retries and topology refresh.
- Dashboard panels expose readiness blockers, Raft lag, route quarantine,
  scheduler backlog, ingestion lag, storage/cache recovery errors, and scale SLOs.
