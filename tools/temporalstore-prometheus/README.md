# TemporalStore Prometheus Helper (Ubuntu local cluster)

This directory wires TemporalStore `/vars` endpoints into Prometheus using a small
Python sidecar + `node_exporter` textfile collector.

The sidecar writes both converted TemporalStore metrics and exporter health metrics:

- `temporalstore_vars_exporter_target_up{service_role,source,url}` is `1` when a `/vars` target was scraped.
- `temporalstore_vars_exporter_target_samples_scraped{service_role,source,url}` reports converted samples.
- `temporalstore_vars_exporter_scrape_errors_total` reports failed targets in the last scrape cycle.
- `temporalstore_service_role_up{service_role,source}` gives a stable role-level health signal.
- `temporalstore_vars_exporter_role_samples_scraped{service_role}` counts converted samples by role.
- `temporalstore_client_validation_up{service_role="client",iteration}` is written by the local validation harness.
- `temporalstore_client_benchmark_qps{service_role="client",phase,threads,iteration}` is written when `RUN_CLIENT_SCALE=1`.
- `temporalstore_raft_gate_*` metrics are written by
  `tools/run_raft_stress_suite_ubuntu22.sh` after raft/failover gates finish,
  including failed gates.
- Converted server/metaserver metrics keep their source label, for example
  `bcache2_server_*{service_role="nodeserver",source="nodeserver"}` and
  `bcache2_metaserver_*{service_role="metaserver",source="metaserver"}`.
- `temporalstore-alerts.yml` installs local production-readiness alerts for
  service availability, exporter scrape errors, client validation, proxy smoke,
  client benchmark errors, and raft gate readiness.

## Defaults

- Server `/vars`: `http://host.docker.internal:18001/vars`
- MetaServer `/vars`: `http://host.docker.internal:18000/vars`
- Proxy `/vars`: `http://host.docker.internal:18090/vars`
- Prometheus: `http://localhost:9090`
- Node Exporter: `http://localhost:9100`

The defaults match the local validation harness:
`tools/run_prometheus_local_ubuntu22.sh`.

## Run

```bash
cd /path/to/BCache2-build-sandbox/tools/temporalstore-prometheus
docker compose up -d
```

Verify:

- Prometheus UI: <http://localhost:9090>
- Node Exporter metrics: <http://localhost:9100/metrics>
- Prometheus query: `temporalstore_vars_exporter_target_up`
- Alert rules: <http://localhost:9090/rules>

## End-to-end local validation

This starts a local C++ cluster, uses the successful smoke deployment as the
client signal, writes client textfile metrics, starts Prometheus, and checks
Prometheus queries twice. If the `bcache2-proxy` release artifact is present, it
also starts proxy and validates proxy `/vars` scraping.

```bash
BUILD_TYPE=Release ITERATIONS=2 bash tools/run_prometheus_local_ubuntu22.sh
```

Raft/fault-tolerance gates publish their latest result into the same textfile
collector directory:

```bash
BUILD_TYPE=Release RUN_BUILD=0 RUN_UNIT=0 RUN_API=0 RUN_PROMETHEUS=0 \
  RUN_INGESTION=0 RUN_REDIS=0 RUN_RAFT=1 \
  bash tools/run_production_readiness_local_ubuntu22.sh
```

## Override targets (optional)

```bash
VARS_TARGETS="nodeserver=http://127.0.0.1:18001/vars,metaserver=http://127.0.0.1:18000/vars,proxy=http://127.0.0.1:18090/vars" \
VARS_INTERVAL_SECONDS=5 \
docker compose up -d
```

## Query checklist

See `sanity-check-queries.md` for a quick copy/paste list.

## Alert rules

The bundled alert file is intentionally local-first and gate-friendly. It alerts
on missing services, failed `/vars` scraping, failed client/proxy validation, and
raft production-gate failures when those textfile metrics are present:

```bash
docker compose exec prometheus promtool check rules /etc/prometheus/temporalstore-alerts.yml
```
