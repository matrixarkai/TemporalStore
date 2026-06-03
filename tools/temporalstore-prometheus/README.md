# TemporalStore Prometheus Helper (Ubuntu local cluster)

This directory wires TemporalStore `/vars` endpoints into Prometheus using a small
Python sidecar + `node_exporter` textfile collector.

## Defaults

- Server `/vars`: `http://host.docker.internal:18001/vars`
- MetaServer `/vars`: `http://host.docker.internal:18000/vars`
- Prometheus: `http://localhost:9090`
- Node Exporter: `http://localhost:9100`

The defaults match `tools/deploy_local_ubuntu22.sh` in this repo (`SERVER_PORT=18001`,
`MS_PORT=18000`).

## Run

```bash
cd /path/to/BCache2-build-sandbox/tools/temporalstore-prometheus
docker compose up -d
```

Verify:

- Prometheus UI: <http://localhost:9090>
- Node Exporter metrics: <http://localhost:9100/metrics>

## Override targets (optional)

```bash
VARS_TARGETS="server=http://127.0.0.1:18001/vars,metaserver=http://127.0.0.1:18000/vars" \
VARS_INTERVAL_SECONDS=5 \
docker compose up -d
```

## Query checklist

See `sanity-check-queries.md` for a quick copy/paste list.

