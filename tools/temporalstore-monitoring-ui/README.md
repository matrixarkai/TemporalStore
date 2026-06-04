# TemporalStore Monitoring UI

Standalone console for TemporalStore operations and observability.

Open `index.html` directly in a browser, or deploy the folder behind Nginx on the
metaserver node. The UI reads `/health.json`; when live health data is unavailable
it renders a safe pending-state sample.

Covered views:

- Cluster overview
- Node level monitoring
- Partition and replica topology
- Diagnostics and trace samples
- Workload testing status
- Dynamic runtime config values
- Scale-test summaries for primary, secondary, and TemporalAggregate workloads
- Data module testing for TemporalAggregate, Feature, IPS, and STRING

Expected optional fields in `health.json`:

- `cluster`: name, status, environment, metaserver count, data-node count
- `health`: metaserver, proxy, exporter, data nodes, shared storage, blockcache
- `runtime_config`: zone/blob sizing, replay knobs, blockcache capacities
- `nodes`: node-level status, role, endpoint, CPU, memory, storage, replay state
- `replication`: replay mode, source, secondary lag, visibility
- `scale_tests`: workload, QPS, p50, p99, secondary lag
- `module_tests`: module coverage for direct/proxy paths and latency

Generate `health.json` from a result directory:

```bash
python3 tools/temporalstore-monitoring-ui/render_health_from_results.py \
  --result-dir /tmp/temporalstore-shared-file-3node-scale-YYYYMMDD_HHMMSS \
  --template tools/temporalstore-monitoring-ui/health.json \
  --secondary-lag-ms 12 \
  --temporalaggregate-qps 150000 \
  --temporalaggregate-p99-ms 4.8 \
  --release-build ok \
  --output tools/temporalstore-monitoring-ui/health.json
```
