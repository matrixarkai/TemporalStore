# Sanity-check PromQL for TemporalStore `/vars`

Use these in Prometheus (`http://localhost:9090`) after 30–60 seconds.

1. Collector + target healthy

```promql
up{job="temporalstore_node_exporter"}
```

2. Textfile collector has fresh data

```promql
time() - node_textfile_mtime_seconds
```

3. We can see server metrics and meta metrics separately

```promql
count({__name__=~"bcache2_.*"})
```

```promql
sum by (source) ({__name__=~"bcache2_server_.*"})
```

```promql
sum by (source) ({__name__=~"bcache2_metaserver_.*"})
```

4. Core runtime counters/throughput should move with traffic

```promql
sum by (source) (increase({__name__=~".*_success.*"}[5m]))
```

```promql
sum by (source) (increase({__name__=~".*_qps.*"}[1m]))
```

5. Watch hot storage/repl/cmd classes

```promql
sum by (source) ({__name__=~".*(index|slot|page|replicator|gc|oplogger).*"})
```

If these series stay empty, re-check:

- `VARS_TARGETS` points to running TemporalStore ports
- Local cluster is actually started (default ports `18001/18000`)
- Exporter container can reach `host.docker.internal`
