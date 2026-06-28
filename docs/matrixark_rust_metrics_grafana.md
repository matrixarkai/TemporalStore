# MatrixArk Rust Metrics For Grafana

The Rust MatrixArk record-log bridge now emits Prometheus-compatible metrics for
the long-lived `matrixark_rust_proxy --serve` path. The legacy
`matrixark_record_log` name is a compatibility/debug alias.

## Metrics Surface

Use the control command:

```json
{"op":"metrics_prometheus"}
```

The response contains a `prometheus` string with text-format metrics.

For textfile-collector style deployments, set:

```bash
export MATRIXARK_RUST_METRICS_PATH=/var/lib/node_exporter/textfile_collector/matrixark_rust_record_log.prom
```

The bridge rewrites that file after parse errors, client creation, connection
errors, and every executed storage command.

## Current Metrics

- `matrixark_rust_record_log_process_start_time_ms`
- `matrixark_rust_record_log_commands_total{op,status}`
- `matrixark_rust_record_log_command_latency_ms_sum{op}`
- `matrixark_rust_record_log_command_latency_ms_max{op}`
- `matrixark_rust_record_log_records_written_total`
- `matrixark_rust_record_log_records_read_total`
- `matrixark_rust_record_log_bytes_written_total`
- `matrixark_rust_record_log_bytes_read_total`
- `matrixark_rust_record_log_clients_created_total`
- `matrixark_rust_record_log_parse_errors_total`
- `matrixark_rust_record_log_client_connect_errors_total`
- `matrixark_rust_record_log_commands_failed_total`

## Grafana Panels To Add

- Rust bridge request rate by op:
  `rate(matrixark_rust_record_log_commands_total[1m])`
- Rust bridge error rate:
  `rate(matrixark_rust_record_log_commands_total{status="error"}[1m])`
- Records written/read per second:
  `rate(matrixark_rust_record_log_records_written_total[1m])`
  and `rate(matrixark_rust_record_log_records_read_total[1m])`
- Approximate payload throughput:
  `rate(matrixark_rust_record_log_bytes_written_total[1m])`
  and `rate(matrixark_rust_record_log_bytes_read_total[1m])`
- Max observed bridge latency by op:
  `matrixark_rust_record_log_command_latency_ms_max`

## Notes

This is process-local bridge telemetry. It complements the native C++ server
metrics such as oplogger, page store, index, raft, and storage manager metrics.
For production Rust parity, the next step is to expose the same metrics from a
Rust proxy instead of CLI-per-operation paths.
