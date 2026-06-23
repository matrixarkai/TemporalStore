# TemporalStore Raw C++ Direct SDK Microbenchmark

## Summary

- status: `passed`
- key_prefix: `matrixark:raw-sdk:1782235593055`
- payload_bytes: `512`
- write workers: `2`
- read workers: `4`
- write QPS: `0.531`
- read QPS: `1071.585`
- write errors: `0`
- read errors: `0`

## Latency

```json
{
  "read": {
    "avg": 0.919,
    "count": 32,
    "max": 1.865,
    "p50": 0.912,
    "p95": 1.64,
    "p99": 1.865
  },
  "write": {
    "avg": 1881.271,
    "count": 32,
    "max": 60018.148,
    "p50": 4.796,
    "p95": 11.51,
    "p99": 60018.148
  }
}
```

## Scope

This microbenchmark intentionally bypasses MatrixArk extraction, OSS models, tree traversal, token packing, and JSON record replay. It measures direct SDK hash writes and reads against the live C++ service.
