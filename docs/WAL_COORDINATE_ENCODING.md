# WAL Coordinate Encoding

Index snapshots retain a log ID and sequence for each WAL-resident block. Both
coordinates remain required: reads need the record location, and reclamation
needs its sequence. The resident record still occupies sixteen bytes.

Snapshot writers now encode each record as `[log_id, sequence]` instead of
repeating the field names in a map. The existing derived struct decoder accepts
both representations, so old named rows and ordered pairs keep the same meaning.
Tests exercise that reader shape directly in JSON and MessagePack, including zero
and maximum integers. Missing coordinates, extra pair elements, invalid values
and duplicate named fields are rejected.

For an 8,000-row registry fixture:

| Encoding | Named Rows | Ordered Pairs | Saved |
|---|---:|---:|---:|
| JSON | 453954 | 293954 | 160000 |
| Named MessagePack | 271127 | 143127 | 128000 |

These are uncompressed registry bytes, not total snapshot size or compressed
storage savings. Resident memory size is unchanged. The change does not remove
recovery metadata, change WAL records, advance apply fences or relax reclamation.
It does not establish lower API p99 latency or resolve existing write failures.

```bash
cargo test -p temporalstore-rust --lib wal_resident_wire -- --test-threads=1
```
