# Proof: Raw C++ TemporalStore Handles 8 Readers

Date: 2026-06-23

## Claim Tested

We needed to prove that the MatrixArk 8-reader retrieval tail is not evidence that the raw C++ TemporalStore storage engine cannot handle 8 concurrent readers.

## Test Boundary

- Backend: `temporalstore-direct-sdk`
- Live C++ onebox: metaserver `127.0.0.1:18000`, server `127.0.0.1:18001`
- Test tool: `tools/run_temporalstore_raw_sdk_microbench.py`
- Operations: 1,000 `hset` writes followed by 1,000 `hget` reads
- Write workers: 2
- Payload: 512 bytes
- This bypasses MatrixArk extraction, OSS models, tree traversal, token packing, ContextPackAudit writes, and Python JSON record replay.

## Results

| Read workers | Status | Read errors | Read QPS | p50 ms | p95 ms | p99 ms | Max ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | passed | 0 | 5154.925 | 1.102 | 2.754 | 4.381 | 6.684 |
| 16 | passed | 0 | 4781.272 | 2.012 | 7.133 | 10.357 | 14.748 |

## Conclusion

The raw C++ TemporalStore storage path does handle 8 concurrent readers on this local setup. The 8-reader MatrixArk retrieval tail is therefore not proven to be a raw C++ reader-cap problem.

The stronger evidence points elsewhere: MatrixArk retrieval still includes query understanding/scoring work and writes `ContextPackAudit` records, so the next bottleneck is likely product-pipeline contention, audit writes, and native context-query pushdown rather than raw hget read capacity.

## Reproduce

```bash
cd /root/src/github-services/TemporalStore
LIB=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so
PYTHONPATH=. TEMPORALSTORE_LIB="$LIB" python3 tools/run_temporalstore_raw_sdk_microbench.py \
  --ops 1000 \
  --write-workers 2 \
  --read-workers 8 \
  --payload-bytes 512 \
  --temporalstore-lib "$LIB" \
  --report-json docs/temporalstore_raw_sdk_reader_8_proof_20260623.json
```

## Artifacts

- `docs/temporalstore_raw_sdk_reader_8_proof_20260623.json`
- `docs/temporalstore_raw_sdk_reader_16_proof_20260623.json`
