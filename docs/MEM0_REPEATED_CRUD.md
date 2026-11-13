# Repeated Memory API Workloads

Use the repository's Rust workload driver and local services:

```bash
bash scripts/mem0_scale_local.sh --infer false \
  --records 100000 --batch-size 128 --tenant-shards 16 --users 1000 \
  --searches 256 --payload-bytes 512 --crud-rounds 128
```

`--crud-rounds` defaults to zero, preserving the existing workload. Each enabled
round seeds twenty fresh memories under a distinct user in one corpus tenant.
It measures add, update, get, history, feedback, list, users, sixteen-item batch
update, sixteen-item batch delete, single delete and user-scoped delete-all.
The main corpus remains present during these calls. Rounds rotate across corpus
tenants and execute sequentially; this is not a concurrent-client throughput test.

Each round also seeds two memories in a separate reset tenant, then measures
reset. Reset latency therefore describes a two-record tenant, not destruction
of the large corpus. Setup for this reset fixture is not part of reset latency.
Each round adds twenty-two fixture records beyond `requested_records`; the main
ingest count and payload-byte denominator do not include these extra records.
Lifecycle storage amplification includes their surviving history and tombstones.

The report records `crud_rounds` and separate `crud_*` latency/error distributions.
The existing corpus add/search and one-shot management metrics are unchanged.
Extra verification metrics cover batch readback and empty lists after scoped
delete-all/reset. Exact mutation counts, unique add IDs, scoped list membership,
update readback, update history and feedback acknowledgments are checked.
Batch readback samples one updated member, not every batch member.
Each required stage must have exactly the configured number of samples; failed
setup cannot silently reduce coverage. Transport and semantic failures are both
counted as request failures, and the process exits nonzero on any failure.

The provider and `infer` setting apply to these writes as well as corpus ingest.
Inference-disabled deterministic runs isolate serving/storage work; they do not
establish model quality or extraction throughput. Search checks remain bounded,
nonempty candidate checks rather than semantic recall. A single request cannot
establish a p99 distribution; use repeated rounds and inspect error counts too.
