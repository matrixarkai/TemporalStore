# TemporalStore Backlog

This backlog tracks open engineering and product work discussed during the AWS/EFS, replication, cache, SDK, and productization work. It is intentionally explicit so we can turn items into issues later.

## Current Status Snapshot - 2026-06-07

Completed since the last backlog update:

- Website and product docs:
  - MatrixArk website now uses public product names: TemporalStore, MatrixDB, and MatrixKV.
  - TemporalStore blog was expanded with architecture, write/read workflow, storage layout, replication, recovery, feature-platform positioning, LLM-context boundaries, and engineering caveats.
  - Product observability pages now exist for TemporalStore, MatrixDB, MatrixKV, and Prometheus under the website/monitoring UI source.

- Observability:
  - Prometheus-compatible text endpoint is live for TemporalStore in the AWS test cluster.
  - Current scrape sources are metaserver, data01, and data02.
  - MatrixDB and MatrixKV observability pages are prepared, but live exporter targets are not wired yet.

- Source-control hygiene:
  - Workspace AWS/deploy/package helper scripts were added under `tools/workspace/`.
  - Smoke packaging helpers and safe SSM runtime templates were added under `tools/workspace/smoke-packaging/`.
  - Runtime archives, binaries, dependency trees, logs, presigned URL files, release/debug outputs, and generated artifacts were intentionally excluded.

- AWS testing state:
  - Latest 30-minute TemporalStore service smoke kept STRING, COMMON, HASH, SET, FEATURE, IPS, and RISK stable for 1,525 iterations.
  - TemporalAggregate failed in the deployed artifact with `Request server resp size check failed`; this is still open and must not be presented as a successful aggregate scale result.
  - Two-replica table creation/recovery still has a `Missing condition info` path in the deployed runtime; secondary aggregate reads remain untrusted until fixed.

## P0 Correctness And Recovery

- Enforce one primary lease/epoch per logical shard.
  - Metaserver must be the authority for each logical shard's primary partition id and epoch.
  - Every primary promotion must create a strictly higher epoch and publish it through membership metadata.
  - Data nodes must reject writes if their local partition role or epoch is stale.
  - Clients/proxy must refresh routing after a stale-primary or epoch-mismatch error.
  - Add a split-brain guardrail test where an old primary keeps running after metaserver promotes a secondary; the old primary must reject writes or be fenced before writes can succeed.
  - Existing code has `primary_id`, `partition_set_version`, and `election_term`; harden this into an explicit lease/fencing contract.

- Add a freshness gate before secondary promotion.
  - Only promote a secondary if its replicator status is healthy.
  - Verify it has replayed through the required oplog/index-log checkpoint.
  - Verify it can serve required page streams after promotion.
  - Reject or delay promotion if the candidate is behind.

- Test primary-down failover with `PROMOTE_SECONDARY`.
  - Kill the primary data-node process.
  - Confirm metaserver freezes the old primary.
  - Confirm a secondary becomes the new primary.
  - Confirm clients refresh routing and writes go to the new primary.
  - Confirm other replicas pull from the new leader.

- Define the recovery contract for old pages.
  - Oplog alone is not enough after page/index dump.
  - Recovery needs dumped page streams, index metadata, and oplog after the checkpoint.
  - For primary-pull mode, ensure the promoted leader has local/shared page streams or add a page-copy/snapshot-copy path.

- Add a primary-pull recovery test that includes dumped historical pages.
  - Force page dump and oplog truncation.
  - Restart a secondary.
  - Verify it rebuilds from page streams plus recent oplog.
  - Repeat after primary promotion.

- Keep both replica recovery paths.
  - `secondary_pull_stream_from_primary=true`: pull index/oplog/page streams from current primary.
  - `secondary_pull_stream_from_primary=false`: read directly from shared/object store.
  - Make both paths easy to switch by table/cluster config, not only process flags.

- Define primary vs secondary read policy.
  - Primary reads are the default for freshest and safest read-after-write semantics.
  - Secondary reads are allowed only for replica-eligible, stale-tolerant reads.
  - Secondary responses must expose or be gated by freshness/lag metadata.
  - Do not route correctness-critical risk/fraud aggregate reads to secondary until replay and lag tests pass.

## P0 EFS And Shared Storage Cleanup

- Tune old blob deletion for EFS.
  - Current defaults are conservative: `stream_blob_deletion_min_age=24h`, `stream_blob_deletion_min_gap=10GB`.
  - Add EFS/test profiles with smaller values.
  - Keep production values safer until failover tests prove the retention window.

- Add shared-store orphan scanner.
  - List files under the shared store root.
  - Compare with live stream headers/index metadata.
  - Report unreferenced blobs before deleting.
  - Add a dry-run mode.

- Add EFS storage metrics.
  - Live page bytes.
  - Obsolete blob bytes.
  - Deleted bytes per minute.
  - Oldest obsolete blob age.
  - Oplog retained bytes.
  - Index-log retained bytes.

- Add an EFS cleanup guardrail test.
  - Write enough data to create multiple blobs/zones.
  - Force dump/GC/truncate.
  - Verify files are actually removed from EFS/shared-file path.
  - Verify readers still work after cleanup.

## P0 Replication And Lag Testing

- Keep the AWS primary-pull vs shared-store comparison repeatable.
  - Current result doc: `docs/primary_pull_vs_shared_store_replication_2026-06-05.md`.
  - Convert the manual SSM commands into a checked-in script.
  - Run with fixed low thread counts for 2-vCPU instances.

- Add concurrent write/read lag benchmarks.
  - STRING visibility lag under background writes and reads.
  - TemporalAggregate visibility lag under background writes and reads.
  - Sequence/Feature model lag under background writes and reads.
  - Run both no-sleep time-to-visible checks and steady-state read-latency checks.

- Separate raw read latency from visibility/retry latency.
  - Report first-attempt read latency.
  - Report retry count.
  - Report time-to-visible on secondary.

- Add lag metrics to the monitoring UI.
  - p50/p95/p99/max lag.
  - Missing keys/features during lag probe.
  - Replica replay throughput.
  - Oplog/index-log gap.

- Fix deployed TemporalAggregate replay/read path.
  - First reproduce the `Request server resp size check failed` with the deployed client/server artifacts.
  - Verify protocol/module version compatibility between client tools and server binaries.
  - Add a minimal TemporalAggregate single-key write/query smoke before returning to high-cardinality benchmarks.
  - Add a secondary replay smoke specifically for TemporalAggregate after the primary path is fixed.

## P0 Build And Release Hygiene

- Keep debug and release outputs separate. Status: partially done, keep enforcing.
  - Avoid overwriting debug and release binaries.
  - Keep `release` and `debug` folders stable.

- Reduce binary/artifact size.
  - Split dynamic libraries from executables.
  - Strip release binaries.
  - Package only needed runtime libraries.
  - Keep binaries out of Git.

- Fix runtime launch scripts to always set `LD_LIBRARY_PATH`.
  - AWS restart failed once because `libthrift.so.0.11.0` was not found.
  - Server, proxy, benchmark, and client examples should use the same runtime env file.

- Never commit third-party dependency code or generated binary artifacts.
  - No Byte/Byteraft/internal dependency code.
  - No build archives.
  - No large `.a`, `.so`, `.tar.gz`, or build directories.
  - No presigned URL files or temporary AWS security-token JSON.

- Keep packaging scripts source-controlled.
  - `tools/workspace/` now contains one-cluster AWS and packaging helpers.
  - `tools/workspace/smoke-packaging/` now contains smoke packaging helpers and safe SSM templates.
  - Next: move one-off root scripts into these folders or delete them after confirming they are obsolete.

## P1 Storage Backend Roadmap

- Harden `shared-file://` for EFS.
  - Confirm condition checks work under multi-node EFS.
  - Decide whether `flock` is needed or intentionally avoided.
  - Add compare-and-condition semantics tests.

- Keep S3/object-store backend as a future durable shared storage option.
  - Use one unified stream/store interface.
  - Support S3-compatible stores and future ByteStore-like backends.
  - Test MinIO or another S3-compatible implementation for local/dev only.

- Define backend selection.
  - Local file: single-node/dev only.
  - Shared file/EFS: AWS simple shared-storage deployment.
  - S3/object store: compute-storage separation, higher latency, stronger durability.
  - Primary-pull: catch-up/recovery optimization.

- Add retention policy by backend.
  - Local file can be aggressive.
  - EFS should balance cost and recovery retention.
  - S3 can use lifecycle policies plus stream-aware deletion.

## P1 Performance

- Tune durable write batching.
  - Current durable mode `storage_async=false` protects recovery but EFS write QPS is low.
  - Evaluate bounded async commit profiles:
    - low-latency: 1 ms or 256 KB
    - default: 2 ms or 512 KB
    - throughput: 5 ms or 1 MB
    - batch ingest: 10-50 ms or 4 MB
  - Record RPO tradeoff for each profile.

- Tune stream/blob size for high-QPS writes.
  - Too-small blobs cause frequent blob switching/freezing/opening.
  - Test larger `stream_max_blob_size` on EFS.

- Test with bigger instances and local NVMe.
  - Compare EFS durable path vs local NVMe plus primary-pull.
  - Measure whether write latency is mostly EFS sync latency.

- Add realistic high-cardinality TemporalAggregate workloads.
  - 1k, 10k, and 100k features.
  - Multiple dimensions.
  - Full-window filters.
  - Concurrent writes and reads.
  - Secondary lag under load.

## P1 Data Models

- Finish TemporalAggregate product shape.
  - Counters: count/sum/min/max.
  - Dimensions: country, merchant, campaign, action type, device type.
  - Window query: arbitrary recent windows over bucketed state.
  - Filter query: dimension predicates over bucketed state.
  - Clear semantics for raw events vs bucket increments.

- Clarify IPS vs TemporalAggregate.
  - IPS: profile/event history with richer per-event state.
  - TemporalAggregate: compact bucketed aggregate state for risk/fraud/frequency cap.
  - Document when to use each.

- Add long sequence feature benchmarks.
  - Thousands of rows per key.
  - Complex filters.
  - Windowed sequence retrieval.
  - Compare primary reads and secondary reads.

- Add JSON model decision.
  - Either implement RedisJSON-like model later or document it as out of scope for the first open-source release.

## P1 Observability And UI

- Polish monitoring UI with node-level view. Status: first multi-product version done.
  - Metaserver, proxy, and data-node status.
  - Partition role and placement.
  - Primary/secondary mapping.
  - Replay mode: primary-pull vs shared-store.
  - Next: wire live MatrixDB and MatrixKV scrape targets.

- Add scale-test panel. Status: placeholder/page structure exists, live data wiring remains.
  - Workload type.
  - Threads.
  - QPS.
  - p50/p95/p99/max latency.
  - Error count.
  - Secondary lag.

- Add data-model panel. Status: first UI copy exists, live module metrics remain.
  - STRING.
  - HASH.
  - Feature/sequence.
  - IPS.
  - TemporalAggregate.

- Add GC/storage panel.
  - EFS bytes.
  - Page-store live bytes.
  - Obsolete bytes.
  - Oplog retained bytes.
  - Last GC time.
  - Delete failures.

## P1 SDK And Client Experience

- Stabilize direct SDK and proxy SDK.
  - C++ dynamic/static libraries.
  - Python client.
  - Go client.
  - Java client.
  - Rust client.

- Document production client behavior.
  - Routing refresh.
  - Primary-pinned writes.
  - Secondary-eligible reads.
  - Retry policy.
  - Visibility semantics.

- Add proxy test coverage.
  - STRING and HASH smoke already passed.
  - FeatureQuery through proxy returned zero points before and needs debugging.

## P1 Cloud Deployment

- Keep AWS Terraform small for testing.
  - One metaserver/proxy/client node.
  - Two data nodes.
  - No EFS mount on metaserver unless it also runs a data node.
  - Reuse existing TemporalStore cluster for tests.

- Add deployment runbook.
  - Apply Terraform.
  - Upload binaries.
  - Start metaserver/proxy/data nodes.
  - Run smoke tests.
  - Run scale tests.
  - Collect results.
  - Destroy or keep resources intentionally.

- Add cost controls.
  - Instance stop automation.
  - Clear owner/project tags.
  - EFS cleanup and size alarms.
  - Document that stopped EC2 does not charge compute but EBS/EFS/IPs may still charge.

## P2 Product And Documentation

- Update the one-cluster TemporalStore doc with latest AWS numbers. Status: partially done.
  - Primary-pull vs shared-store comparison.
  - EFS write latency caveats.
  - Secondary lag numbers.
  - Add the newest Prometheus/live-observability screenshots or endpoint references.

- Update the blog/product docs. Status: TemporalStore deep-dive blog done; keep product pages current.
  - High-cardinality temporal features.
  - Fraud/risk/frequency cap examples.
  - Why not plain Redis/ordinary KV for arbitrary windows and filtered aggregates.
  - Storage cleanup story for durable online serving.
  - Add a shorter executive version for non-technical buyers.

- Add architecture diagrams. Status: blog/website diagrams added; deeper docs still need diagrams.
  - Metaserver/proxy/data-node request flow.
  - Primary-pull replication.
  - Shared-store recovery.
  - Page/index/oplog cleanup.
  - Failover and promotion.

- Add model-extension UI/DSL design.
  - Register custom model.
  - Register UDF.
  - Validate schema.
  - Test model in sandbox.
  - Deploy model version.

## P2 Future Areas

- Multi-tenancy.
  - Namespace isolation.
  - Per-tenant quota.
  - Placement policy.
  - Metrics and billing labels.

- Multi-region.
  - Async replication first.
  - Conflict policy for active-active only if needed.
  - CRDT support is not a near-term requirement.

- LLM context/cache exploration.
  - Temporal context/state store.
  - Integration under context retrieval systems.
  - LMCache-compatible remote storage interface only if it maps cleanly.
  - Do not position as GPU KV-cache replacement without tensor-aware cache semantics.

- GPU-specific models.
  - Most TemporalStore data models do not need GPU compute.
  - GPU work may matter only for vector/tensor transformations or embedding/reranking pipelines, not the core temporal storage engine.
