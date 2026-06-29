# Raft Production-Ready Attempt - 2026-06-08

## Request

Make TemporalStore data-node Raft production-ready, avoid regressions on the shared-storage path, and test on AWS.

## What Was Completed Before This Attempt

- Commit `1445c42` was already pushed to the private `main-no-deps` branch.
- That commit hardened the data-Raft replica apply path:
  - data-Raft snapshot/load callbacks are wired from the RustRaft FSM to `Partition`;
  - a per-partition applied-index sidecar is restored and advanced under `--data_raft_work_dir/applied/<partition_id>`;
  - data-Raft replicas load local streams instead of restoring primary/shared-store stream metadata;
  - data-Raft replicas open local streams writable for committed FSM apply;
  - committed FSM apply bypasses local readonly/quota admission so quorum-committed entries apply deterministically.

## Build Attempt

Fresh release configure from the clean no-deps repo was attempted with:

```bash
cmake -S . -B output-ubuntu22/release \
  -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_MTCACHE=ON \
  -DENABLE_MTCACHE_SSD_CACHE=ON
```

Result: build is blocked before compile.

The clean repo intentionally has no dependency tree. A local-only `thirdparty` bridge was attempted from `local_only/BCache2-local-scrubbed`, but that scrubbed dependency tree contains empty submodule placeholders:

| Dependency | Local scrubbed source count |
| --- | ---: |
| boost | 0 |
| openssl | 0 |
| brpc | 0 |
| byte | 0 |
| rustraft | 0 |
| mtcache | 0 |
| thrift | 0 |
| rapidjson | 0 |
| rocketmq | 0 |

CMake failed because OpenSSL and Boost source directories were empty. Byte/RustRaft sources are also absent, so a current-source server binary cannot be produced from this machine yet.

## AWS Status

AWS SSO was refreshed successfully:

- account: `657817560042`
- role/session: `temporalstore`

Terraform was initialized locally under `infra/aws/temporalstore-test`.

Current AWS test state:

- Terraform output is empty.
- Terraform state is absent.
- EC2 lookup for TemporalStore/Matrix tags returned no running/stopped test instances.

No AWS test was run because there is no active cluster and no current-source artifact to deploy.

## Production-Readiness Status

Raft is not production-ready yet.

The remaining real blockers are:

1. Real partition snapshot export/import.
   - `Partition::CreateDataRaftSnapshot()` and `Partition::LoadDataRaftSnapshot()` now implement local `file://` snapshot payloads. Object-store/shared-store snapshot export/import remains fail-closed until a separate adapter is added.
   - A production implementation must export/import object/page/index/oplog state plus the applied index.

2. Atomic recovery point.
   - The applied-index sidecar is better than the previous in-memory-only state.
   - It is not yet atomically committed with object/page/index/oplog mutation.

3. Current-source build artifact.
   - Need a buildable local dependency tree or a prebuilt dependency/runtime bundle that matches the current source.

4. AWS validation.
   - After building a coherent artifact, rerun:
     - shared-store smoke/QPS regression;
     - Raft one-leader/two-replica smoke;
     - Raft concurrent write/read QPS;
     - Raft secondary visibility-lag probe;
     - leader kill/failover test;
     - shared-store no-regression test.

## Safe Next Step

Populate or mount a real local dependency tree for:

- `byte`
- `rustraft`
- `brpc`
- `boost`
- `openssl`
- `thrift`
- `mtcache`
- `rapidjson`
- `rocketmq`

Then rebuild current commit `1445c42`, stage the artifact, create/reuse the AWS test cluster, and run the regression matrix above.
