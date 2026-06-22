# TemporalStore AWS ASG Autoscale Test

Date: 2026-06-07

## Goal

Make AWS Auto Scaling work through the TemporalStore metaserver, not by having new data nodes coordinate directly with other data nodes.

The intended first version is conservative:

- A new ASG data node boots as a secondary-capable node.
- The node starts its local TemporalStore server.
- The node registers with metaserver through `ManageService/AddServer`.
- Metaserver owns placement, rebalance, primary movement, and routing updates.
- Scale-down goes through metaserver drain: `FreezeServer`, then `DropServer` if safe.
- The ASG lifecycle action completes only after drain succeeds.

## Code Changes

Terraform:

- `infra/aws/temporalstore-test/main.tf`
  - Added optional data-node ASG with desired capacity defaulting to `0`.
  - Added ASG launch template using the existing VPC, subnet, EFS, cache EBS, and EC2 role.
  - Added ASG termination lifecycle hook.
  - Added EventBridge rule for ASG terminate lifecycle events.
  - Added Lambda drain handler triggered by EventBridge.
  - Added S3 upload of autoscale bootstrap scripts.
  - Added IAM permissions for ASG lifecycle, SSM drain command, and Lambda logs.

Scripts:

- `scripts/package_temporalstore_runtime.sh`
  - Removed the hard-coded `/home/vj/tslink` source path.
  - Discovers normal and sandboxed `build-ubuntu22` / `output-ubuntu22` directories.
  - Copies `libthrift_conv_shim.so` into the package when available.
  - Emits `env/temporalstore-runtime-env.sh` so all binaries share one runtime library/preload setup.
  - Emits `sbin/` wrapper entrypoints for metaserver, server, proxy, stream tool, and client smoke tool.
  - Emits `sbin/check-runtime-closure`, which runs `ldd -r` and fails on missing libraries or undefined symbols.
  - Supports `TEMPORALSTORE_REQUIRE_RUNTIME_CLOSURE=1` so CI can fail if the shim is missing.

- `infra/aws/temporalstore-test/scripts/temporalstore_asg_start_server.sh`
  - Starts the data-node server from the packaged runtime.
  - Uses the metaserver endpoint, host spec, blockcache flags, async storage flags, and primary-pull replication flag from environment.
  - Prefers packaged `sbin/bcache2-server` wrappers when present.
  - Falls back to explicit `LD_LIBRARY_PATH` / `LD_PRELOAD` for the current older AWS runtime package.

- `infra/aws/temporalstore-test/scripts/temporalstore_asg_node_register.sh`
  - Discovers private IP from EC2 metadata.
  - Calls `ManageService/AddServer` against metaserver.
  - Treats already-existing registration as idempotent success.

- `infra/aws/temporalstore-test/scripts/temporalstore_asg_node_drain.sh`
  - Discovers private IP from EC2 metadata.
  - Calls `ManageService/FreezeServer`.
  - Calls `ManageService/DropServer` after freeze when configured.
  - Can either complete ASG lifecycle itself or let Lambda own the lifecycle token.

- `infra/aws/temporalstore-test/lambda/asg_drain_handler.py`
  - Receives ASG terminate lifecycle event.
  - Runs the drain script through SSM on the terminating instance.
  - Completes the lifecycle action after SSM drain returns.

Test harness:

- `infra/aws/temporalstore-test/run_asg_scale_test.ps1`
  - Applies the ASG resources at desired capacity `0`.
  - Starts a clean single metaserver on the meta node.
  - Scales ASG to `1`.
  - Waits for EC2, SSM, and cloud-init.
  - Captures data-node status and metaserver `ListServer`.
  - Scales ASG back to `0`.
  - Captures metaserver `ListServer` after drain.

## AWS Test Result

Latest result directory:

`infra/aws/temporalstore-test/asg-scale-results-20260607_014327`

Topology:

- Metaserver node: `i-05f55360d92c43908`, private `10.70.1.161:17000`
- Autoscaled data node during test: `i-069d81e31d77d8e50`, private `10.70.1.237:17001`

Scale-up result:

```json
{
  "servers": [
    {
      "server_info": {
        "id": 1,
        "endpoint": {"ip4": "10.70.1.237", "port": 17001},
        "location": {
          "vregion": "vregion",
          "vdc": "vdc1",
          "vau": "asg-secondary",
          "tag": "asg-secondary"
        },
        "state": "SERVER_NORMAL"
      }
    }
  ]
}
```

Scale-down result:

```json
{}
```

ASG activity timing:

- Launch selected: `2026-06-07T08:44:31Z`
- Launch completed: `2026-06-07T08:44:36Z`
- Termination selected: `2026-06-07T08:46:12Z`
- Termination completed: `2026-06-07T08:46:36Z`
- Scale-in duration after lifecycle hook: about `23s`

This is much better than the earlier instance-shutdown-only drain attempt, which waited for the full `600s` lifecycle timeout.

Final ASG state after the test:

```json
{
  "Desired": 0,
  "Instances": []
}
```

## Current Limitations

- The test validates node registration and drain. It does not yet validate partition rebalance onto the ASG node.
- The first safe version keeps `asg_drain_force=false`; if a node owns partitions that cannot be safely frozen, scale-down should fail or wait instead of forcing data movement.
- The current AWS artifact is still the older package shape, so the ASG scripts keep fallback preload logic. The package script now produces wrapper entrypoints and an env file for the next artifact.
- The local checkout does not contain the shim source/binary; CI or release packaging should run with `TEMPORALSTORE_REQUIRE_RUNTIME_CLOSURE=1` and a build artifact that contains `libthrift_conv_shim.so`.
- The harness starts a clean single metaserver for the ASG test. Production should run metaserver as a normal service or HA metaserver set.

## Runtime Closure Check

AWS metaserver node runtime closure was checked with the current staged runtime:

```text
== /opt/temporalstore/bin/bcache2-metaserver ==
== /opt/temporalstore/bin/bcache2-server ==
== /opt/temporalstore/bin/bcache2-proxy ==
RUNTIME_CLOSURE_OK
```

The check used:

```text
LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib:/opt/temporalstore/lib:/opt/temporalstore/sdk/lib
LD_PRELOAD=/opt/temporalstore/runtime/lib/libthrift_conv_shim.so
ldd -r <binary>
```

No `not found`, `undefined symbol`, or `bwrite_conv` error remained.

## Next Steps

- Add a table with one primary and one secondary, then scale ASG up and verify metaserver places only secondary partitions first.
- Add promotion policy: only allow reads from the ASG node after catch-up/health checks.
- Add gradual primary movement after the node remains healthy for a configured window.
- Add scale-down protection when the ASG node owns a primary.
- Add a CloudWatch dashboard for ASG events, Lambda drain success/failure, registration latency, and scale-in duration.
