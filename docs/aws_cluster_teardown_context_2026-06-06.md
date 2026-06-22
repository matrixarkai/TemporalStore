# AWS Cluster Teardown Context - 2026-06-06

## Purpose

This note saves the current AWS TemporalStore test-cluster state before teardown, so the next session can restart faster without rediscovering instance IDs, ports, test docs, and caveats.

## AWS Account And Region

- AWS profile: `temporalstore`
- Account: `657817560042`
- Region: `us-west-2`
- Local Terraform directory: `infra/aws/temporalstore-test`
- Important caveat: local Terraform state was missing at teardown time. `terraform state list` returned `No state file was found`, so teardown was done by AWS tags/resource IDs instead of `terraform destroy`.

## Cluster Resources Before Teardown

### EC2

| Role | Name | Instance ID | Type | Private IP | Public IP | State |
|---|---|---|---|---|---|---|
| metaserver/proxy/client | `temporalstore-test-meta-01` | `i-003c930417f7ee609` | `t3.small` | `10.70.1.79` | `54.185.179.199` | running |
| data | `temporalstore-test-data-01` | `i-0724d90b323786546` | `c7i.large` | `10.70.1.163` | `34.220.40.91` | running |
| data | `temporalstore-test-data-02` | `i-096334bd8cc7ab259` | `c7i.large` | `10.70.1.202` | `54.203.2.154` | running |

### EBS Volumes

| Name | Volume ID | Size | Type | Attached To |
|---|---|---:|---|---|
| root | `vol-0aa9e62c15628f04d` | 20 GiB | gp3 | meta-01 |
| root | `vol-07efb74a16a47b012` | 20 GiB | gp3 | data-01 |
| `temporalstore-test-data-01-ssd-cache` | `vol-0d903edda0144daad` | 10 GiB | gp3 | data-01 |
| root | `vol-0038bf821c5dd403d` | 20 GiB | gp3 | data-02 |
| `temporalstore-test-data-02-ssd-cache` | `vol-0a9aba6db4ee5fce9` | 10 GiB | gp3 | data-02 |

### Shared Storage

- EFS filesystem: `fs-04c6e37f04543e37d`
- Name: `temporalstore-test-shared`
- Approx size before teardown: `1.09 GB`
- Mount target: `fsmt-0a9c70dfbbadf5f26`
- Mount target subnet: `subnet-0f3d56ec9ef6a5949`
- Mount target IP: `10.70.1.46`

### Network

- VPC: `vpc-0d27fac240c75815a`, CIDR `10.70.0.0/16`
- Subnet: `subnet-0f3d56ec9ef6a5949`, CIDR `10.70.1.0/24`, AZ `us-west-2a`
- Internet gateway: `igw-0b33439b8c2b6169c`
- Route table: `rtb-0f89955d4d25f4a98`
- Security groups:
  - `sg-082c37ba25320792a`, `temporalstore-test-nodes`
  - `sg-059cea488ef59f7ea`, `temporalstore-test-efs`

### IAM

- Instance profile: `temporalstore-test-ec2-profile`
- Role: `temporalstore-test-ec2-role`

### S3 Artifact Buckets

These buckets existed before teardown and were deleted after the EC2/EFS/VPC teardown:

- `temporalstore-artifacts-657817560042-us-west-2`, about `374 MB`
- `temporalstore-temp-artifacts-657817560042-us-west-2`, about `209 MB`
- `temporalstore-test-artifacts-657817560042-us-west-2`, about `1.36 GB`

They contained runtime/test artifacts and can be recreated/re-uploaded from local builds next time.

## Last Runtime State

Before teardown, data01 and data02 were restored to:

```text
--storage_async=false
--enable_blockcache=false
```

The latest storage async benchmark temporarily ran both:

```text
--storage_async=false
--storage_async=true
```

but the cluster was restored to conservative sync mode afterward.

## Latest Test Docs

- `docs/aws_one_cluster_test_index_2026-06-06.md`
- `docs/aws_storage_async_compare_2026-06-06.md`
- `docs/aws_three_service_results_summary_2026-06-06.md`
- `docs/aws_bytekv_abase_one_hour_soak_2026-06-06.md`
- `docs/aws_scale_replication_results_2026-06-04.md`
- `docs/aws_efs_performance_summary_2026-06-04.md`
- `docs/abase_redis_api_test_2026-06-06.md`
- `../BYTEKV/docs/aws_scale_latency_2026-06-06.md`

## Latest Useful Scripts

- TemporalStore async comparison:
  - `infra/aws/temporalstore-test/run_storage_async_compare.ps1`
- TemporalStore cache tier test:
  - `infra/aws/temporalstore-test/run_cache_tier_latency_test.ps1`
- Low-thread concurrent sweep:
  - `infra/aws/temporalstore-test/run_low_thread_concurrent_sweep.ps1`

## Recreate Checklist For Next Time

1. Authenticate AWS:

```powershell
aws sso login --profile temporalstore
aws sts get-caller-identity --profile temporalstore --region us-west-2
```

2. Recreate the cluster from:

```text
infra/aws/temporalstore-test
```

Because the old local Terraform state was missing, recreate from a clean Terraform apply and preserve the new state file/backend before running long tests.

3. Confirm the intended topology:

```text
1 metaserver/proxy/client node
2 data nodes
EFS mounted only on data nodes
extra gp3 volume on each data node for SSD/blockcache experiments
```

4. Upload runtime artifacts and start:

```text
bcache2-metaserver on meta node, port 17000
bcache2-proxy on meta node, port 17080
bcache2-server on data01, port 17001
bcache2-server on data02, port 17002
```

5. Run smoke before scale:

```text
STRING set/get
HASH set/get
TemporalAggregate incr/query
secondary visibility lag probe
```

## Lessons For Next Start

- Do not rely on local Terraform state unless it exists and is current.
- Keep `storage_async=false` as the safe default.
- Use `storage_async=true` only in controlled tests until crash/RPO tests are added.
- The EFS path is functional but sync writes are slow; async writes are much faster.
- Disable blockcache when testing storage persistence latency, otherwise mtcache SSD lock/restart issues can pollute the result.
- For 2-vCPU data nodes, prefer 1/2/4 client-thread sweeps; avoid accidental high-thread runs.

## Final Teardown Verification

Final sweep after teardown showed no remaining active/current resources for:

- EC2 instances tagged `Project=TemporalStore`, excluding already-terminated history
- EBS volumes tagged `Project=TemporalStore`
- EFS filesystems tagged `Project=TemporalStore`
- VPCs tagged `Project=TemporalStore`
- S3 buckets with names containing `temporalstore`
- IAM instance profile `temporalstore-test-ec2-profile`
- IAM role `temporalstore-test-ec2-role`

## Current Minimal AWS State - 2026-06-10

After the later 2026-06-09 replication benchmark, the temporary benchmark resources were destroyed again and the environment was reduced to one always-on server node for the website, observation UI, and future lightweight coordination.

Only running EC2 instance in `us-west-2`:

| Role | Name | Instance ID | Type | Private IP | Public IP | State |
|---|---|---|---|---|---|---|
| metaserver/client/company website | `temporalstore-test-meta-01` | `i-05f55360d92c43908` | `t3.small` | `10.70.1.161` | `34.223.234.63` | running |

Confirmed absent:

- No EFS filesystems.
- No data-node EC2 instances.
- No extra test/cache EBS volumes.

Only remaining EBS volume:

| Volume ID | Size | Type | Attached To | Purpose |
|---|---:|---|---|---|
| `vol-07b2c066714e8b418` | 20 GiB | gp3 | `i-05f55360d92c43908` | root disk |

Live URLs on the remaining node:

- `https://matrixark.ai/`
- `https://matrixark.ai/observation/`
- `https://matrixark.ai/monitoring/`

Operational note:

- Keep this node if the company website and observation pages should remain reachable.
- Stop or terminate this node too if the goal is zero EC2 compute spend.
