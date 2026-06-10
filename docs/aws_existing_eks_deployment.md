# AWS Existing EKS Deployment

This path deploys the Rust TemporalStore TemporalStore Rust stack onto an existing EKS cluster. It reuses
the cluster; it does not create VPC, EKS, node groups, or ECR.

Components:

- `metaserver` on port `17001`
- `server` on port `17002`
- `proxy` on port `17000`
- `redis_proxy` on port `16379`
- one PVC for server page-store, index, and cache files
- optional validation jobs for distributed Raft, shared-store/local-WAL storage modes, and write/read QPS

Build and push the image:

```bash
docker build -t <account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag> .
aws ecr get-login-password --region <region> \
  | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker push <account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag>
```

The image includes service binaries and validation harness binaries:

- `distributed_raft_harness`
- `scale_harness`
- `client_scale_harness`
- `raft_secondary_replication_harness`
- `storage_modes_harness`

Validate Terraform:

```bash
cd infra/aws-existing-eks
terraform init -backend=false
terraform validate
```

Plan/apply against an existing EKS cluster:

```bash
terraform plan \
  -var aws_region=<region> \
  -var eks_cluster_name=<cluster-name> \
  -var image=<account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag>

terraform apply \
  -var aws_region=<region> \
  -var eks_cluster_name=<cluster-name> \
  -var image=<account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag> \
  -var enable_validation_jobs=true
```

Validation after apply:

```bash
aws eks update-kubeconfig --region <region> --name <cluster-name>
kubectl -n temporalstore get pods,svc,pvc
kubectl -n temporalstore port-forward svc/temporalstore-proxy 17000:17000
curl http://127.0.0.1:17000/health
```

Redis-compatible validation:

```bash
kubectl -n temporalstore port-forward svc/temporalstore-redis 16379:16379
redis-cli -p 16379 SET aws:key value
redis-cli -p 16379 GET aws:key
```

Scale testing the stateless layers:

```bash
export AWS_REGION=<region>
export TS_EKS_CLUSTER_NAME=<cluster-name>
export TS_NAMESPACE=temporalstore
export TS_PROXY_REPLICAS=3
export TS_REDIS_PROXY_REPLICAS=3
export TS_LOAD_CONCURRENCY=32
export TS_LOAD_SECONDS=60

tools/scale_test_aws_existing_eks.sh
```

The scale script:

- reuses the existing EKS cluster
- scales only `temporalstore-proxy` and `temporalstore-redis`
- waits for rollout
- port-forwards the Redis service
- runs `tools/redis_scale_load.py` with concurrent `SET`/`GET` traffic
- prints QPS and p50/p95/p99 latency

Full deploy and validation:

```bash
export AWS_REGION=<region>
export TS_EKS_CLUSTER_NAME=<cluster-name>
export TS_IMAGE=<account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag>

# Optional if the image is not already built and pushed.
export TS_BUILD_IMAGE=1
export TS_PUSH_IMAGE=1

# Applies Terraform, waits for services, waits for validation jobs, prints logs, then runs Redis QPS.
tools/deploy_and_test_aws_existing_eks.sh
```

The script deletes stale validation jobs before apply, waits for fresh job completion, and parses the
JSON logs with `tools/validate_aws_validation_log.py`. It fails if replica reads return wrong values,
Raft apply health is unhealthy, follower writes are accepted, write QPS is zero, replica lag is
non-zero, shared-store sync lag is non-zero, or local-WAL restore cannot read back committed data.

Validation jobs:

- `temporalstore-raft-validation`: runs `distributed_raft_harness` in EKS. It validates
  separate-node Raft HTTP replication, follower write rejection, leader transfer, scale down/up,
  apply-health convergence, WAL file creation, and external snapshot bootstrap/read.
- `temporalstore-scale-validation`: runs `scale_harness`. It prints write/read QPS, p50/p95/p99
  replica-read latency, failover count, scale events, replication lag, and shared-store sync/async
  comparison.
- `temporalstore-storage-validation`: runs `storage_modes_harness`. It validates sync storage,
  async storage flush/replay, and Raft local-file WAL restore.

You can tune the QPS/latency job without editing Terraform:

```bash
export TS_TERRAFORM_EXTRA_ARGS='
  -var scale_validation_args=[
    "--nodes","5",
    "--string-ops","10000",
    "--hash-ops","2000",
    "--sequence-keys","8",
    "--sequence-len","1000",
    "--scale-events","4",
    "--failover-every","500",
    "--read-sample-every","50",
    "--compare-shared-store","true",
    "--shared-store-ops","10000",
    "--shared-store-flush-every","50"
  ]'
```

The scale harness JSON includes:

- `write_ops_per_sec`
- `raft_replica_read_latency.p50_us/p95_us/p99_us`
- `max_replica_lag`
- `shared_store.sync_replica_read_latency`
- `shared_store.async_replica_read_latency`
- `shared_store.async_max_lag`

Manual harness command, useful on EC2 or inside an EKS debug pod:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-target \
cargo run --release -p temporalstore-rust --bin scale_harness -- \
  --nodes 5 \
  --string-ops 5000 \
  --hash-ops 1000 \
  --sequence-keys 8 \
  --sequence-len 1000 \
  --scale-events 4 \
  --failover-every 500 \
  --read-sample-every 50 \
  --compare-shared-store true \
  --shared-store-ops 5000 \
  --shared-store-flush-every 50
```

Notes:

- The current Rust server is still a single-shard/single-server runtime. The Terraform deploys one
  server replica intentionally because the local page/index files are on one PVC.
- The EKS validation jobs exercise distributed Raft and replication behavior in separate pods/jobs,
  while the always-on service deployment remains a conservative single-server topology.
- Use `ClusterIP` instead of `LoadBalancer` for `proxy_service_type` and `redis_service_type` if you
  only want port-forwarded validation and no external AWS load balancers.
