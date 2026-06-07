# AWS Existing EKS Deployment

This path deploys the Rust TemporalStore single-node stack onto an existing EKS cluster. It reuses
the cluster; it does not create VPC, EKS, node groups, or ECR.

Components:

- `metaserver` on port `17001`
- `server` on port `17002`
- `proxy` on port `17000`
- `redis_proxy` on port `16379`
- one PVC for server page-store, index, and cache files

Build and push the image:

```bash
docker build -t <account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag> .
aws ecr get-login-password --region <region> \
  | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker push <account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag>
```

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
  -var image=<account>.dkr.ecr.<region>.amazonaws.com/temporalstore-rust:<tag>
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

Notes:

- The current Rust server is still a single-shard/single-server runtime. The Terraform deploys one
  server replica intentionally because the local page/index files are on one PVC.
- Raft and autoscale behavior are modeled in tests, but this EKS Terraform is not yet a distributed
  multi-shard production topology.
- Use `ClusterIP` instead of `LoadBalancer` for `proxy_service_type` and `redis_service_type` if you
  only want port-forwarded validation and no external AWS load balancers.
