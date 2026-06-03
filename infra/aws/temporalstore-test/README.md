# TemporalStore AWS Test Cluster

Small EC2-first deployment for validating TemporalStore on AWS.

## Shape

- 2 data nodes: `c7i.large`, 2 vCPU, 4 GiB memory, 20 GiB root gp3
- 1 metaserver node: `t3.small`, 2 vCPU, 2 GiB memory, 20 GiB root gp3
- 2 extra data-node cache volumes: 10 GiB gp3 each
- 1 EFS filesystem for shared page/index/oplog/snapshot testing
- SSM Session Manager access; SSH is closed by default
- Monitoring UI on metaserver port `8088`; public access is closed unless `allowed_monitoring_cidr` is set

## Paths

Data nodes:

- `/mnt/temporalstore-cache/mtcache-ssd`: MtCache SSD tier on extra EBS gp3
- `/mnt/temporalstore-shared`: EFS shared storage

All nodes:

- `/opt/temporalstore`: binaries/config placeholder
- `/var/log/temporalstore`: service logs placeholder

Metaserver node:

- `/opt/temporalstore/monitoring-ui`: static monitoring console
- `http://<meta-public-ip>:8088/`: UI URL when `allowed_monitoring_cidr` allows your IP

Benchmarks/client commands can run from `meta-01`; there is no separate client EC2 instance.

## Commands

```powershell
$env:AWS_PROFILE="temporalstore"
terraform init
terraform plan
terraform apply
terraform output
```

To temporarily expose the monitoring UI to your current IP, set:

```powershell
terraform apply -var "allowed_monitoring_cidr=<your-ip>/32"
```

To connect after apply:

```powershell
aws ssm start-session --profile temporalstore --region us-west-2 --target <instance-id>
```

## Cleanup

```powershell
terraform destroy
```
