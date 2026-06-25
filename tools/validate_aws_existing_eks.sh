#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT}/infra/aws-existing-eks"

: "${AWS_REGION:?set AWS_REGION}"
: "${TS_EKS_CLUSTER_NAME:?set TS_EKS_CLUSTER_NAME}"
: "${TS_IMAGE:?set TS_IMAGE}"
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"

cd "${TF_DIR}"

echo "== terraform fmt/check =="
terraform fmt -check

echo "== aws identity =="
aws sts get-caller-identity

echo "== terraform init/validate =="
terraform init -backend=false
terraform validate

echo "== eks cluster =="
aws eks describe-cluster --region "${AWS_REGION}" --name "${TS_EKS_CLUSTER_NAME}" >/tmp/temporalstore-eks-cluster.json

echo "== terraform plan =="
terraform plan \
  -var "aws_region=${AWS_REGION}" \
  -var "aws_profile=${AWS_PROFILE:-}" \
  -var "eks_cluster_name=${TS_EKS_CLUSTER_NAME}" \
  -var "image=${TS_IMAGE}" \
  ${TS_TERRAFORM_EXTRA_ARGS:-}

if [[ "${TS_TERRAFORM_APPLY:-0}" == "1" ]]; then
  echo "== terraform apply =="
  terraform apply -auto-approve \
    -var "aws_region=${AWS_REGION}" \
    -var "aws_profile=${AWS_PROFILE:-}" \
    -var "eks_cluster_name=${TS_EKS_CLUSTER_NAME}" \
    -var "image=${TS_IMAGE}" \
    ${TS_TERRAFORM_EXTRA_ARGS:-}

  if command -v kubectl >/dev/null 2>&1; then
    aws eks update-kubeconfig --region "${AWS_REGION}" --name "${TS_EKS_CLUSTER_NAME}"
    kubectl -n "${TS_NAMESPACE:-temporalstore}" rollout status deploy/temporalstore-metaserver --timeout=120s
    kubectl -n "${TS_NAMESPACE:-temporalstore}" rollout status deploy/temporalstore-server --timeout=120s
    kubectl -n "${TS_NAMESPACE:-temporalstore}" rollout status deploy/temporalstore-proxy --timeout=120s
    kubectl -n "${TS_NAMESPACE:-temporalstore}" rollout status deploy/temporalstore-redis --timeout=120s
    kubectl -n "${TS_NAMESPACE:-temporalstore}" get pods,svc,pvc
  else
    echo "kubectl not found; apply completed but Kubernetes rollout validation was skipped." >&2
  fi
else
  echo "TS_TERRAFORM_APPLY is not 1; stopped after plan."
fi
