#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT}/infra/aws-existing-eks"

: "${AWS_REGION:?set AWS_REGION}"
: "${TS_EKS_CLUSTER_NAME:?set TS_EKS_CLUSTER_NAME}"
: "${TS_IMAGE:?set TS_IMAGE to an ECR image, for example account.dkr.ecr.region.amazonaws.com/temporalstore-rust:tag}"

NAMESPACE="${TS_NAMESPACE:-temporalstore}"
BUILD_IMAGE="${TS_BUILD_IMAGE:-0}"
PUSH_IMAGE="${TS_PUSH_IMAGE:-0}"
APPLY="${TS_TERRAFORM_APPLY:-1}"
ENABLE_JOBS="${TS_ENABLE_VALIDATION_JOBS:-1}"
WAIT_TIMEOUT="${TS_VALIDATION_WAIT_TIMEOUT:-900s}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 127
  fi
}

require aws
require terraform
require kubectl
if [[ "${BUILD_IMAGE}" == "1" || "${PUSH_IMAGE}" == "1" ]]; then
  require docker
fi

echo "== aws identity =="
aws sts get-caller-identity

if [[ "${BUILD_IMAGE}" == "1" ]]; then
  echo "== docker build ${TS_IMAGE} =="
  docker build -t "${TS_IMAGE}" "${ROOT}"
fi

if [[ "${PUSH_IMAGE}" == "1" ]]; then
  registry="${TS_IMAGE%%/*}"
  echo "== docker login ${registry} =="
  aws ecr get-login-password --region "${AWS_REGION}" \
    | docker login --username AWS --password-stdin "${registry}"
  echo "== docker push ${TS_IMAGE} =="
  docker push "${TS_IMAGE}"
fi

echo "== kubeconfig =="
aws eks update-kubeconfig --region "${AWS_REGION}" --name "${TS_EKS_CLUSTER_NAME}"

echo "== terraform init/validate =="
cd "${TF_DIR}"
terraform init -backend=false
terraform validate

echo "== terraform ${APPLY} =="
if [[ "${APPLY}" == "1" ]]; then
  terraform apply -auto-approve \
    -var "aws_region=${AWS_REGION}" \
    -var "aws_profile=${AWS_PROFILE:-}" \
    -var "eks_cluster_name=${TS_EKS_CLUSTER_NAME}" \
    -var "namespace=${NAMESPACE}" \
    -var "image=${TS_IMAGE}" \
    -var "enable_validation_jobs=${ENABLE_JOBS}" \
    ${TS_TERRAFORM_EXTRA_ARGS:-}
else
  terraform plan \
    -var "aws_region=${AWS_REGION}" \
    -var "aws_profile=${AWS_PROFILE:-}" \
    -var "eks_cluster_name=${TS_EKS_CLUSTER_NAME}" \
    -var "namespace=${NAMESPACE}" \
    -var "image=${TS_IMAGE}" \
    -var "enable_validation_jobs=${ENABLE_JOBS}" \
    ${TS_TERRAFORM_EXTRA_ARGS:-}
  echo "TS_TERRAFORM_APPLY is not 1; stopped after plan."
  exit 0
fi

echo "== rollouts =="
kubectl -n "${NAMESPACE}" rollout status deploy/temporalstore-metaserver --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deploy/temporalstore-server --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deploy/temporalstore-proxy --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deploy/temporalstore-redis --timeout=180s
kubectl -n "${NAMESPACE}" get pods,svc,pvc,jobs -o wide

if [[ "${ENABLE_JOBS}" == "1" || "${ENABLE_JOBS}" == "true" ]]; then
  for job in temporalstore-raft-validation temporalstore-scale-validation temporalstore-storage-validation; do
    echo "== wait job/${job} =="
    kubectl -n "${NAMESPACE}" wait --for=condition=complete "job/${job}" --timeout="${WAIT_TIMEOUT}"
    echo "== logs job/${job} =="
    kubectl -n "${NAMESPACE}" logs "job/${job}" --all-containers=true
  done
fi

echo "== redis port-forward qps smoke =="
"${ROOT}/tools/scale_test_aws_existing_eks.sh"
