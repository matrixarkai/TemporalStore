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
VALIDATION_JOBS=(
  temporalstore-raft-validation
  temporalstore-scale-validation
  temporalstore-storage-validation
)
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 127
  fi
}

require aws
require terraform
require kubectl
require python3
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
kubectl auth can-i get pods -n "${NAMESPACE}" >/dev/null
if [[ "${ENABLE_JOBS}" == "1" || "${ENABLE_JOBS}" == "true" ]]; then
  kubectl auth can-i create jobs -n "${NAMESPACE}" >/dev/null
  kubectl auth can-i delete jobs -n "${NAMESPACE}" >/dev/null
fi

if [[ "${ENABLE_JOBS}" == "1" || "${ENABLE_JOBS}" == "true" ]]; then
  echo "== remove old validation jobs =="
  kubectl -n "${NAMESPACE}" delete jobs "${VALIDATION_JOBS[@]}" --ignore-not-found=true >/dev/null 2>&1 || true
fi

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
  for job in "${VALIDATION_JOBS[@]}"; do
    echo "== wait job/${job} =="
    if ! kubectl -n "${NAMESPACE}" wait --for=condition=complete "job/${job}" --timeout="${WAIT_TIMEOUT}"; then
      kubectl -n "${NAMESPACE}" describe "job/${job}" >&2 || true
      kubectl -n "${NAMESPACE}" logs "job/${job}" --all-containers=true >&2 || true
      echo "validation job ${job} did not complete" >&2
      exit 1
    fi
    echo "== logs job/${job} =="
    log_file="/tmp/${job}.log"
    kubectl -n "${NAMESPACE}" logs "job/${job}" --all-containers=true | tee "${log_file}"
    python3 "${ROOT}/tools/validate_aws_validation_log.py" --job "${job}" --log "${log_file}"
  done
fi

echo "== redis port-forward qps smoke =="
"${ROOT}/tools/scale_test_aws_existing_eks.sh"
