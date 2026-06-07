#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

: "${AWS_REGION:?set AWS_REGION}"
: "${TS_EKS_CLUSTER_NAME:?set TS_EKS_CLUSTER_NAME}"

NAMESPACE="${TS_NAMESPACE:-temporalstore}"
PROXY_REPLICAS="${TS_PROXY_REPLICAS:-3}"
REDIS_REPLICAS="${TS_REDIS_PROXY_REPLICAS:-3}"
REDIS_LOCAL_PORT="${TS_REDIS_LOCAL_PORT:-16379}"
LOAD_CONCURRENCY="${TS_LOAD_CONCURRENCY:-32}"
LOAD_SECONDS="${TS_LOAD_SECONDS:-60}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "$1 is required" >&2
    exit 127
  fi
}

require aws
require kubectl
require python3

echo "== aws identity =="
aws sts get-caller-identity

echo "== kubeconfig =="
aws eks update-kubeconfig --region "${AWS_REGION}" --name "${TS_EKS_CLUSTER_NAME}"

echo "== current cluster resources =="
kubectl -n "${NAMESPACE}" get deploy,po,svc,pvc

echo "== scale stateless proxy layers =="
kubectl -n "${NAMESPACE}" scale deploy/temporalstore-proxy --replicas="${PROXY_REPLICAS}"
kubectl -n "${NAMESPACE}" scale deploy/temporalstore-redis --replicas="${REDIS_REPLICAS}"
kubectl -n "${NAMESPACE}" rollout status deploy/temporalstore-proxy --timeout=180s
kubectl -n "${NAMESPACE}" rollout status deploy/temporalstore-redis --timeout=180s

echo "== post-scale resources =="
kubectl -n "${NAMESPACE}" get deploy,po,svc -o wide

cleanup() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "${PF_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "== port-forward redis service =="
kubectl -n "${NAMESPACE}" port-forward svc/temporalstore-redis "${REDIS_LOCAL_PORT}:16379" >/tmp/temporalstore-redis-port-forward.log 2>&1 &
PF_PID=$!
sleep 3
if ! kill -0 "${PF_PID}" >/dev/null 2>&1; then
  cat /tmp/temporalstore-redis-port-forward.log >&2 || true
  echo "port-forward failed" >&2
  exit 1
fi

echo "== redis-compatible load =="
python3 "${ROOT}/tools/redis_scale_load.py" \
  --host 127.0.0.1 \
  --port "${REDIS_LOCAL_PORT}" \
  --concurrency "${LOAD_CONCURRENCY}" \
  --duration-seconds "${LOAD_SECONDS}"

echo "== pod resource snapshot =="
kubectl -n "${NAMESPACE}" top pods 2>/dev/null || echo "metrics-server not available; skipped kubectl top pods"
kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp | tail -40
