#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-prebenchmark-gate-$(date +%Y%m%d-%H%M%S)}"
BACKEND="${BACKEND:-cpp}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-127.0.0.1:18000}"
NAMESPACE="${MATRIXARK_TEMPORALSTORE_NAMESPACE:-deploy_ns}"
TABLE="${MATRIXARK_TEMPORALSTORE_TABLE:-deploy_table}"
TOPOLOGY_TIMEOUT_MS="${TOPOLOGY_TIMEOUT_MS:-30000}"
RUN_TOPOLOGY_GATE="${RUN_TOPOLOGY_GATE:-1}"
RUN_PROXY_CLIENT_GATE="${RUN_PROXY_CLIENT_GATE:-1}"
RUN_CPP_CLIENT_BUILD_GATE="${RUN_CPP_CLIENT_BUILD_GATE:-1}"
CPP_CLIENT_BUILD_TIMEOUT_S="${CPP_CLIENT_BUILD_TIMEOUT_S:-300}"
CPP_CLIENT_BUILD_TARGETS="${CPP_CLIENT_BUILD_TARGETS:-customer_client_example}"
RUN_INGESTION_GATE="${RUN_INGESTION_GATE:-1}"
RUN_CACHE_EVICTION_GATE="${RUN_CACHE_EVICTION_GATE:-1}"
RUN_CONTEXT_PARITY_GATE="${RUN_CONTEXT_PARITY_GATE:-0}"
INGESTION_DRY_RUN="${INGESTION_DRY_RUN:-1}"
INGESTION_RECORDS="${INGESTION_RECORDS:-1200}"
INGESTION_BATCH_SIZE="${INGESTION_BATCH_SIZE:-128}"
STAGE_TIMEOUT_S="${STAGE_TIMEOUT_S:-600}"

mkdir -p "${RESULT_DIR}"
STAGES_JSONL="${RESULT_DIR}/stages.jsonl"
REPORT_JSON="${RESULT_DIR}/prebenchmark_gate_report.json"
REPORT_MD="${RESULT_DIR}/prebenchmark_gate_report.md"
: > "${STAGES_JSONL}"

json_quote() {
  python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'
}

write_stage() {
  local name="$1"
  local status="$2"
  local remediation="$3"
  local seconds="$4"
  local case_dir="$5"
  local stdout_tail=""
  local stderr_tail=""
  if [[ -f "${case_dir}/stdout.log" ]]; then
    stdout_tail="$(tail -80 "${case_dir}/stdout.log" || true)"
  fi
  if [[ -f "${case_dir}/stderr.log" ]]; then
    stderr_tail="$(tail -80 "${case_dir}/stderr.log" || true)"
  fi
  python3 - "${STAGES_JSONL}" "${name}" "${status}" "${remediation}" "${seconds}" "${case_dir}" "${stdout_tail}" "${stderr_tail}" <<'PY'
import json
import sys
path, name, status, remediation, seconds, case_dir, stdout_tail, stderr_tail = sys.argv[1:]
with open(path, 'a', encoding='utf-8') as fh:
    fh.write(json.dumps({
        'name': name,
        'status': status,
        'remediation': remediation,
        'seconds': int(seconds),
        'case_dir': case_dir,
        'stdout_tail': stdout_tail,
        'stderr_tail': stderr_tail,
    }, sort_keys=True) + '\n')
PY
}

finish_report() {
  local final_status="$1"
  local blocker_stage="${2:-}"
  python3 - "${STAGES_JSONL}" "${REPORT_JSON}" "${REPORT_MD}" "${final_status}" "${blocker_stage}" "${RESULT_DIR}" "${BACKEND}" "${METASERVER}" "${NAMESPACE}" "${TABLE}" <<'PY'
import json
import sys
from pathlib import Path
stages_path, report_json, report_md, final_status, blocker_stage, result_dir, backend, metaserver, namespace, table = sys.argv[1:]
stages = []
if Path(stages_path).exists():
    with open(stages_path, encoding='utf-8') as fh:
        for line in fh:
            if line.strip():
                stages.append(json.loads(line))
report = {
    'status': final_status,
    'blocker_stage': blocker_stage,
    'backend': backend,
    'topology': {'metaserver': metaserver, 'namespace': namespace, 'table': table},
    'result_dir': result_dir,
    'stage_order': [stage['name'] for stage in stages],
    'stages': stages,
    'policy': {
        'topology_before_ingestion': True,
        'ingestion_before_context_parity': True,
        'cache_eviction_before_scale_claims': True,
        'stop_on_first_failure': True,
    },
}
Path(report_json).write_text(json.dumps(report, indent=2, sort_keys=True) + '\n', encoding='utf-8')
lines = [
    '# TemporalStore Prebenchmark Gate',
    '',
    f'- status: `{final_status}`',
    f'- blocker_stage: `{blocker_stage}`',
    f'- backend: `{backend}`',
    f'- metaserver: `{metaserver}`',
    f'- namespace/table: `{namespace}` / `{table}`',
    f'- result_dir: `{result_dir}`',
    '',
    '| Stage | Status | Seconds | Remediation | Case dir |',
    '| --- | --- | ---: | --- | --- |',
]
for stage in stages:
    lines.append('| {name} | `{status}` | {seconds} | {remediation} | `{case_dir}` |'.format(**stage))
if any(stage.get('status') != 'pass' for stage in stages):
    lines.extend(['', '## Failure Tails', ''])
    for stage in stages:
        if stage.get('status') == 'pass':
            continue
        lines.extend([
            f"### {stage['name']}",
            '',
            f"Remediation bucket: {stage['remediation']}",
            '',
            'stdout tail:',
            '```text',
            stage.get('stdout_tail', '')[-4000:],
            '```',
            '',
            'stderr tail:',
            '```text',
            stage.get('stderr_tail', '')[-4000:],
            '```',
            '',
        ])
Path(report_md).write_text('\n'.join(lines) + '\n', encoding='utf-8')
print(json.dumps(report, indent=2, sort_keys=True))
PY
}

run_stage() {
  local name="$1"
  local remediation="$2"
  shift 2
  local case_dir="${RESULT_DIR}/${name}"
  local start_s end_s code
  mkdir -p "${case_dir}"
  start_s="$(date +%s)"
  set +e
  (cd "${ROOT}" && timeout "${STAGE_TIMEOUT_S}" "$@") > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"
  local stage_status=fail
  if [[ "${code}" == "124" ]]; then
    stage_status=timeout
    if (( end_s - start_s >= STAGE_TIMEOUT_S )); then
      echo "stage ${name} timed out after ${STAGE_TIMEOUT_S}s" >> "${case_dir}/stderr.log"
    else
      echo "stage ${name} reported an inner timeout before the outer ${STAGE_TIMEOUT_S}s guard" >> "${case_dir}/stderr.log"
    fi
  fi
  if [[ "${code}" == "0" ]]; then
    write_stage "${name}" pass "${remediation}" "$((end_s - start_s))" "${case_dir}"
    return 0
  fi
  write_stage "${name}" "${stage_status}" "${remediation}" "$((end_s - start_s))" "${case_dir}"
  finish_report fail "${name}"
  return "${code}"
}

if [[ "${RUN_TOPOLOGY_GATE}" == "1" ]]; then
  run_stage topology_readiness \
    "fix metaserver reachability, namespace/table creation, placement, slot coverage, primary assignment, or topology readiness retries" \
    bash tools/wait_temporalstore_topology_ready.sh --backend "${BACKEND}" --metaserver "${METASERVER}" --namespace "${NAMESPACE}" --table "${TABLE}" --timeout-ms "${TOPOLOGY_TIMEOUT_MS}"
fi


if [[ "${RUN_CPP_CLIENT_BUILD_GATE}" == "1" ]]; then
  if [[ "${BACKEND}" != "cpp" ]]; then
    write_stage cpp_client_target_build skip "C++ client target build gate is C++-specific" 0 "${RESULT_DIR}/cpp_client_target_build"
  else
    run_stage cpp_client_target_build \
      "fix stale build tree, missing client target dependencies, long compile fan-in, or local client build timeout before proxy/client pressure" \
      env BUILD_TYPE="${BUILD_TYPE}" BUILD_TARGETS="${CPP_CLIENT_BUILD_TARGETS}" BUILD_TIMEOUT_S="${CPP_CLIENT_BUILD_TIMEOUT_S}" ARTIFACT_DIR="${RESULT_DIR}/cpp_client_target_build/artifacts" bash tools/run_cpp_client_target_gate.sh
  fi
fi

if [[ "${RUN_PROXY_CLIENT_GATE}" == "1" ]]; then
  if [[ "${BACKEND}" != "cpp" ]]; then
    write_stage proxy_client skip "proxy/client gate is C++-specific; use Rust proxy/direct SDK parity for rust" 0 "${RESULT_DIR}/proxy_client"
  else
    run_stage proxy_client \
      "fix launcher, live proxy port, direct SDK oracle, request timeout, or C++ proxy status warnings" \
      env BUILD_TYPE="${BUILD_TYPE}" bash tools/run_cpp_benchmark_transport_parity_ubuntu22.sh
  fi
fi

if [[ "${RUN_INGESTION_GATE}" == "1" ]]; then
  run_stage ingestion_write_path \
    "fix queue replay, append batching, async oplog, or backend write timeout before MatrixArk context parity" \
    env BUILD_TYPE="${BUILD_TYPE}" FORCE_BUILD="${INGESTION_FORCE_BUILD:-0}" DRY_RUN="${INGESTION_DRY_RUN}" RECORDS="${INGESTION_RECORDS}" BATCH_SIZE="${INGESTION_BATCH_SIZE}" ITERATIONS=1 SOURCES=api,kafka,flink bash tools/run_queue_ingestion_replay_ubuntu22.sh
fi

if [[ "${RUN_CACHE_EVICTION_GATE}" == "1" ]]; then
  run_stage cache_eviction_invariants \
    "fix cache admission, eviction counters, refill-from-persistence, page compaction, GC, and recovery invariants before scale claims" \
    env BUILD_TYPE="${BUILD_TYPE}" RUN_RUNTIME_TEST=auto bash tools/run_cache_fallback_metrics_ubuntu22.sh
  run_stage deep_storage_mode_matrix \
    "fix cache/eviction/storage mode parity matrix gaps before production scale claims" \
    python3 third_party/TemporalStoreTestCorpus/tools/run_deep_storage_mode_parity.py --report-json "${RESULT_DIR}/deep_storage_mode_parity.json" --report-md "${RESULT_DIR}/deep_storage_mode_parity.md"
fi

if [[ "${RUN_CONTEXT_PARITY_GATE}" == "1" ]]; then
  run_stage matrixark_context_parity \
    "fix backend drift before MatrixArk context parity or benchmark tuning" \
    python3 third_party/TemporalStoreTestCorpus/tools/run_matrixark_required_pipeline_parity.py --backends "${BACKEND}" --run-id "prebenchmark_${BACKEND}_$(date +%Y%m%d_%H%M%S)"
fi

finish_report pass ""
echo "PASS TemporalStore prebenchmark gate"
echo "report_json=${REPORT_JSON}"
echo "report_md=${REPORT_MD}"
