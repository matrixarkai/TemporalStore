#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-remote-auth-gate-$(date +%Y%m%d_%H%M%S)}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
EXPECTED_REMOTE_REGEX="${EXPECTED_REMOTE_REGEX:-bjmeetsfo/TemporalStore\\.git}"
REMOTE_TIMEOUT_S="${REMOTE_TIMEOUT_S:-30}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-remote-auth.prom}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
ls_remote_code=1
expected_remote_match=0
visible_refs=0
credential_helper_gh_missing=0

write_metrics() {
  local pass="$1"
  cat > "${METRICS_FILE}" <<METRICS
# HELP temporalstore_remote_auth_gate_pass Whether the non-interactive git remote auth gate passed.
# TYPE temporalstore_remote_auth_gate_pass gauge
temporalstore_remote_auth_gate_pass ${pass}
# HELP temporalstore_remote_auth_ls_remote_exit_code Exit code from git ls-remote with terminal prompts disabled.
# TYPE temporalstore_remote_auth_ls_remote_exit_code gauge
temporalstore_remote_auth_ls_remote_exit_code ${ls_remote_code}
# HELP temporalstore_remote_auth_expected_remote_match Whether the configured remote URL matches the expected account/repository.
# TYPE temporalstore_remote_auth_expected_remote_match gauge
temporalstore_remote_auth_expected_remote_match ${expected_remote_match}
# HELP temporalstore_remote_auth_visible_refs Number of refs returned by git ls-remote.
# TYPE temporalstore_remote_auth_visible_refs gauge
temporalstore_remote_auth_visible_refs ${visible_refs}
# HELP temporalstore_remote_auth_credential_helper_gh_missing Whether git config requires gh auth but gh is unavailable.
# TYPE temporalstore_remote_auth_credential_helper_gh_missing gauge
temporalstore_remote_auth_credential_helper_gh_missing ${credential_helper_gh_missing}
METRICS
}

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

trap 'write_metrics 0' EXIT

log "TemporalStore remote auth gate"
log "result_dir=${RESULT_DIR}"
log "remote_name=${REMOTE_NAME}"
log "metrics_file=${METRICS_FILE}"

remote_url="$(git -C "${ROOT}" remote get-url "${REMOTE_NAME}")"
log "remote_url=${remote_url}"
if ! printf '%s\n' "${remote_url}" | grep -Eq "${EXPECTED_REMOTE_REGEX}"; then
  log "FAIL unexpected remote url"
  exit 2
fi
expected_remote_match=1

helpers="$(git -C "${ROOT}" config --get-all credential.helper || true)"
printf '%s\n' "${helpers}" > "${RESULT_DIR}/credential_helpers.txt"
if printf '%s\n' "${helpers}" | grep -q 'gh auth git-credential' && ! command -v gh >/dev/null 2>&1; then
  credential_helper_gh_missing=1
  log "FAIL credential helper requires gh, but gh is not installed in this WSL environment"
  exit 3
fi

set +e
GIT_TERMINAL_PROMPT=0 timeout "${REMOTE_TIMEOUT_S}" git -C "${ROOT}" ls-remote "${REMOTE_NAME}" \
  > "${RESULT_DIR}/ls_remote.out" 2> "${RESULT_DIR}/ls_remote.err"
ls_remote_code=$?
set -e

if [[ "${ls_remote_code}" == "0" ]]; then
  visible_refs="$(wc -l < "${RESULT_DIR}/ls_remote.out" | tr -d ' ')"
  log "PASS TemporalStore remote auth gate"
  write_metrics 1
  trap - EXIT
  exit 0
fi

log "FAIL git ls-remote code=${ls_remote_code}"
tail -80 "${RESULT_DIR}/ls_remote.err" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
exit "${ls_remote_code}"
