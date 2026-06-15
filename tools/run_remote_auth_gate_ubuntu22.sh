#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-remote-auth-gate-$(date +%Y%m%d_%H%M%S)}"
REMOTE_NAME="${REMOTE_NAME:-origin}"
EXPECTED_REMOTE_REGEX="${EXPECTED_REMOTE_REGEX:-bjmeetsfo/TemporalStore\\.git}"
REMOTE_TIMEOUT_S="${REMOTE_TIMEOUT_S:-30}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

log "TemporalStore remote auth gate"
log "result_dir=${RESULT_DIR}"
log "remote_name=${REMOTE_NAME}"

remote_url="$(git -C "${ROOT}" remote get-url "${REMOTE_NAME}")"
log "remote_url=${remote_url}"
if ! printf '%s\n' "${remote_url}" | grep -Eq "${EXPECTED_REMOTE_REGEX}"; then
  log "FAIL unexpected remote url"
  exit 2
fi

helpers="$(git -C "${ROOT}" config --get-all credential.helper || true)"
printf '%s\n' "${helpers}" > "${RESULT_DIR}/credential_helpers.txt"
if printf '%s\n' "${helpers}" | grep -q 'gh auth git-credential' && ! command -v gh >/dev/null 2>&1; then
  log "FAIL credential helper requires gh, but gh is not installed in this WSL environment"
  exit 3
fi

set +e
GIT_TERMINAL_PROMPT=0 timeout "${REMOTE_TIMEOUT_S}" git -C "${ROOT}" ls-remote "${REMOTE_NAME}" \
  > "${RESULT_DIR}/ls_remote.out" 2> "${RESULT_DIR}/ls_remote.err"
code=$?
set -e

if [[ "${code}" == "0" ]]; then
  log "PASS TemporalStore remote auth gate"
  exit 0
fi

log "FAIL git ls-remote code=${code}"
tail -80 "${RESULT_DIR}/ls_remote.err" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
exit "${code}"
