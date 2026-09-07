#!/usr/bin/env bash
# Shared runtime environment for the launchers that run the Rust engine from a checkout.
#
# `tools/run_rust_unified_tests.sh` and `tools/run_temporalstore_rust_ubuntu22.sh` both source this
# file and call `temporalstore_export_sdk_loader_path`. It was not in the repository, so both
# scripts died on their own line 5 -- `set -euo pipefail` turns a missing `source` into an
# immediate exit, before either script does anything, and the message names a path rather than a
# cause.
#
# The body is the loader-path block that `tools/matrixark_codex_rust_hook.sh` already runs inline,
# lifted into the function those two scripts expect rather than invented: the SDK builds a cdylib,
# and a binary that links it needs the directory holding it on the loader path.

# Put the built SDK's library directory on the dynamic loader path, if it exists.
# Takes the repository root. Safe to call more than once and safe when nothing is built yet --
# a directory that is not there is skipped rather than exported as an empty entry, because an
# empty component in LD_LIBRARY_PATH means "the current directory" to the loader.
temporalstore_export_sdk_loader_path() {
  local root="${1:-}"
  if [[ -z "${root}" ]]; then
    echo "temporalstore_export_sdk_loader_path: repository root argument is required" >&2
    return 2
  fi

  local libdir
  for libdir in \
    "${root}/output/sdk/lib" \
    "${root}/sdk/lib"; do
    if [[ -d "${libdir}" ]]; then
      export LD_LIBRARY_PATH="${libdir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    fi
  done
}
